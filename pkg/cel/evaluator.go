/*
Copyright 2025 BubuStack.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package cel

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"math"
	"reflect"
	"regexp"
	"strings"
	"time"
	"unicode"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
	"github.com/google/cel-go/common/types/traits"
	"github.com/google/cel-go/ext"
	"google.golang.org/protobuf/types/known/structpb"

	"go.opentelemetry.io/otel/attribute"

	"github.com/bubustack/bobrapet/pkg/observability"
)

const (
	ManifestLengthKey = "__bubu_manifest_len"
	ManifestHashKey   = "__bubu_manifest_hash"
	ManifestTypeKey   = "__bubu_manifest_type"
)

// celEnvLib provides custom functions to the CEL environment.
type celEnvLib struct{}

func (celEnvLib) LibraryName() string {
	return "bubu"
}
func (celEnvLib) ProgramOptions() []cel.ProgramOption {
	return []cel.ProgramOption{}
}
func (celEnvLib) CompileOptions() []cel.EnvOption {
	return []cel.EnvOption{}
}

// Evaluator is responsible for CEL evaluation.
type Config struct {
	EvaluationTimeout   time.Duration
	MaxExpressionLength int
	EnableMacros        *bool
}

type Evaluator struct {
	env               *cel.Env
	cache             *CompilationCache
	evaluationTimeout time.Duration
	maxExprLength     int
	macrosEnabled     bool
}

func baseEnvOptions(enableMacros bool) []cel.EnvOption {
	opts := []cel.EnvOption{
		cel.Lib(celEnvLib{}),
		ext.Strings(),
		ext.Encoders(),
		lenFunction(),
		rangeFunction(),
		sampleFunction(),
		hashOfFunction(),
		typeOfFunction(),
		nowFunction(),
		formatTimeFunction(),
		// Declare top-level variables that can be used in CEL expressions
		cel.Variable(
			"inputs",
			cel.MapType(cel.StringType, cel.AnyType),
		), // Story inputs (from trigger, API, or manual invocation)
		cel.Variable(
			"steps",
			cel.MapType(cel.StringType, cel.AnyType),
		), // Other steps' outputs (batch & realtime)
		cel.Variable(
			"packet",
			cel.MapType(cel.StringType, cel.AnyType),
		), // Current packet metadata (realtime only)
		cel.Variable(
			"item",
			cel.DynType,
		), // Current loop item when evaluating loop templates
		cel.Variable(
			"index",
			cel.IntType,
		), // Loop iteration index (0-based)
		cel.Variable(
			"total",
			cel.IntType,
		), // Total loop iterations
		cel.Variable(
			"now",
			cel.TimestampType,
		), // Current time
	}
	if !enableMacros {
		opts = append(opts, cel.ClearMacros())
	}
	return opts
}

// New creates a new Evaluator.
func New(logger observability.Logger, cfg Config) (*Evaluator, error) {
	enableMacros := true
	if cfg.EnableMacros != nil {
		enableMacros = *cfg.EnableMacros
	}
	env, err := cel.NewEnv(baseEnvOptions(enableMacros)...)
	if err != nil {
		return nil, fmt.Errorf("failed to create CEL environment: %w", err)
	}
	cache := NewCompilationCache(DefaultCacheConfig(), env, logger)
	return &Evaluator{
		env:               env,
		cache:             cache,
		evaluationTimeout: cfg.EvaluationTimeout,
		maxExprLength:     cfg.MaxExpressionLength,
		macrosEnabled:     enableMacros,
	}, nil
}

// Close stops background goroutines and releases resources.
func (e *Evaluator) Close() {
	if e == nil || e.cache == nil {
		return
	}
	e.cache.Stop()
}

// EvaluateWhenCondition evaluates a step's `when` condition.
func (e *Evaluator) EvaluateWhenCondition(ctx context.Context, when string, vars map[string]any) (bool, error) {
	ctx, span := observability.StartSpan(ctx, "cel.EvaluateWhenCondition", attribute.String("expression_type", "when"))
	defer span.End()
	if strings.TrimSpace(when) == "" {
		return true, nil
	}

	if err := e.enforceExpressionLength(when); err != nil {
		return false, err
	}
	sanitized := sanitizeCELExpression(when)
	program, err := e.compile(ctx, sanitized, "when")
	if err != nil {
		return false, fmt.Errorf("failed to compile 'when' expression: %w", err)
	}

	out, err := e.evalProgram(ctx, program, vars, "when")
	if err != nil {
		return false, fmt.Errorf("failed to evaluate 'when' expression: %w", err)
	}

	result, ok := out.Value().(bool)
	if !ok {
		return false, fmt.Errorf("'when' expression did not evaluate to a boolean, got %T", out.Value())
	}

	return result, nil
}

// ResolveWithInputs resolves the `with` block for a step.
func (e *Evaluator) ResolveWithInputs(
	ctx context.Context,
	with map[string]any,
	vars map[string]any,
) (map[string]any, error) {
	ctx, span := observability.StartSpan(ctx, "cel.ResolveWithInputs", attribute.String("expression_type", "with"))
	defer span.End()
	if with == nil {
		return nil, nil
	}

	resolved := make(map[string]any, len(with))
	for key, val := range with {
		out, err := e.resolveWithValue(ctx, val, vars)
		if err != nil {
			return nil, fmt.Errorf("failed to resolve 'with' expression for key '%s': %w", key, err)
		}
		resolved[key] = out
	}
	return resolved, nil
}

func (e *Evaluator) resolveWithValue(ctx context.Context, value any, vars map[string]any) (any, error) {
	switch typed := value.(type) {
	case map[string]any:
		result := make(map[string]any, len(typed))
		for k, v := range typed {
			resolved, err := e.resolveWithValue(ctx, v, vars)
			if err != nil {
				return nil, fmt.Errorf("map key '%s': %w", k, err)
			}
			result[k] = resolved
		}
		return result, nil
	case []any:
		result := make([]any, len(typed))
		for i, elem := range typed {
			resolved, err := e.resolveWithValue(ctx, elem, vars)
			if err != nil {
				return nil, fmt.Errorf("index %d: %w", i, err)
			}
			result[i] = resolved
		}
		return result, nil
	case string:
		return e.resolveWithString(ctx, typed, vars)
	default:
		return value, nil
	}
}

func (e *Evaluator) resolveWithString(ctx context.Context, value string, vars map[string]any) (any, error) {
	trimmed := strings.TrimSpace(value)

	// Check if the entire string is a single CEL expression (legacy behavior)
	if strings.HasPrefix(trimmed, "{{") && strings.HasSuffix(trimmed, "}}") {
		// Check if there are no other {{ }} pairs inside (single expression)
		inner := trimmed[2 : len(trimmed)-2]
		if !strings.Contains(inner, "{{") && !strings.Contains(inner, "}}") {
			return e.evaluateSingleExpression(ctx, inner, vars)
		}
	}

	// GitHub Actions-style template interpolation: "{{ expr }}-suffix" or "prefix-{{ expr }}"
	// Find all {{ ... }} blocks and replace them with evaluated values
	if !strings.Contains(value, "{{") {
		return value, nil
	}

	result := value
	start := 0
	for {
		openIdx := strings.Index(result[start:], "{{")
		if openIdx == -1 {
			break
		}
		openIdx += start

		closeIdx := strings.Index(result[openIdx:], "}}")
		if closeIdx == -1 {
			// Malformed template, return as-is
			return value, nil
		}
		closeIdx += openIdx

		// Extract expression between {{ and }}
		expr := strings.TrimSpace(result[openIdx+2 : closeIdx])
		if expr == "" {
			start = closeIdx + 2
			continue
		}

		// Evaluate the expression
		evalResult, err := e.evaluateSingleExpression(ctx, expr, vars)
		if err != nil {
			return nil, err
		}

		// Convert result to string
		var strResult string
		switch v := evalResult.(type) {
		case string:
			strResult = v
		case nil:
			strResult = ""
		default:
			strResult = fmt.Sprint(v)
		}

		// Replace {{ expr }} with evaluated value
		result = result[:openIdx] + strResult + result[closeIdx+2:]
		start = openIdx + len(strResult)
	}

	return result, nil
}

// evaluateSingleExpression evaluates a single CEL expression (without {{ }})
func (e *Evaluator) evaluateSingleExpression(ctx context.Context, expr string, vars map[string]any) (any, error) {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return "", nil
	}

	if err := e.enforceExpressionLength(expr); err != nil {
		return nil, err
	}
	sanitized := sanitizeCELExpression(expr)
	program, err := e.compile(ctx, sanitized, "with")
	if err != nil {
		return nil, fmt.Errorf("failed to compile expression '%s': %w", expr, err)
	}

	out, err := e.evalProgram(ctx, program, vars, "with")
	if err != nil {
		if shouldBlockForMissingStepOutputs(expr, err) {
			return nil, &ErrEvaluationBlocked{
				Reason: fmt.Sprintf("awaiting upstream data referenced by %s: %v", expr, err),
			}
		}
		return nil, fmt.Errorf("failed to evaluate expression '%s': %w", expr, err)
	}

	return celValueToInterface(out)
}

func celValueToInterface(val ref.Val) (any, error) {
	if val == nil || val == types.NullValue {
		return nil, nil
	}
	native, err := val.ConvertToNative(reflect.TypeOf(&structpb.Value{}))
	if err == nil {
		return native.(*structpb.Value).AsInterface(), nil
	}
	// Fall back to direct value when StructPB conversion is not supported (e.g. ints, bools)
	return val.Value(), nil
}

func (e *Evaluator) compile(ctx context.Context, expr, exprType string) (cel.Program, error) {
	ctx, span := observability.StartSpan(ctx, "cel.compile", attribute.String("expression_type", exprType))
	defer span.End()
	cached, err := e.cache.CompileAndCache(ctx, expr, exprType)
	if err != nil {
		if isUndeclaredLoopVarError(err) {
			fallback, fallbackErr := compileWithLoopVars(expr, e.macrosEnabled)
			if fallbackErr == nil {
				return fallback, nil
			}
		}
		return nil, err
	}
	return cached.Program, nil
}

func compileWithLoopVars(expr string, macrosEnabled bool) (cel.Program, error) {
	env, err := cel.NewEnv(baseEnvOptions(macrosEnabled)...)
	if err != nil {
		return nil, err
	}
	ast, issues := env.Compile(expr)
	if issues != nil && issues.Err() != nil {
		return nil, issues.Err()
	}
	return env.Program(ast)
}

func nowFunction() cel.EnvOption {
	return cel.Function("now",
		cel.Overload("now_timestamp",
			[]*cel.Type{},
			cel.TimestampType,
			cel.FunctionBinding(func(values ...ref.Val) ref.Val {
				return types.Timestamp{Time: time.Now()}
			})))
}

func formatTimeFunction() cel.EnvOption {
	return cel.Function("format_time",
		cel.MemberOverload("timestamp_format_string",
			[]*cel.Type{cel.TimestampType, cel.StringType},
			cel.StringType,
			cel.BinaryBinding(formatTime)))
}

func isUndeclaredLoopVarError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "undeclared reference to 'item'") ||
		strings.Contains(msg, "undeclared reference to 'index'") ||
		strings.Contains(msg, "undeclared reference to 'total'")
}

func lenFunction() cel.EnvOption {
	return cel.Function("len",
		cel.Overload(
			"len_dyn",
			[]*cel.Type{cel.AnyType},
			cel.IntType,
			cel.UnaryBinding(func(value ref.Val) ref.Val {
				length, err := celLengthFromRefVal(value)
				if err != nil {
					return types.NewErr("%s", err.Error())
				}
				return types.Int(length)
			}),
		),
	)
}

func sampleFunction() cel.EnvOption {
	return cel.Function("sample",
		cel.Overload(
			"sample_dyn",
			[]*cel.Type{cel.AnyType},
			cel.AnyType,
			cel.UnaryBinding(func(value ref.Val) ref.Val {
				return value
			}),
		),
	)
}

func hashOfFunction() cel.EnvOption {
	return cel.Function("hash_of",
		cel.Overload(
			"hash_of_dyn",
			[]*cel.Type{cel.AnyType},
			cel.StringType,
			cel.UnaryBinding(func(value ref.Val) ref.Val {
				if mapper, ok := value.(traits.Mapper); ok {
					if hash, ok := manifestHashFromMapper(mapper); ok {
						return types.String(hash)
					}
				}
				hash, err := hashFromNativeValue(value.Value())
				if err != nil {
					return types.NewErr("%s", err.Error())
				}
				return types.String(hash)
			}),
		),
	)
}

func typeOfFunction() cel.EnvOption {
	return cel.Function("type_of",
		cel.Overload(
			"type_of_dyn",
			[]*cel.Type{cel.AnyType},
			cel.StringType,
			cel.UnaryBinding(func(value ref.Val) ref.Val {
				if mapper, ok := value.(traits.Mapper); ok {
					if typed, ok := manifestTypeFromMapper(mapper); ok {
						return types.String(typed)
					}
				}
				return types.String(typeFromNativeValue(value.Value()))
			}),
		),
	)
}

func rangeFunction() cel.EnvOption {
	return cel.Function("range",
		cel.Overload(
			"range_int",
			[]*cel.Type{cel.IntType},
			cel.ListType(cel.IntType),
			cel.UnaryBinding(func(value ref.Val) ref.Val {
				count, ok := value.Value().(int64)
				if !ok {
					return types.NewErr("range requires an integer")
				}
				return rangeValues(0, count)
			}),
		),
		cel.Overload(
			"range_double",
			[]*cel.Type{cel.DoubleType},
			cel.ListType(cel.IntType),
			cel.UnaryBinding(func(value ref.Val) ref.Val {
				count, ok := value.Value().(float64)
				if !ok {
					return types.NewErr("range requires a number")
				}
				ival, err := coerceRangeBound(count)
				if err != nil {
					return types.NewErr("%s", err.Error())
				}
				return rangeValues(0, ival)
			}),
		),
		cel.Overload(
			"range_int_int",
			[]*cel.Type{cel.IntType, cel.IntType},
			cel.ListType(cel.IntType),
			cel.BinaryBinding(func(startVal, endVal ref.Val) ref.Val {
				start, ok := startVal.Value().(int64)
				if !ok {
					return types.NewErr("range start must be an integer")
				}
				end, ok := endVal.Value().(int64)
				if !ok {
					return types.NewErr("range end must be an integer")
				}
				return rangeValues(start, end)
			}),
		),
		cel.Overload(
			"range_int_double",
			[]*cel.Type{cel.IntType, cel.DoubleType},
			cel.ListType(cel.IntType),
			cel.BinaryBinding(func(startVal, endVal ref.Val) ref.Val {
				start, ok := startVal.Value().(int64)
				if !ok {
					return types.NewErr("range start must be an integer")
				}
				end, ok := endVal.Value().(float64)
				if !ok {
					return types.NewErr("range end must be a number")
				}
				ival, err := coerceRangeBound(end)
				if err != nil {
					return types.NewErr("%s", err.Error())
				}
				return rangeValues(start, ival)
			}),
		),
		cel.Overload(
			"range_double_int",
			[]*cel.Type{cel.DoubleType, cel.IntType},
			cel.ListType(cel.IntType),
			cel.BinaryBinding(func(startVal, endVal ref.Val) ref.Val {
				start, ok := startVal.Value().(float64)
				if !ok {
					return types.NewErr("range start must be a number")
				}
				ival, err := coerceRangeBound(start)
				if err != nil {
					return types.NewErr("%s", err.Error())
				}
				end, ok := endVal.Value().(int64)
				if !ok {
					return types.NewErr("range end must be an integer")
				}
				return rangeValues(ival, end)
			}),
		),
		cel.Overload(
			"range_double_double",
			[]*cel.Type{cel.DoubleType, cel.DoubleType},
			cel.ListType(cel.IntType),
			cel.BinaryBinding(func(startVal, endVal ref.Val) ref.Val {
				start, ok := startVal.Value().(float64)
				if !ok {
					return types.NewErr("range start must be a number")
				}
				end, ok := endVal.Value().(float64)
				if !ok {
					return types.NewErr("range end must be a number")
				}
				istart, err := coerceRangeBound(start)
				if err != nil {
					return types.NewErr("%s", err.Error())
				}
				iend, err := coerceRangeBound(end)
				if err != nil {
					return types.NewErr("%s", err.Error())
				}
				return rangeValues(istart, iend)
			}),
		),
	)
}

func coerceRangeBound(value float64) (int64, error) {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return 0, fmt.Errorf("range requires a finite number")
	}
	if value < 0 {
		return 0, fmt.Errorf("range requires non-negative bounds")
	}
	if math.Mod(value, 1) != 0 {
		return 0, fmt.Errorf("range requires whole numbers")
	}
	return int64(value), nil
}

func rangeValues(start, end int64) ref.Val {
	if start < 0 || end < 0 {
		return types.NewErr("range requires non-negative bounds")
	}
	if end <= start {
		return types.NewDynamicList(types.DefaultTypeAdapter, []ref.Val{})
	}
	out := make([]ref.Val, 0, end-start)
	for i := start; i < end; i++ {
		out = append(out, types.Int(i))
	}
	return types.NewDynamicList(types.DefaultTypeAdapter, out)
}

func celLengthFromRefVal(value ref.Val) (int64, error) {
	if value == nil || value == types.NullValue {
		return 0, nil
	}

	if mapper, ok := value.(traits.Mapper); ok {
		if length, ok := manifestLengthFromMapper(mapper); ok {
			return length, nil
		}
	}

	if sizer, ok := value.(traits.Sizer); ok {
		if length, ok := lengthFromSizer(sizer); ok {
			return length, nil
		}
	}

	if length, ok := lengthFromNativeValue(value.Value()); ok {
		return length, nil
	}

	return 0, fmt.Errorf("len: unsupported argument type %s", value.Type())
}

func manifestLengthFromMapper(mapper traits.Mapper) (int64, bool) {
	manifestVal, found := mapper.Find(types.String(ManifestLengthKey))
	if !found || manifestVal == nil || manifestVal == types.NullValue {
		return 0, false
	}
	return coerceRefValToInt64(manifestVal)
}

func manifestHashFromMapper(mapper traits.Mapper) (string, bool) {
	manifestVal, found := mapper.Find(types.String(ManifestHashKey))
	if !found || manifestVal == nil || manifestVal == types.NullValue {
		return "", false
	}
	if str, ok := manifestVal.(types.String); ok {
		return string(str), true
	}
	if native, err := manifestVal.ConvertToNative(reflect.TypeOf("")); err == nil {
		if typed, ok := native.(string); ok {
			return typed, true
		}
	}
	return "", false
}

func manifestTypeFromMapper(mapper traits.Mapper) (string, bool) {
	manifestVal, found := mapper.Find(types.String(ManifestTypeKey))
	if !found || manifestVal == nil || manifestVal == types.NullValue {
		return "", false
	}
	if str, ok := manifestVal.(types.String); ok {
		return string(str), true
	}
	if native, err := manifestVal.ConvertToNative(reflect.TypeOf("")); err == nil {
		if typed, ok := native.(string); ok {
			return typed, true
		}
	}
	return "", false
}

func lengthFromSizer(sizer traits.Sizer) (int64, bool) {
	size := sizer.Size()
	if size == nil || size == types.NullValue {
		return 0, true
	}
	if intVal, ok := size.(types.Int); ok {
		return int64(intVal), true
	}
	if native, err := size.ConvertToNative(reflect.TypeOf(int64(0))); err == nil {
		if typed, ok := native.(int64); ok {
			return typed, true
		}
	}
	if converted, err := size.ConvertToNative(reflect.TypeOf(int(0))); err == nil {
		if typed, ok := converted.(int); ok {
			return int64(typed), true
		}
	}
	return coerceToInt64(size.Value())
}

func lengthFromNativeValue(value any) (int64, bool) {
	switch v := value.(type) {
	case string:
		return int64(len(v)), true
	case []byte:
		return int64(len(v)), true
	case []any:
		return int64(len(v)), true
	case map[string]any:
		if length, ok := coerceToInt64(v[ManifestLengthKey]); ok {
			return length, true
		}
		return int64(len(v)), true
	case map[any]any:
		if length, ok := coerceToInt64(v[ManifestLengthKey]); ok {
			return length, true
		}
		return int64(len(v)), true
	default:
		return 0, false
	}
}

func hashFromNativeValue(value any) (string, error) {
	if value == nil {
		return "", fmt.Errorf("hash_of: value is null")
	}
	switch v := value.(type) {
	case string:
		return hashString(v), nil
	case []byte:
		return hashBytes(v), nil
	default:
		return "", fmt.Errorf("hash_of: unsupported argument type %T", value)
	}
}

func hashString(value string) string {
	sum := sha256.Sum256([]byte(value))
	return fmt.Sprintf("%x", sum)
}

func hashBytes(value []byte) string {
	sum := sha256.Sum256(value)
	return fmt.Sprintf("%x", sum)
}

func typeFromNativeValue(value any) string {
	if value == nil {
		return "null"
	}
	switch value.(type) {
	case map[string]any, map[any]any:
		return "object"
	case []any:
		return "array"
	case string:
		return "string"
	case bool:
		return "bool"
	case float32, float64, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return "number"
	}
	rv := reflect.ValueOf(value)
	if !rv.IsValid() {
		return "null"
	}
	for rv.Kind() == reflect.Pointer {
		if rv.IsNil() {
			return "null"
		}
		rv = rv.Elem()
	}
	switch rv.Kind() {
	case reflect.Map, reflect.Struct:
		return "object"
	case reflect.Slice, reflect.Array:
		return "array"
	case reflect.String:
		return "string"
	case reflect.Bool:
		return "bool"
	case reflect.Float32, reflect.Float64, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return "number"
	default:
		return rv.Kind().String()
	}
}

func coerceRefValToInt64(val ref.Val) (int64, bool) {
	if val == nil || val == types.NullValue {
		return 0, false
	}
	if intVal, ok := val.(types.Int); ok {
		return int64(intVal), true
	}
	if native, err := val.ConvertToNative(reflect.TypeOf(int64(0))); err == nil {
		if typed, ok := native.(int64); ok {
			return typed, true
		}
	}
	return coerceToInt64(val.Value())
}

func coerceToInt64(value any) (int64, bool) {
	switch v := value.(type) {
	case int:
		return int64(v), true
	case int32:
		return int64(v), true
	case int64:
		return v, true
	case float32:
		return int64(v), true
	case float64:
		return int64(v), true
	case uint:
		return int64(v), true
	case uint32:
		return int64(v), true
	case uint64:
		if v > math.MaxInt64 {
			return 0, false
		}
		return int64(v), true
	case types.Int:
		return int64(v), true
	case *int64:
		if v == nil {
			return 0, false
		}
		return *v, true
	default:
		return 0, false
	}
}

var (
	stepReferencePattern = regexp.MustCompile(`steps\.([A-Za-z0-9_\-]+)`)
)

func sanitizeCELExpression(expr string) string {
	return stepReferencePattern.ReplaceAllStringFunc(expr, func(match string) string {
		submatches := stepReferencePattern.FindStringSubmatch(match)
		if len(submatches) != 2 {
			return match
		}
		original := submatches[1]
		alias := sanitizeIdentifier(original)
		if alias == original {
			return match
		}
		return strings.Replace(match, original, alias, 1)
	})
}

func (e *Evaluator) enforceExpressionLength(expr string) error {
	if e == nil {
		return nil
	}
	if e.maxExprLength > 0 && len(expr) > e.maxExprLength {
		return fmt.Errorf("cel expression exceeds max length of %d characters", e.maxExprLength)
	}
	return nil
}

func (e *Evaluator) evalProgram(
	ctx context.Context,
	program cel.Program,
	vars map[string]any,
	exprType string,
) (ref.Val, error) {
	if e == nil || e.evaluationTimeout <= 0 {
		val, _, err := program.Eval(vars)
		return val, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	timedCtx, cancel := context.WithTimeout(ctx, e.evaluationTimeout)
	defer cancel()
	type evalResult struct {
		val ref.Val
		err error
	}
	resultCh := make(chan evalResult, 1)
	go func() {
		val, _, err := program.Eval(vars)
		resultCh <- evalResult{val: val, err: err}
	}()
	select {
	case <-timedCtx.Done():
		if errors.Is(timedCtx.Err(), context.DeadlineExceeded) {
			return nil, fmt.Errorf("cel evaluation timed out after %s (%s)", e.evaluationTimeout, exprType)
		}
		return nil, fmt.Errorf("cel evaluation cancelled: %w", timedCtx.Err())
	case res := <-resultCh:
		return res.val, res.err
	}
}

func sanitizeIdentifier(name string) string {
	var b strings.Builder
	b.Grow(len(name))
	for _, r := range name {
		if unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_' {
			b.WriteRune(r)
		} else {
			b.WriteRune('_')
		}
	}
	return b.String()
}

func shouldBlockForMissingStepOutputs(expr string, evalErr error) bool {
	expr = strings.TrimSpace(expr)
	if expr == "" || evalErr == nil {
		return false
	}
	if !referencesStepsContext(expr) {
		return false
	}
	errMsg := evalErr.Error()
	return strings.Contains(errMsg, "no such key") ||
		strings.Contains(errMsg, "no such attribute") ||
		strings.Contains(errMsg, "undefined field")
}

func referencesStepsContext(expr string) bool {
	if expr == "" {
		return false
	}
	normalized := strings.ReplaceAll(expr, " ", "")
	return strings.Contains(normalized, "steps[") || strings.Contains(normalized, "steps.")
}
