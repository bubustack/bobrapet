package cel

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"regexp"
	"strings"
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

const ManifestLengthKey = "__bubu_manifest_len"

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
type Evaluator struct {
	env   *cel.Env
	cache *CompilationCache
}

// New creates a new Evaluator.
func New(logger observability.Logger) (*Evaluator, error) {
	env, err := cel.NewEnv(
		cel.Lib(celEnvLib{}),
		ext.Strings(),
		ext.Encoders(),
		lenFunction(),
		// Declare top-level variables that can be used in expressions.
		cel.Variable("inputs", cel.MapType(cel.StringType, cel.AnyType)),
		cel.Variable("steps", cel.MapType(cel.StringType, cel.AnyType)),
		cel.Variable("payload", cel.MapType(cel.StringType, cel.AnyType)),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create CEL environment: %w", err)
	}
	cache := NewCompilationCache(DefaultCacheConfig(), env, logger)
	return &Evaluator{env: env, cache: cache}, nil
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

	sanitized := sanitizeCELExpression(when)
	program, err := e.compile(ctx, sanitized, "when")
	if err != nil {
		return false, fmt.Errorf("failed to compile 'when' expression: %w", err)
	}

	out, _, err := program.Eval(vars)
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

	resolved := make(map[string]any)
	for key, val := range with {
		strVal, ok := val.(string)
		if !ok {
			resolved[key] = val
			continue
		}

		if strings.HasPrefix(strVal, "{{") && strings.HasSuffix(strVal, "}}") {
			expr := strings.TrimSpace(strVal[2 : len(strVal)-2])
			sanitized := sanitizeCELExpression(expr)
			program, err := e.compile(ctx, sanitized, "with")
			if err != nil {
				return nil, fmt.Errorf("failed to compile 'with' expression for key '%s': %w", key, err)
			}

			out, _, err := program.Eval(vars)
			if err != nil {
				return nil, fmt.Errorf("failed to evaluate 'with' expression for key '%s': %w", key, err)
			}

			// Convert CEL output to a standard Go type.
			nativeValue, err := out.ConvertToNative(reflect.TypeOf(&structpb.Value{}))
			if err != nil {
				return nil, fmt.Errorf("failed to convert CEL output for key '%s': %w", key, err)
			}
			resolved[key] = nativeValue.(*structpb.Value).AsInterface()
		} else {
			resolved[key] = strVal
		}
	}
	return resolved, nil
}

func (e *Evaluator) compile(ctx context.Context, expr, exprType string) (cel.Program, error) {
	ctx, span := observability.StartSpan(ctx, "cel.compile", attribute.String("expression_type", exprType))
	defer span.End()
	cached, err := e.cache.CompileAndCache(ctx, expr, exprType)
	if err != nil {
		return nil, err
	}
	return cached.Program, nil
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
