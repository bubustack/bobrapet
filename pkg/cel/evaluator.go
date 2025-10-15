package cel

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/ext"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/bubustack/bobrapet/pkg/observability"
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
	if strings.TrimSpace(when) == "" {
		return true, nil
	}

	program, err := e.compile(ctx, when, "when")
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
			program, err := e.compile(ctx, expr, "with")
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
	cached, err := e.cache.CompileAndCache(ctx, expr, exprType)
	if err != nil {
		return nil, err
	}
	return cached.Program, nil
}
