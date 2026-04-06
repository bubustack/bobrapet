package reconcile

import (
	"context"
	"fmt"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// PipelineHooks defines the callbacks required to run the standard
// prepare → ensure → finalize reconcile pipeline.
type PipelineHooks[T client.Object, Prepared any] struct {
	// Prepare performs controller-specific preparation work and may
	// short-circuit reconciliation by returning a ctrl.Result.
	// When Prepare returns a non-nil ctrl.Result, the pipeline exits immediately.
	Prepare func(ctx context.Context, obj T) (Prepared, *ctrl.Result, error)
	// Ensure performs the primary reconciliation using the prepared context.
	Ensure func(ctx context.Context, obj T, prepared Prepared) (ctrl.Result, error)
	// Finalize is invoked regardless of Ensure success. When nil, the pipeline
	// simply returns Ensure's result/error.
	Finalize func(
		ctx context.Context,
		obj T,
		prepared Prepared,
		ensureResult ctrl.Result,
		ensureErr error,
	) (ctrl.Result, error)
}

// RunPipeline executes the shared prepare → ensure → finalize pipeline.
func RunPipeline[T client.Object, Prepared any](
	ctx context.Context,
	obj T,
	hooks PipelineHooks[T, Prepared],
) (ctrl.Result, error) {
	if hooks.Prepare == nil {
		return ctrl.Result{}, fmt.Errorf("pipeline hooks must provide Prepare")
	}
	if hooks.Ensure == nil {
		return ctrl.Result{}, fmt.Errorf("pipeline hooks must provide Ensure")
	}

	prepared, shortCircuit, err := hooks.Prepare(ctx, obj)
	if err != nil {
		return ctrl.Result{}, err
	}
	if shortCircuit != nil {
		return *shortCircuit, nil
	}

	result, ensureErr := hooks.Ensure(ctx, obj, prepared)
	if hooks.Finalize == nil {
		return result, ensureErr
	}
	return hooks.Finalize(ctx, obj, prepared, result, ensureErr)
}
