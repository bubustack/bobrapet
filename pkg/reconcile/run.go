package reconcile

import (
	"context"
	"fmt"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// RunOptions configures the shared reconcile runner.
type RunOptions struct {
	// Controller is the name recorded in reconcile metrics.
	Controller string
	// Timeout bounds each reconcile loop; zero disables the timeout.
	Timeout time.Duration
}

// RunHooks defines controller-specific callbacks executed by Run.
type RunHooks[T client.Object] struct {
	// Get must fetch the latest object for the reconcile request.
	Get func(ctx context.Context, req ctrl.Request) (T, error)
	// HandleDeletion may short-circuit reconciliation when the object is deleting.
	// When handled is true, Run returns the provided result/error immediately.
	HandleDeletion func(ctx context.Context, obj T) (handled bool, result ctrl.Result, err error)
	// EnsureFinalizer adds controller-specific finalizers when needed.
	EnsureFinalizer func(ctx context.Context, obj T) error
	// HandleNormal performs the controller-specific reconciliation once the
	// object is fetched, deletion is handled, and finalizers are ensured.
	HandleNormal func(ctx context.Context, obj T) (ctrl.Result, error)
}

// Run wraps common reconcile scaffolding (timeout, metrics, fetch, deletion
// handling, and finalizer enforcement) while delegating controller-specific
// logic to the provided hooks.
func Run[T client.Object](
	ctx context.Context,
	req ctrl.Request,
	opts RunOptions,
	hooks RunHooks[T],
) (res ctrl.Result, err error) {
	if hooks.Get == nil {
		return ctrl.Result{}, fmt.Errorf("reconcile hooks must provide Get")
	}

	ctx, finish := StartControllerReconcile(ctx, opts.Controller, opts.Timeout)
	defer func() { finish(err) }()

	obj, err := hooks.Get(ctx, req)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}
	if hooks.HandleDeletion != nil {
		handled, delResult, delErr := hooks.HandleDeletion(ctx, obj)
		if handled || delErr != nil {
			return delResult, delErr
		}
	}

	if hooks.EnsureFinalizer != nil {
		if err := hooks.EnsureFinalizer(ctx, obj); err != nil {
			return ctrl.Result{}, err
		}
	}

	if hooks.HandleNormal == nil {
		return ctrl.Result{}, nil
	}
	return hooks.HandleNormal(ctx, obj)
}
