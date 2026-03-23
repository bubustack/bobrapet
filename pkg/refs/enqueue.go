package refs

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// EnqueueByField lists objects using the provided indexed field selector and returns
// reconcile requests for each item. Callers can pass additional list options (e.g.,
// namespace filters) via opts.
func EnqueueByField(
	ctx context.Context,
	reader client.Reader,
	list client.ObjectList,
	field string,
	value string,
	opts ...client.ListOption,
) ([]reconcile.Request, error) {
	options := append([]client.ListOption{client.MatchingFields{field: value}}, opts...)
	if err := reader.List(ctx, list, options...); err != nil {
		return nil, err
	}

	reqs := make([]reconcile.Request, 0)
	err := meta.EachListItem(list, func(obj runtime.Object) error {
		k8sObj, ok := obj.(client.Object)
		if !ok {
			return fmt.Errorf("object %T does not implement client.Object", obj)
		}
		reqs = append(reqs, reconcile.Request{
			NamespacedName: types.NamespacedName{
				Name:      k8sObj.GetName(),
				Namespace: k8sObj.GetNamespace(),
			},
		})
		return nil
	})
	if err != nil {
		return nil, err
	}

	return reqs, nil
}
