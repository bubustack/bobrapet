package refs

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// CountByField lists objects using the provided field selector and returns the count.
// list must be a pointer to a controller-runtime List type (e.g. *v1alpha1.StoryList).
func CountByField(
	ctx context.Context,
	c client.Client,
	list client.ObjectList,
	fieldKey string,
	fieldValue string,
	extraOpts ...client.ListOption,
) (int, error) {
	return CountByFieldFiltered(ctx, c, list, fieldKey, fieldValue, nil, extraOpts...)
}

// CountByFieldFiltered behaves like CountByField but applies an optional predicate to each
// returned object before counting. The predicate receives the concrete client.Object and
// should return true when the item contributes to the count.
func CountByFieldFiltered(
	ctx context.Context,
	c client.Client,
	list client.ObjectList,
	fieldKey string,
	fieldValue string,
	filter func(client.Object) bool,
	extraOpts ...client.ListOption,
) (int, error) {
	options := make([]client.ListOption, 0, 1+len(extraOpts))
	options = append(options, client.MatchingFields{fieldKey: fieldValue})
	options = append(options, extraOpts...)
	if err := c.List(ctx, list, options...); err != nil {
		return 0, err
	}
	if filter == nil {
		return meta.LenList(list), nil
	}
	count := 0
	if err := meta.EachListItem(list, func(obj runtime.Object) error {
		item, ok := obj.(client.Object)
		if !ok {
			return fmt.Errorf("list item does not implement client.Object")
		}
		if filter(item) {
			count++
		}
		return nil
	}); err != nil {
		return 0, err
	}
	return count, nil
}
