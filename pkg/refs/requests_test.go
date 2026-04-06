package refs

import (
	"reflect"
	"testing"

	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

func TestRequestsFromNames(t *testing.T) {
	t.Run("dedupes and trims", func(t *testing.T) {
		requests := RequestsFromNames(" transport-a ", "transport-b", "transport-a", " ")
		want := []reconcile.Request{
			{NamespacedName: types.NamespacedName{Name: "transport-a"}},
			{NamespacedName: types.NamespacedName{Name: "transport-b"}},
		}
		if !reflect.DeepEqual(requests, want) {
			t.Fatalf("RequestsFromNames() = %#v, want %#v", requests, want)
		}
	})

	t.Run("returns nil when no valid names", func(t *testing.T) {
		if got := RequestsFromNames("", "   "); got != nil {
			t.Fatalf("RequestsFromNames() = %#v, want nil", got)
		}
	})
}
