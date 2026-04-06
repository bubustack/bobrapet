package storage

import (
	"context"
	"strings"
	"testing"
)

func TestValidateStorageRefRejectsTraversalSegments(t *testing.T) {
	t.Helper()
	cases := []string{
		"safe/..",
		"../outside",
		"safe/../more",
	}

	for _, path := range cases {
		if err := validateStorageRef(path); err == nil {
			t.Fatalf("expected %q to be rejected as traversal, got nil", path)
		}
	}
}

func TestStorageManager_WriteBlobRejectsTraversalPath(t *testing.T) {
	sm := &StorageManager{store: newMockStore()}
	err := sm.WriteBlob(context.Background(), "safe/..", "text/plain", []byte("data"))
	if err == nil || !strings.Contains(err.Error(), "invalid storage path") {
		t.Fatalf("expected WriteBlob to reject traversal path, got %v", err)
	}
}

func TestStorageManager_ReadBlobRejectsTraversalPath(t *testing.T) {
	sm := &StorageManager{store: newMockStore()}
	_, err := sm.ReadBlob(context.Background(), "safe/..")
	if err == nil || !strings.Contains(err.Error(), "invalid storage path") {
		t.Fatalf("expected ReadBlob to reject traversal path, got %v", err)
	}
}

func TestStorageManager_HydrateFromTraversalRefFails(t *testing.T) {
	mock := newMockStore()
	mock.data["safe/.."] = []byte(`{"contentType":"json","data":"{}"}`)
	sm := &StorageManager{store: mock}

	if _, err := sm.hydrateFromStorageRef(context.Background(), "safe/..", 0, false); err == nil {
		t.Fatalf("expected hydrateFromStorageRef to reject traversal path")
	}
}
