package config

import (
	"strings"
	"testing"
)

func TestNewOperatorConfigManagerReturnsConstructorError(t *testing.T) {
	t.Parallel()

	manager, err := NewOperatorConfigManager(nil, "default", "cfg")
	if err == nil {
		t.Fatal("expected constructor error")
	}
	if manager != nil {
		t.Fatalf("expected nil manager on constructor error, got %#v", manager)
	}
	if !strings.Contains(err.Error(), "Client must be provided") {
		t.Fatalf("expected wrapped client validation error, got %v", err)
	}
}

func TestMustNewOperatorConfigManagerPanicsOnConstructorError(t *testing.T) {
	t.Parallel()

	defer func() {
		recovered := recover()
		if recovered == nil {
			t.Fatal("expected panic from must constructor")
		}
		if !strings.Contains(recovered.(error).Error(), "Client must be provided") {
			t.Fatalf("expected wrapped client validation error, got %v", recovered)
		}
	}()

	_ = MustNewOperatorConfigManager(nil, "default", "cfg")
}
