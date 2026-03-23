package validation

import (
	"testing"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
)

func TestValidateDownstreamTargets(t *testing.T) {
	tests := []struct {
		name    string
		targets []runsv1alpha1.DownstreamTarget
		wantErr bool
	}{
		{
			name:    "empty slice",
			targets: nil,
			wantErr: false,
		},
		{
			name: "valid grpc target",
			targets: []runsv1alpha1.DownstreamTarget{
				{GRPCTarget: &runsv1alpha1.GRPCTarget{Endpoint: "svc:80"}},
			},
			wantErr: false,
		},
		{
			name: "valid terminate target",
			targets: []runsv1alpha1.DownstreamTarget{
				{Terminate: &runsv1alpha1.TerminateTarget{}},
			},
			wantErr: false,
		},
		{
			name: "invalid missing transports",
			targets: []runsv1alpha1.DownstreamTarget{
				{},
			},
			wantErr: true,
		},
		{
			name: "invalid multiple transports",
			targets: []runsv1alpha1.DownstreamTarget{
				{
					GRPCTarget: &runsv1alpha1.GRPCTarget{Endpoint: "svc:80"},
					Terminate:  &runsv1alpha1.TerminateTarget{},
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateDownstreamTargets(tt.targets)
			if tt.wantErr && err == nil {
				t.Fatalf("expected error but got nil")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}
