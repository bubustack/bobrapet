package v1alpha1

import "testing"

func TestValidateReferenceMaps(t *testing.T) {
	tests := []struct {
		name    string
		doc     string
		wantErr bool
	}{
		{
			name: "valid storage ref with metadata",
			doc:  `{"output":{"$bubuStorageRef":"outputs/run/output.json","$bubuStorageContentType":"application/json","$bubuStorageSchema":"com.example","$bubuStorageSchemaVersion":"v1"}}`,
		},
		{
			name:    "storage ref with extra key",
			doc:     `{"$bubuStorageRef":"outputs/run/output.json","extra":true}`,
			wantErr: true,
		},
		{
			name: "valid secret ref string",
			doc:  `{"secret":{"$bubuSecretRef":"ns/secret:key"}}`,
		},
		{
			name:    "invalid secret ref missing key",
			doc:     `{"$bubuSecretRef":{"name":"secret"}}`,
			wantErr: true,
		},
		{
			name: "valid configmap ref map",
			doc:  `{"$bubuConfigMapRef":{"name":"cfg","key":"value","format":"json"}}`,
		},
		{
			name:    "invalid ref format",
			doc:     `{"$bubuSecretRef":{"name":"secret","key":"value","format":"xml"}}`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateReferenceMaps([]byte(tt.doc), "test")
			if tt.wantErr && err == nil {
				t.Fatalf("expected error, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}
