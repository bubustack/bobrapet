/*
Copyright 2025 BubuStack.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package runs

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	catalogv1alpha1 "github.com/bubustack/bobrapet/api/catalog/v1alpha1"
	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	bubuv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/internal/config"
	"github.com/bubustack/bobrapet/pkg/logging"
	"github.com/bubustack/bobrapet/pkg/refs"
)

// newDeclaredKeysScheme creates a scheme with all types needed for
// checkDeclaredOutputKeys tests (including corev1 for Event creation).
func newDeclaredKeysScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, runsv1alpha1.AddToScheme(scheme))
	require.NoError(t, bubuv1alpha1.AddToScheme(scheme))
	require.NoError(t, catalogv1alpha1.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))
	return scheme
}

func newTestStep(t *testing.T, output map[string]any) (*catalogv1alpha1.EngramTemplate, *bubuv1alpha1.Engram, *runsv1alpha1.StepRun) {
	t.Helper()
	template := &catalogv1alpha1.EngramTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: "tpl"},
	}
	engram := &bubuv1alpha1.Engram{
		ObjectMeta: metav1.ObjectMeta{Name: "engram", Namespace: "default"},
		Spec: bubuv1alpha1.EngramSpec{
			TemplateRef: refs.EngramTemplateReference{Name: "tpl"},
		},
	}
	var rawOutput *runtime.RawExtension
	if output != nil {
		b, err := json.Marshal(output)
		require.NoError(t, err)
		rawOutput = &runtime.RawExtension{Raw: b}
	}
	step := &runsv1alpha1.StepRun{
		ObjectMeta: metav1.ObjectMeta{Name: "step", Namespace: "default"},
		Spec: runsv1alpha1.StepRunSpec{
			StepID:      "step",
			StoryRunRef: refs.StoryRunReference{ObjectReference: refs.ObjectReference{Name: "sr"}},
			EngramRef:   &refs.EngramReference{ObjectReference: refs.ObjectReference{Name: "engram"}},
		},
		Status: runsv1alpha1.StepRunStatus{
			Output: rawOutput,
		},
	}
	return template, engram, step
}

func TestCheckDeclaredOutputKeys(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		declaredKeys []string
		outputKeys   map[string]any
		wantWarning  bool
		wantMissing  string // substring to look for in event message
		wantExtra    string // substring to look for in event message
	}{
		{
			name:         "matching keys emits no warning",
			declaredKeys: []string{"body", "status"},
			outputKeys:   map[string]any{"body": "ok", "status": 200},
			wantWarning:  false,
		},
		{
			name:         "missing key emits warning",
			declaredKeys: []string{"body", "status", "headers"},
			outputKeys:   map[string]any{"body": "ok", "status": 200},
			wantWarning:  true,
			wantMissing:  "headers",
		},
		{
			name:         "extra key emits warning",
			declaredKeys: []string{"body"},
			outputKeys:   map[string]any{"body": "ok", "extra": true},
			wantWarning:  true,
			wantExtra:    "extra",
		},
		{
			name:         "both missing and extra keys",
			declaredKeys: []string{"body", "headers"},
			outputKeys:   map[string]any{"body": "ok", "unexpected": 1},
			wantWarning:  true,
			wantMissing:  "headers",
			wantExtra:    "unexpected",
		},
		{
			name:         "no declared keys skips check",
			declaredKeys: nil,
			outputKeys:   map[string]any{"anything": 1},
			wantWarning:  false,
		},
		{
			name:         "nil output skips check",
			declaredKeys: []string{"body"},
			outputKeys:   nil,
			wantWarning:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			scheme := newDeclaredKeysScheme(t)

			template, engram, step := newTestStep(t, tt.outputKeys)
			template.Spec.DeclaredOutputKeys = tt.declaredKeys

			cl := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(step, engram, template)
			fakeClient := cl.Build()

			reconciler := &StepRunReconciler{
				ControllerDependencies: config.ControllerDependencies{
					Client: fakeClient,
					Scheme: scheme,
				},
				Recorder: events.NewFakeRecorder(10),
			}

			ctx := context.Background()
			logger := logging.NewControllerLogger(ctx, "test").WithStepRun(step)
			reconciler.checkDeclaredOutputKeys(ctx, step, logger)

			var events corev1.EventList
			require.NoError(t, fakeClient.List(ctx, &events))
			var warnings []corev1.Event
			for _, ev := range events.Items {
				if ev.Type == corev1.EventTypeWarning && ev.Reason == eventReasonOutputKeyMismatch {
					warnings = append(warnings, ev)
				}
			}

			if !tt.wantWarning {
				assert.Empty(t, warnings, "expected no OutputKeyMismatch warning events")
				return
			}

			require.Len(t, warnings, 1, "expected exactly one OutputKeyMismatch warning event")
			msg := warnings[0].Message
			if tt.wantMissing != "" {
				assert.Contains(t, msg, tt.wantMissing, "event message should mention missing key")
			}
			if tt.wantExtra != "" {
				assert.Contains(t, msg, tt.wantExtra, "event message should mention extra key")
			}
		})
	}
}
