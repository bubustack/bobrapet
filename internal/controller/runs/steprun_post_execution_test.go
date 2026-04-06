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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	"github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/internal/config"
	"github.com/bubustack/bobrapet/pkg/refs"
	"github.com/bubustack/core/templating"
)

func newTestEvaluator(t *testing.T) *templating.Evaluator {
	t.Helper()
	eval, err := templating.New(templating.Config{
		EvaluationTimeout: 5 * time.Second,
		MaxOutputBytes:    4096,
	})
	require.NoError(t, err)
	t.Cleanup(func() { eval.Close() })
	return eval
}

func TestEvaluatePostExecution(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	require.NoError(t, runsv1alpha1.AddToScheme(scheme))
	require.NoError(t, v1alpha1.AddToScheme(scheme))

	tests := []struct {
		name        string
		condition   string
		failMsg     string
		output      string // JSON output on the StepRun
		wantErr     bool
		errContains string
	}{
		{
			name:      "condition true — step succeeds",
			condition: `{{ eq (index .steps "my-step" "output" "ok") true }}`,
			output:    `{"ok": true}`,
			wantErr:   false,
		},
		{
			name:        "condition false — step fails with default message",
			condition:   `{{ eq (index .steps "my-step" "output" "ok") true }}`,
			output:      `{"ok": false}`,
			wantErr:     true,
			errContains: "post-execution condition evaluated to false",
		},
		{
			name:        "condition false — step fails with custom message",
			condition:   `{{ eq (index .steps "my-step" "output" "ok") true }}`,
			failMsg:     "notification was not delivered",
			output:      `{"ok": false}`,
			wantErr:     true,
			errContains: "notification was not delivered",
		},
		{
			name:    "no postExecution — step succeeds normally",
			output:  `{"ok": true}`,
			wantErr: false,
		},
		{
			name:      "no output — condition can still evaluate",
			condition: `{{ eq 1 1 }}`,
			wantErr:   false,
		},
		{
			name:        "invalid template — returns evaluation error",
			condition:   `{{ .nonexistent.deep.path }}`,
			output:      `{"ok": true}`,
			wantErr:     true,
			errContains: "post-execution condition evaluation error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			steps := []v1alpha1.Step{
				{
					Name: "my-step",
					Ref:  &refs.EngramReference{ObjectReference: refs.ObjectReference{Name: "some-engram"}},
				},
			}
			if tt.condition != "" {
				steps[0].PostExecution = &v1alpha1.PostExecutionCheck{
					Condition:      tt.condition,
					FailureMessage: tt.failMsg,
				}
			}

			story := &v1alpha1.Story{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-story",
					Namespace: "default",
				},
				Spec: v1alpha1.StorySpec{
					Steps: steps,
				},
			}

			storyRun := &runsv1alpha1.StoryRun{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-run",
					Namespace: "default",
				},
				Spec: runsv1alpha1.StoryRunSpec{
					StoryRef: refs.StoryReference{ObjectReference: refs.ObjectReference{Name: "test-story"}},
				},
			}

			stepRun := &runsv1alpha1.StepRun{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-step",
					Namespace: "default",
				},
				Spec: runsv1alpha1.StepRunSpec{
					StepID:      "my-step",
					StoryRunRef: refs.StoryRunReference{ObjectReference: refs.ObjectReference{Name: "test-run"}},
				},
			}
			if tt.output != "" {
				stepRun.Status.Output = &runtime.RawExtension{Raw: []byte(tt.output)}
			}

			client := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(story, storyRun, stepRun).
				Build()

			reconciler := &StepRunReconciler{
				ControllerDependencies: config.ControllerDependencies{
					Client:            client,
					Scheme:            scheme,
					TemplateEvaluator: newTestEvaluator(t),
				},
			}

			err := reconciler.evaluatePostExecution(context.Background(), stepRun)
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errContains)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestEvaluatePostExecution_StoryNotFound(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	require.NoError(t, runsv1alpha1.AddToScheme(scheme))
	require.NoError(t, v1alpha1.AddToScheme(scheme))

	// StoryRun exists but Story does not — should not block.
	storyRun := &runsv1alpha1.StoryRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "orphan-run",
			Namespace: "default",
		},
		Spec: runsv1alpha1.StoryRunSpec{
			StoryRef: refs.StoryReference{ObjectReference: refs.ObjectReference{Name: "missing-story"}},
		},
	}

	stepRun := &runsv1alpha1.StepRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "orphan-step",
			Namespace: "default",
		},
		Spec: runsv1alpha1.StepRunSpec{
			StepID:      "my-step",
			StoryRunRef: refs.StoryRunReference{ObjectReference: refs.ObjectReference{Name: "orphan-run"}},
		},
	}

	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(storyRun, stepRun).
		Build()

	reconciler := &StepRunReconciler{
		ControllerDependencies: config.ControllerDependencies{
			Client:            client,
			Scheme:            scheme,
			TemplateEvaluator: newTestEvaluator(t),
		},
	}

	err := reconciler.evaluatePostExecution(context.Background(), stepRun)
	require.NoError(t, err, "transient story lookup failure should not block step completion")
}
