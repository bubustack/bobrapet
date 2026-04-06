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

package v1alpha1

import (
	"context"
	"encoding/json"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	catalogv1alpha1 "github.com/bubustack/bobrapet/api/catalog/v1alpha1"
	policyv1alpha1 "github.com/bubustack/bobrapet/api/policy/v1alpha1"
	bubushv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/internal/config"
	"github.com/bubustack/bobrapet/pkg/enums"
	"github.com/bubustack/bobrapet/pkg/refs"
)

func TestStoryValidatorValidateEngramStepCachedPrefersAPIReader(t *testing.T) {
	t.Parallel()

	testScheme := buildWebhookTestScheme(t)
	template := &catalogv1alpha1.EngramTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: "tpl-api-reader"},
	}
	engram := &bubushv1alpha1.Engram{
		ObjectMeta: metav1.ObjectMeta{Name: "eng-api-reader", Namespace: "default"},
		Spec: bubushv1alpha1.EngramSpec{
			TemplateRef: refs.EngramTemplateReference{Name: template.Name},
		},
	}

	validator := StoryCustomValidator{
		Client:    fake.NewClientBuilder().WithScheme(testScheme).Build(),
		APIReader: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(template, engram).Build(),
	}

	story := &bubushv1alpha1.Story{
		ObjectMeta: metav1.ObjectMeta{Name: "parent-story", Namespace: "default"},
	}
	step := &bubushv1alpha1.Step{
		Name: "run-engram",
		Ref: &refs.EngramReference{
			ObjectReference: refs.ObjectReference{Name: engram.Name},
		},
	}

	warnings, err := validator.validateEngramStepCached(context.Background(), story, step, engramCache{}, templateCache{})
	if err != nil {
		t.Fatalf("validateEngramStepCached returned error: %v", err)
	}
	if len(warnings) != 0 {
		t.Fatalf("expected no warnings when APIReader has objects, got: %v", warnings)
	}
}

func TestStoryValidatorValidateExecuteStoryReferencesPrefersAPIReader(t *testing.T) {
	t.Parallel()

	testScheme := buildWebhookTestScheme(t)
	child := &bubushv1alpha1.Story{
		ObjectMeta: metav1.ObjectMeta{Name: "child-story", Namespace: "default"},
	}

	validator := StoryCustomValidator{
		Client:    fake.NewClientBuilder().WithScheme(testScheme).Build(),
		APIReader: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(child).Build(),
	}

	parent := &bubushv1alpha1.Story{
		ObjectMeta: metav1.ObjectMeta{Name: "parent-story", Namespace: "default"},
	}
	steps := []bubushv1alpha1.Step{
		{
			Name: "invoke-child",
			Type: enums.StepTypeExecuteStory,
			With: mustRawJSONExtension(t, map[string]any{
				"storyRef": map[string]any{"name": child.Name},
			}),
		},
	}

	warnings, err := validator.validateExecuteStoryReferences(context.Background(), parent, steps)
	if err != nil {
		t.Fatalf("validateExecuteStoryReferences returned error: %v", err)
	}
	if len(warnings) != 0 {
		t.Fatalf("expected no warnings when APIReader has referenced Story, got: %v", warnings)
	}
}

func TestStoryValidatorValidateExecuteStoryReferencesFallsBackToClient(t *testing.T) {
	t.Parallel()

	testScheme := buildWebhookTestScheme(t)
	child := &bubushv1alpha1.Story{
		ObjectMeta: metav1.ObjectMeta{Name: "child-story", Namespace: "default"},
	}

	validator := StoryCustomValidator{
		Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(child).Build(),
	}

	parent := &bubushv1alpha1.Story{
		ObjectMeta: metav1.ObjectMeta{Name: "parent-story", Namespace: "default"},
	}
	steps := []bubushv1alpha1.Step{
		{
			Name: "invoke-child",
			Type: enums.StepTypeExecuteStory,
			With: mustRawJSONExtension(t, map[string]any{
				"storyRef": map[string]any{"name": child.Name},
			}),
		},
	}

	warnings, err := validator.validateExecuteStoryReferences(context.Background(), parent, steps)
	if err != nil {
		t.Fatalf("validateExecuteStoryReferences returned error: %v", err)
	}
	if len(warnings) != 0 {
		t.Fatalf("expected no warnings when falling back to Client, got: %v", warnings)
	}
}

func TestStoryValidatorValidateExecuteStoryReferencesUsesAPIReaderForCrossNamespaceGrant(t *testing.T) {
	t.Parallel()

	testScheme := buildWebhookTestScheme(t)
	targetNamespace := "other"
	childName := "child-story"

	child := &bubushv1alpha1.Story{
		ObjectMeta: metav1.ObjectMeta{Name: childName, Namespace: targetNamespace},
	}
	grant := &policyv1alpha1.ReferenceGrant{
		ObjectMeta: metav1.ObjectMeta{Name: "allow-story-to-story", Namespace: targetNamespace},
		Spec: policyv1alpha1.ReferenceGrantSpec{
			From: []policyv1alpha1.ReferenceGrantFrom{
				{
					Group:     "bubustack.io",
					Kind:      "Story",
					Namespace: "default",
				},
			},
			To: []policyv1alpha1.ReferenceGrantTo{
				{
					Group: "bubustack.io",
					Kind:  "Story",
					Name:  testStringPtr(childName),
				},
			},
		},
	}

	cfg := config.DefaultControllerConfig()
	cfg.ReferenceCrossNamespacePolicy = config.ReferenceCrossNamespacePolicyGrant

	validator := StoryCustomValidator{
		Client:        fake.NewClientBuilder().WithScheme(testScheme).Build(),
		APIReader:     fake.NewClientBuilder().WithScheme(testScheme).WithObjects(child, grant).Build(),
		Config:        cfg,
		ConfigManager: nil,
	}

	parent := &bubushv1alpha1.Story{
		ObjectMeta: metav1.ObjectMeta{Name: "parent-story", Namespace: "default"},
	}
	steps := []bubushv1alpha1.Step{
		{
			Name: "invoke-child",
			Type: enums.StepTypeExecuteStory,
			With: mustRawJSONExtension(t, map[string]any{
				"storyRef": map[string]any{
					"name":      childName,
					"namespace": targetNamespace,
				},
			}),
		},
	}

	warnings, err := validator.validateExecuteStoryReferences(context.Background(), parent, steps)
	if err != nil {
		t.Fatalf("validateExecuteStoryReferences returned error: %v", err)
	}
	if len(warnings) != 0 {
		t.Fatalf("expected no warnings, got: %v", warnings)
	}
}

func TestImpulseValidatorFetchTemplatePrefersAPIReader(t *testing.T) {
	t.Parallel()

	testScheme := buildWebhookTestScheme(t)
	template := &catalogv1alpha1.ImpulseTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: "tpl-api-reader"},
	}

	validator := ImpulseCustomValidator{
		Client:    fake.NewClientBuilder().WithScheme(testScheme).Build(),
		APIReader: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(template).Build(),
	}

	got, err := validator.fetchTemplate(context.Background(), template.Name)
	if err != nil {
		t.Fatalf("fetchTemplate returned error: %v", err)
	}
	if got.Name != template.Name {
		t.Fatalf("expected template %q, got %q", template.Name, got.Name)
	}
}

func TestImpulseValidatorFetchTemplateFallsBackToClient(t *testing.T) {
	t.Parallel()

	testScheme := buildWebhookTestScheme(t)
	template := &catalogv1alpha1.ImpulseTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: "tpl-client-fallback"},
	}

	validator := ImpulseCustomValidator{
		Client: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(template).Build(),
	}

	got, err := validator.fetchTemplate(context.Background(), template.Name)
	if err != nil {
		t.Fatalf("fetchTemplate returned error: %v", err)
	}
	if got.Name != template.Name {
		t.Fatalf("expected template %q, got %q", template.Name, got.Name)
	}
}

func TestImpulseValidatorValidateImpulseUsesAPIReaderForCrossNamespaceGrant(t *testing.T) {
	t.Parallel()

	testScheme := buildWebhookTestScheme(t)
	targetNamespace := "other"
	storyName := "target-story"

	template := &catalogv1alpha1.ImpulseTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: "tpl-granted"},
		Spec: catalogv1alpha1.ImpulseTemplateSpec{
			TemplateSpec: catalogv1alpha1.TemplateSpec{
				SupportedModes: []enums.WorkloadMode{enums.WorkloadModeDeployment},
			},
		},
	}
	story := &bubushv1alpha1.Story{
		ObjectMeta: metav1.ObjectMeta{Name: storyName, Namespace: targetNamespace},
	}
	grant := &policyv1alpha1.ReferenceGrant{
		ObjectMeta: metav1.ObjectMeta{Name: "allow-impulse-to-story", Namespace: targetNamespace},
		Spec: policyv1alpha1.ReferenceGrantSpec{
			From: []policyv1alpha1.ReferenceGrantFrom{
				{
					Group:     "bubustack.io",
					Kind:      "Impulse",
					Namespace: "default",
				},
			},
			To: []policyv1alpha1.ReferenceGrantTo{
				{
					Group: "bubustack.io",
					Kind:  "Story",
					Name:  testStringPtr(storyName),
				},
			},
		},
	}

	cfg := config.DefaultControllerConfig()
	cfg.ReferenceCrossNamespacePolicy = config.ReferenceCrossNamespacePolicyGrant

	validator := ImpulseCustomValidator{
		Client:    fake.NewClientBuilder().WithScheme(testScheme).Build(),
		APIReader: fake.NewClientBuilder().WithScheme(testScheme).WithObjects(template, story, grant).Build(),
		Config:    cfg,
	}

	impulse := &bubushv1alpha1.Impulse{
		ObjectMeta: metav1.ObjectMeta{Name: "impulse", Namespace: "default"},
		Spec: bubushv1alpha1.ImpulseSpec{
			TemplateRef: refs.ImpulseTemplateReference{Name: template.Name},
			StoryRef: refs.StoryReference{
				ObjectReference: refs.ObjectReference{
					Name:      storyName,
					Namespace: testStringPtr(targetNamespace),
				},
			},
		},
	}

	warnings, err := validator.validateImpulse(context.Background(), impulse)
	if err != nil {
		t.Fatalf("validateImpulse returned error: %v", err)
	}
	if len(warnings) != 0 {
		t.Fatalf("expected no warnings, got: %v", warnings)
	}
}

func buildWebhookTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()

	testScheme := runtime.NewScheme()
	if err := scheme.AddToScheme(testScheme); err != nil {
		t.Fatalf("add core scheme: %v", err)
	}
	if err := bubushv1alpha1.AddToScheme(testScheme); err != nil {
		t.Fatalf("add bobrapet v1alpha1 scheme: %v", err)
	}
	if err := catalogv1alpha1.AddToScheme(testScheme); err != nil {
		t.Fatalf("add catalog scheme: %v", err)
	}
	if err := policyv1alpha1.AddToScheme(testScheme); err != nil {
		t.Fatalf("add policy scheme: %v", err)
	}
	return testScheme
}

func mustRawJSONExtension(t *testing.T, value any) *runtime.RawExtension {
	t.Helper()

	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal raw json: %v", err)
	}
	return &runtime.RawExtension{Raw: raw}
}

func testStringPtr(v string) *string {
	return &v
}
