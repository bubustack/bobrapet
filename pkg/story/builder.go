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

package story

import (
	"encoding/json"
	"fmt"

	bubuv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/pkg/enums"
	"github.com/bubustack/bobrapet/pkg/refs"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// Builder provides a fluent API for constructing bobrapet Story resources that
// matches the behavior already used by controllers/webhooks and SDK helpers.
type Builder struct {
	story     bubuv1alpha1.Story
	stepNames map[string]struct{}
}

// New returns a Builder initialized with name/namespace metadata and the default
// batch execution pattern.
func New(name, namespace string) *Builder {
	return &Builder{
		story: bubuv1alpha1.Story{
			TypeMeta: metav1.TypeMeta{
				APIVersion: bubuv1alpha1.GroupVersion.String(),
				Kind:       "Story",
			},
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: namespace,
			},
			Spec: bubuv1alpha1.StorySpec{
				Pattern: enums.BatchPattern,
				Steps:   []bubuv1alpha1.Step{},
			},
		},
		stepNames: make(map[string]struct{}),
	}
}

// WithLabels sets object labels, overwriting any existing map when labels is nil.
func (b *Builder) WithLabels(labels map[string]string) *Builder {
	if labels == nil {
		b.story.Labels = nil
		return b
	}
	if b.story.Labels == nil {
		b.story.Labels = make(map[string]string, len(labels))
	}
	for k, v := range labels {
		b.story.Labels[k] = v
	}
	return b
}

// Pattern overrides the story execution pattern.
func (b *Builder) Pattern(pattern enums.StoryPattern) *Builder {
	b.story.Spec.Pattern = pattern
	return b
}

// Policy sets the story policy (deep-copied).
func (b *Builder) Policy(policy *bubuv1alpha1.StoryPolicy) *Builder {
	if policy == nil {
		b.story.Spec.Policy = nil
		return b
	}
	copy := *policy
	b.story.Spec.Policy = &copy
	return b
}

// AddStep appends a custom step to the story after validating names and needs.
func (b *Builder) AddStep(step bubuv1alpha1.Step) error {
	if step.Name == "" {
		return fmt.Errorf("step name is required")
	}
	if _, exists := b.stepNames[step.Name]; exists {
		return fmt.Errorf("duplicate step name %q", step.Name)
	}
	seenNeeds := make(map[string]struct{}, len(step.Needs))
	for _, need := range step.Needs {
		if _, dup := seenNeeds[need]; dup {
			return fmt.Errorf("step %q lists dependency %q more than once", step.Name, need)
		}
		seenNeeds[need] = struct{}{}
		if need == step.Name {
			return fmt.Errorf("step %q cannot depend on itself", step.Name)
		}
		if _, exists := b.stepNames[need]; !exists {
			return fmt.Errorf("dependency %q for step %q must be added before referencing", need, step.Name)
		}
	}
	if step.Type == enums.StepTypeLoop {
		if loopErrs := ValidateLoopStep(&step); len(loopErrs) > 0 {
			return fmt.Errorf("%s", loopErrs[0].Message)
		}
	}
	b.story.Spec.Steps = append(b.story.Spec.Steps, step)
	b.stepNames[step.Name] = struct{}{}
	return nil
}

// AddEngramStep adds a step referencing an Engram with optional needs and config.
func (b *Builder) AddEngramStep(
	name string,
	ref refs.EngramReference,
	needs []string,
	with any,
	opts ...func(*bubuv1alpha1.Step),
) error {
	rawWith, err := toRawExtension(with)
	if err != nil {
		return fmt.Errorf("failed to marshal step %q with block: %w", name, err)
	}
	refCopy := ref
	var needsCopy []string
	if len(needs) > 0 {
		needsCopy = append([]string(nil), needs...)
	}
	step := bubuv1alpha1.Step{
		Name:  name,
		Needs: needsCopy,
		Ref:   &refCopy,
		With:  rawWith,
	}
	for _, opt := range opts {
		opt(&step)
	}
	return b.AddStep(step)
}

// Build returns the constructed Story.
func (b *Builder) Build() (*bubuv1alpha1.Story, error) {
	if len(b.story.Spec.Steps) == 0 {
		return nil, fmt.Errorf("story must contain at least one step")
	}
	result := b.story.DeepCopy()
	return result, nil
}

func toRawExtension(input any) (*runtime.RawExtension, error) {
	if input == nil {
		return nil, nil
	}
	switch v := input.(type) {
	case *runtime.RawExtension:
		if v == nil {
			return nil, nil
		}
		return v.DeepCopy(), nil
	case runtime.RawExtension:
		return v.DeepCopy(), nil
	default:
		payload, err := json.Marshal(input)
		if err != nil {
			return nil, err
		}
		return &runtime.RawExtension{Raw: payload}, nil
	}
}
