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
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	bubuv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/internal/config"
	"github.com/bubustack/bobrapet/pkg/enums"
	"github.com/bubustack/bobrapet/pkg/refs"
	runsidentity "github.com/bubustack/bobrapet/pkg/runs/identity"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

var _ = Describe("StoryTrigger Webhook", func() {
	const (
		storyNamespace = "default"
		storyName      = "triggered-story"
		submissionID   = "submission-001"
		triggerKey     = "customer-42"
	)

	var (
		validator StoryTriggerCustomValidator
	)

	triggerHash := func(raw []byte) string {
		hash, err := runsidentity.ComputeTriggerInputHash(raw)
		Expect(err).NotTo(HaveOccurred())
		return hash
	}

	triggerFor := func(storyRef refs.StoryReference) *runsv1alpha1.StoryTrigger {
		inputs := []byte(`{"payload":"ok"}`)
		mode := bubuv1alpha1.TriggerDedupeKey
		targetNamespace := storyNamespace
		if storyRef.Namespace != nil && *storyRef.Namespace != "" {
			targetNamespace = *storyRef.Namespace
		}
		return &runsv1alpha1.StoryTrigger{
			ObjectMeta: metav1.ObjectMeta{
				Name:      runsidentity.DeriveStoryTriggerName(targetNamespace, storyRef.Name, triggerKey, submissionID),
				Namespace: storyNamespace,
			},
			Spec: runsv1alpha1.StoryTriggerSpec{
				StoryRef: storyRef,
				Inputs: &runtime.RawExtension{
					Raw: inputs,
				},
				DeliveryIdentity: runsv1alpha1.TriggerDeliveryIdentity{
					Mode:         &mode,
					Key:          triggerKey,
					InputHash:    triggerHash(inputs),
					SubmissionID: submissionID,
				},
			},
		}
	}

	ensureStory := func() {
		story := &bubuv1alpha1.Story{
			ObjectMeta: metav1.ObjectMeta{
				Name:      storyName,
				Namespace: storyNamespace,
			},
			Spec: bubuv1alpha1.StorySpec{
				Version: "v1",
				Steps: []bubuv1alpha1.Step{{
					Name: "prepare",
					Type: enums.StepTypeCondition,
				}},
			},
		}
		Expect(k8sClient.Create(ctx, story)).To(Succeed())
		DeferCleanup(func() {
			_ = k8sClient.Delete(ctx, story)
		})
	}

	BeforeEach(func() {
		validator = StoryTriggerCustomValidator{
			Client: k8sClient,
			Config: config.DefaultControllerConfig(),
		}
	})

	It("admits creation with a deterministic name", func() {
		ensureStory()

		obj := triggerFor(refs.StoryReference{
			ObjectReference: refs.ObjectReference{Name: storyName},
			Version:         "v1",
		})

		_, err := validator.ValidateCreate(context.Background(), obj)
		Expect(err).NotTo(HaveOccurred())
	})

	It("rejects a non-deterministic metadata.name", func() {
		ensureStory()

		obj := triggerFor(refs.StoryReference{
			ObjectReference: refs.ObjectReference{Name: storyName},
			Version:         "v1",
		})
		obj.Name = "wrong-name"

		_, err := validator.ValidateCreate(context.Background(), obj)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("metadata.name must be"))
	})

	It("rejects a missing input hash when key dedupe is used", func() {
		ensureStory()

		obj := triggerFor(refs.StoryReference{
			ObjectReference: refs.ObjectReference{Name: storyName},
			Version:         "v1",
		})
		obj.Spec.DeliveryIdentity.InputHash = ""

		_, err := validator.ValidateCreate(context.Background(), obj)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("spec.deliveryIdentity.inputHash"))
	})

	It("rejects a mismatched input hash", func() {
		ensureStory()

		obj := triggerFor(refs.StoryReference{
			ObjectReference: refs.ObjectReference{Name: storyName},
			Version:         "v1",
		})
		obj.Spec.DeliveryIdentity.InputHash = strings.Repeat("a", 64)

		_, err := validator.ValidateCreate(context.Background(), obj)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("does not match spec.inputs"))
	})

	It("rejects spec changes after creation", func() {
		ensureStory()

		oldObj := triggerFor(refs.StoryReference{
			ObjectReference: refs.ObjectReference{Name: storyName},
			Version:         "v1",
		})
		newObj := triggerFor(refs.StoryReference{
			ObjectReference: refs.ObjectReference{Name: storyName},
			Version:         "v1",
		})
		newObj.Spec.ImpulseRef = &refs.ImpulseReference{
			ObjectReference: refs.ObjectReference{Name: "impulse-a"},
		}

		_, err := validator.ValidateUpdate(context.Background(), oldObj, newObj)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("spec is immutable"))
	})

	It("rejects cross-namespace story references", func() {
		ensureStory()

		otherNamespace := "other"
		obj := triggerFor(refs.StoryReference{
			ObjectReference: refs.ObjectReference{
				Name:      storyName,
				Namespace: &otherNamespace,
			},
			Version: "v1",
		})
		obj.Name = runsidentity.DeriveStoryTriggerName(otherNamespace, storyName, triggerKey, submissionID)

		_, err := validator.ValidateCreate(context.Background(), obj)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("cross-namespace StoryRef"))
	})
})
