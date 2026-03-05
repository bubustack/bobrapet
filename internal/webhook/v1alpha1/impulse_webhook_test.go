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
	"encoding/json"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	catalogv1alpha1 "github.com/bubustack/bobrapet/api/catalog/v1alpha1"
	bubushv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/pkg/enums"
	"github.com/bubustack/bobrapet/pkg/refs"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

var _ = Describe("Impulse Webhook", func() {
	var (
		obj       *bubushv1alpha1.Impulse
		oldObj    *bubushv1alpha1.Impulse
		validator ImpulseCustomValidator
		defaulter ImpulseCustomDefaulter
	)

	BeforeEach(func() {
		obj = &bubushv1alpha1.Impulse{}
		oldObj = &bubushv1alpha1.Impulse{}
		validator = ImpulseCustomValidator{Client: k8sClient}
		Expect(validator).NotTo(BeNil(), "Expected validator to be initialized")
		defaulter = ImpulseCustomDefaulter{}
		Expect(defaulter).NotTo(BeNil(), "Expected defaulter to be initialized")
		Expect(oldObj).NotTo(BeNil(), "Expected oldObj to be initialized")
		Expect(obj).NotTo(BeNil(), "Expected obj to be initialized")
	})

	AfterEach(func() {
		// TODO (user): Add any teardown logic common to all tests
	})

	Context("When creating Impulse under Defaulting Webhook", func() {
		It("Default is a no-op that returns nil for any Impulse", func() {
			By("calling Default on a minimal Impulse")
			err := defaulter.Default(ctx, &bubushv1alpha1.Impulse{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "impulse-default",
					Namespace: "default",
				},
			})
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Context("When creating or updating Impulse under Validating Webhook", func() {
		It("rejects template version mismatches", func() {
			template := &catalogv1alpha1.ImpulseTemplate{
				ObjectMeta: metav1.ObjectMeta{
					Name: "tpl-version",
				},
				Spec: catalogv1alpha1.ImpulseTemplateSpec{
					TemplateSpec: catalogv1alpha1.TemplateSpec{
						Version:        "v1",
						SupportedModes: []enums.WorkloadMode{enums.WorkloadModeDeployment},
					},
				},
			}
			Expect(k8sClient.Create(ctx, template)).To(Succeed())
			DeferCleanup(func() {
				_ = k8sClient.Delete(ctx, template)
			})

			story := &bubushv1alpha1.Story{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "story-version",
					Namespace: "default",
				},
				Spec: bubushv1alpha1.StorySpec{
					Version: "v1",
					Steps: []bubushv1alpha1.Step{{
						Name: "prepare",
						Type: enums.StepTypeCondition,
					}},
				},
			}
			Expect(k8sClient.Create(ctx, story)).To(Succeed())
			DeferCleanup(func() {
				_ = k8sClient.Delete(ctx, story)
			})

			obj = &bubushv1alpha1.Impulse{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "impulse-version",
					Namespace: "default",
				},
				Spec: bubushv1alpha1.ImpulseSpec{
					TemplateRef: refs.ImpulseTemplateReference{
						Name:    "tpl-version",
						Version: "v2",
					},
					StoryRef: refs.StoryReference{
						ObjectReference: refs.ObjectReference{
							Name: "story-version",
						},
					},
				},
			}

			_, err := validator.ValidateCreate(ctx, obj)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("does not match template's actual version"))
		})

		It("rejects cross-namespace story references", func() {
			other := "other"
			namespace := &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{
					Name: other,
				},
			}
			Expect(k8sClient.Create(ctx, namespace)).To(Succeed())
			DeferCleanup(func() {
				_ = k8sClient.Delete(ctx, namespace)
			})
			template := &catalogv1alpha1.ImpulseTemplate{
				ObjectMeta: metav1.ObjectMeta{
					Name: "tpl-cross",
				},
				Spec: catalogv1alpha1.ImpulseTemplateSpec{
					TemplateSpec: catalogv1alpha1.TemplateSpec{
						Version:        "v1",
						SupportedModes: []enums.WorkloadMode{enums.WorkloadModeDeployment},
					},
				},
			}
			Expect(k8sClient.Create(ctx, template)).To(Succeed())
			DeferCleanup(func() {
				_ = k8sClient.Delete(ctx, template)
			})

			story := &bubushv1alpha1.Story{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "target-story",
					Namespace: other,
				},
				Spec: bubushv1alpha1.StorySpec{
					Steps: []bubushv1alpha1.Step{{
						Name: "prepare",
						Type: enums.StepTypeCondition,
					}},
				},
			}
			Expect(k8sClient.Create(ctx, story)).To(Succeed())
			DeferCleanup(func() {
				_ = k8sClient.Delete(ctx, story)
			})

			obj = &bubushv1alpha1.Impulse{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "impulse-cross",
					Namespace: "default",
				},
				Spec: bubushv1alpha1.ImpulseSpec{
					TemplateRef: refs.ImpulseTemplateReference{
						Name: "tpl-cross",
					},
					StoryRef: refs.StoryReference{
						ObjectReference: refs.ObjectReference{
							Name:      "target-story",
							Namespace: &other,
						},
					},
				},
			}

			_, err := validator.ValidateCreate(ctx, obj)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("cross-namespace StoryRef"))
		})

		It("rejects workload modes not supported by template", func() {
			template := &catalogv1alpha1.ImpulseTemplate{
				ObjectMeta: metav1.ObjectMeta{
					Name: "tpl-modes",
				},
				Spec: catalogv1alpha1.ImpulseTemplateSpec{
					TemplateSpec: catalogv1alpha1.TemplateSpec{
						Version:        "v1",
						SupportedModes: []enums.WorkloadMode{enums.WorkloadModeDeployment},
					},
				},
			}
			Expect(k8sClient.Create(ctx, template)).To(Succeed())
			DeferCleanup(func() {
				_ = k8sClient.Delete(ctx, template)
			})

			story := &bubushv1alpha1.Story{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "story-modes",
					Namespace: "default",
				},
				Spec: bubushv1alpha1.StorySpec{
					Steps: []bubushv1alpha1.Step{{
						Name: "prepare",
						Type: enums.StepTypeCondition,
					}},
				},
			}
			Expect(k8sClient.Create(ctx, story)).To(Succeed())
			DeferCleanup(func() {
				_ = k8sClient.Delete(ctx, story)
			})

			obj = &bubushv1alpha1.Impulse{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "impulse-modes",
					Namespace: "default",
				},
				Spec: bubushv1alpha1.ImpulseSpec{
					TemplateRef: refs.ImpulseTemplateReference{
						Name: "tpl-modes",
					},
					StoryRef: refs.StoryReference{
						ObjectReference: refs.ObjectReference{
							Name: "story-modes",
						},
					},
					Workload: &bubushv1alpha1.WorkloadSpec{
						Mode: enums.WorkloadModeStatefulSet,
					},
				},
			}

			_, err := validator.ValidateCreate(ctx, obj)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("supportedModes"))
		})

		It("validates secrets against template secretSchema keys", func() {
			template := &catalogv1alpha1.ImpulseTemplate{
				ObjectMeta: metav1.ObjectMeta{
					Name: "tpl-secrets",
				},
				Spec: catalogv1alpha1.ImpulseTemplateSpec{
					TemplateSpec: catalogv1alpha1.TemplateSpec{
						Version:        "v1",
						SupportedModes: []enums.WorkloadMode{enums.WorkloadModeDeployment},
						SecretSchema: map[string]catalogv1alpha1.SecretDefinition{
							"apiKey": {Required: true},
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, template)).To(Succeed())
			DeferCleanup(func() {
				_ = k8sClient.Delete(ctx, template)
			})

			story := &bubushv1alpha1.Story{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "story-secrets",
					Namespace: "default",
				},
				Spec: bubushv1alpha1.StorySpec{
					Steps: []bubushv1alpha1.Step{{
						Name: "prepare",
						Type: enums.StepTypeCondition,
					}},
				},
			}
			Expect(k8sClient.Create(ctx, story)).To(Succeed())
			DeferCleanup(func() {
				_ = k8sClient.Delete(ctx, story)
			})

			obj = &bubushv1alpha1.Impulse{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "impulse-secrets",
					Namespace: "default",
				},
				Spec: bubushv1alpha1.ImpulseSpec{
					TemplateRef: refs.ImpulseTemplateReference{
						Name: "tpl-secrets",
					},
					StoryRef: refs.StoryReference{
						ObjectReference: refs.ObjectReference{
							Name: "story-secrets",
						},
					},
					Secrets: map[string]string{
						"unknown": "some-secret",
					},
				},
			}

			_, err := validator.ValidateCreate(ctx, obj)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("secret \"apiKey\" is required"))
			Expect(err.Error()).To(ContainSubstring("secret \"unknown\" is not defined"))
		})

		It("allows retry delays of zero", func() {
			template := &catalogv1alpha1.ImpulseTemplate{
				ObjectMeta: metav1.ObjectMeta{
					Name: "tpl-retry",
				},
				Spec: catalogv1alpha1.ImpulseTemplateSpec{
					TemplateSpec: catalogv1alpha1.TemplateSpec{
						Version:        "v1",
						SupportedModes: []enums.WorkloadMode{enums.WorkloadModeDeployment},
					},
				},
			}
			Expect(k8sClient.Create(ctx, template)).To(Succeed())
			DeferCleanup(func() {
				_ = k8sClient.Delete(ctx, template)
			})

			story := &bubushv1alpha1.Story{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "story-retry",
					Namespace: "default",
				},
				Spec: bubushv1alpha1.StorySpec{
					Steps: []bubushv1alpha1.Step{{
						Name: "prepare",
						Type: enums.StepTypeCondition,
					}},
				},
			}
			Expect(k8sClient.Create(ctx, story)).To(Succeed())
			DeferCleanup(func() {
				_ = k8sClient.Delete(ctx, story)
			})

			zero := "0s"
			obj = &bubushv1alpha1.Impulse{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "impulse-retry",
					Namespace: "default",
				},
				Spec: bubushv1alpha1.ImpulseSpec{
					TemplateRef: refs.ImpulseTemplateReference{
						Name: "tpl-retry",
					},
					StoryRef: refs.StoryReference{
						ObjectReference: refs.ObjectReference{
							Name: "story-retry",
						},
					},
					DeliveryPolicy: &bubushv1alpha1.TriggerDeliveryPolicy{
						Retry: &bubushv1alpha1.TriggerRetryPolicy{
							BaseDelay: &zero,
							MaxDelay:  &zero,
						},
					},
				},
			}

			_, err := validator.ValidateCreate(ctx, obj)
			Expect(err).NotTo(HaveOccurred())
		})

		It("does not validate mapping against template context schema", func() {
			schema := map[string]any{
				"type":                 "object",
				"additionalProperties": false,
				"properties": map[string]any{
					"allowed": map[string]any{"type": "string"},
				},
			}
			schemaBytes, err := json.Marshal(schema)
			Expect(err).NotTo(HaveOccurred())

			template := &catalogv1alpha1.ImpulseTemplate{
				ObjectMeta: metav1.ObjectMeta{
					Name: "tpl-mapping",
				},
				Spec: catalogv1alpha1.ImpulseTemplateSpec{
					TemplateSpec: catalogv1alpha1.TemplateSpec{
						Version:        "v1",
						SupportedModes: []enums.WorkloadMode{enums.WorkloadModeDeployment},
					},
					ContextSchema: &runtime.RawExtension{Raw: schemaBytes},
				},
			}
			Expect(k8sClient.Create(ctx, template)).To(Succeed())
			DeferCleanup(func() {
				_ = k8sClient.Delete(ctx, template)
			})

			story := &bubushv1alpha1.Story{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "story-mapping",
					Namespace: "default",
				},
				Spec: bubushv1alpha1.StorySpec{
					Steps: []bubushv1alpha1.Step{{
						Name: "prepare",
						Type: enums.StepTypeCondition,
					}},
				},
			}
			Expect(k8sClient.Create(ctx, story)).To(Succeed())
			DeferCleanup(func() {
				_ = k8sClient.Delete(ctx, story)
			})

			mapping := map[string]any{"customInput": 42}
			mappingBytes, err := json.Marshal(mapping)
			Expect(err).NotTo(HaveOccurred())

			obj = &bubushv1alpha1.Impulse{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "impulse-mapping",
					Namespace: "default",
				},
				Spec: bubushv1alpha1.ImpulseSpec{
					TemplateRef: refs.ImpulseTemplateReference{
						Name: "tpl-mapping",
					},
					StoryRef: refs.StoryReference{
						ObjectReference: refs.ObjectReference{
							Name: "story-mapping",
						},
					},
					Mapping: &runtime.RawExtension{Raw: mappingBytes},
				},
			}

			_, err = validator.ValidateCreate(ctx, obj)
			Expect(err).NotTo(HaveOccurred())
		})

		// ── ValidateUpdate ────────────────────────────────────────────────────────

		It("ValidateUpdate skips full validation when spec is unchanged (metadata-only update)", func() {
			By("using an intentionally invalid spec on both sides to prove early-exit fires")
			// Both have templateRef.name == "" which would fail validation if it ran.
			// Because the specs are identical, the validator must short-circuit before
			// touching the API server.
			oldObj = &bubushv1alpha1.Impulse{
				ObjectMeta: metav1.ObjectMeta{Name: "impulse-noop", Namespace: "default"},
				Spec:       bubushv1alpha1.ImpulseSpec{},
			}
			obj = &bubushv1alpha1.Impulse{
				ObjectMeta: metav1.ObjectMeta{Name: "impulse-noop", Namespace: "default"},
				Spec:       bubushv1alpha1.ImpulseSpec{},
			}
			_, err := validator.ValidateUpdate(ctx, oldObj, obj)
			Expect(err).NotTo(HaveOccurred(), "identical spec must skip validation entirely")
		})

		It("ValidateUpdate skips full validation when DeletionTimestamp is set (finalizer removal)", func() {
			By("marking the new object as terminating")
			now := metav1.Now()
			obj = &bubushv1alpha1.Impulse{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "impulse-deleting",
					Namespace:         "default",
					DeletionTimestamp: &now,
				},
				Spec: bubushv1alpha1.ImpulseSpec{}, // missing templateRef – would fail if run
			}
			_, err := validator.ValidateUpdate(ctx, oldObj, obj)
			Expect(err).NotTo(HaveOccurred(), "objects under deletion must bypass spec validation")
		})

		It("ValidateUpdate runs validation and catches errors when spec changes", func() {
			template := &catalogv1alpha1.ImpulseTemplate{
				ObjectMeta: metav1.ObjectMeta{Name: "tpl-update"},
				Spec: catalogv1alpha1.ImpulseTemplateSpec{
					TemplateSpec: catalogv1alpha1.TemplateSpec{
						Version:        "v1",
						SupportedModes: []enums.WorkloadMode{enums.WorkloadModeDeployment},
					},
				},
			}
			Expect(k8sClient.Create(ctx, template)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, template) })

			story := &bubushv1alpha1.Story{
				ObjectMeta: metav1.ObjectMeta{Name: "story-update", Namespace: "default"},
				Spec: bubushv1alpha1.StorySpec{
					Steps: []bubushv1alpha1.Step{{Name: "prepare", Type: enums.StepTypeCondition}},
				},
			}
			Expect(k8sClient.Create(ctx, story)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, story) })

			validSpec := bubushv1alpha1.ImpulseSpec{
				TemplateRef: refs.ImpulseTemplateReference{Name: "tpl-update"},
				StoryRef:    refs.StoryReference{ObjectReference: refs.ObjectReference{Name: "story-update"}},
			}
			oldObj = &bubushv1alpha1.Impulse{
				ObjectMeta: metav1.ObjectMeta{Name: "impulse-update", Namespace: "default"},
				Spec:       validSpec,
			}
			// Change spec: set forbidden job mode.
			obj = &bubushv1alpha1.Impulse{
				ObjectMeta: metav1.ObjectMeta{Name: "impulse-update", Namespace: "default"},
				Spec: bubushv1alpha1.ImpulseSpec{
					TemplateRef: refs.ImpulseTemplateReference{Name: "tpl-update"},
					StoryRef:    refs.StoryReference{ObjectReference: refs.ObjectReference{Name: "story-update"}},
					Workload:    &bubushv1alpha1.WorkloadSpec{Mode: enums.WorkloadModeJob},
				},
			}

			_, err := validator.ValidateUpdate(ctx, oldObj, obj)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("must not be 'job'"))
		})

		// ── Admission warnings ────────────────────────────────────────────────────

		It("warns when referenced Story does not exist yet (soft admission)", func() {
			template := &catalogv1alpha1.ImpulseTemplate{
				ObjectMeta: metav1.ObjectMeta{Name: "tpl-warn-missing"},
				Spec: catalogv1alpha1.ImpulseTemplateSpec{
					TemplateSpec: catalogv1alpha1.TemplateSpec{
						Version:        "v1",
						SupportedModes: []enums.WorkloadMode{enums.WorkloadModeDeployment},
					},
				},
			}
			Expect(k8sClient.Create(ctx, template)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, template) })

			obj = &bubushv1alpha1.Impulse{
				ObjectMeta: metav1.ObjectMeta{Name: "impulse-warn-missing", Namespace: "default"},
				Spec: bubushv1alpha1.ImpulseSpec{
					TemplateRef: refs.ImpulseTemplateReference{Name: "tpl-warn-missing"},
					StoryRef: refs.StoryReference{
						ObjectReference: refs.ObjectReference{Name: "story-does-not-exist"},
					},
				},
			}

			warnings, err := validator.ValidateCreate(ctx, obj)
			Expect(err).NotTo(HaveOccurred(), "missing Story must not block admission")
			Expect(warnings).To(ContainElement(ContainSubstring("not found yet")))
		})

		It("warns when referenced Story version does not match (soft admission)", func() {
			template := &catalogv1alpha1.ImpulseTemplate{
				ObjectMeta: metav1.ObjectMeta{Name: "tpl-warn-ver"},
				Spec: catalogv1alpha1.ImpulseTemplateSpec{
					TemplateSpec: catalogv1alpha1.TemplateSpec{
						Version:        "v1",
						SupportedModes: []enums.WorkloadMode{enums.WorkloadModeDeployment},
					},
				},
			}
			Expect(k8sClient.Create(ctx, template)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, template) })

			story := &bubushv1alpha1.Story{
				ObjectMeta: metav1.ObjectMeta{Name: "story-warn-ver", Namespace: "default"},
				Spec: bubushv1alpha1.StorySpec{
					Version: "v1",
					Steps:   []bubushv1alpha1.Step{{Name: "prepare", Type: enums.StepTypeCondition}},
				},
			}
			Expect(k8sClient.Create(ctx, story)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, story) })

			storyVersion := "v2"
			obj = &bubushv1alpha1.Impulse{
				ObjectMeta: metav1.ObjectMeta{Name: "impulse-warn-ver", Namespace: "default"},
				Spec: bubushv1alpha1.ImpulseSpec{
					TemplateRef: refs.ImpulseTemplateReference{Name: "tpl-warn-ver"},
					StoryRef: refs.StoryReference{
						ObjectReference: refs.ObjectReference{Name: "story-warn-ver"},
						Version:         storyVersion,
					},
				},
			}

			warnings, err := validator.ValidateCreate(ctx, obj)
			Expect(err).NotTo(HaveOccurred(), "Story version mismatch must not block admission")
			Expect(warnings).To(ContainElement(ContainSubstring("Story version mismatch")))
		})

		// ── Throttle policy ───────────────────────────────────────────────────────

		It("rejects negative maxInFlight in throttle policy", func() {
			neg := int32(-1)
			obj = &bubushv1alpha1.Impulse{
				ObjectMeta: metav1.ObjectMeta{Name: "impulse-throttle", Namespace: "default"},
				Spec: bubushv1alpha1.ImpulseSpec{
					Throttle: &bubushv1alpha1.TriggerThrottlePolicy{MaxInFlight: &neg},
				},
			}
			_, err := validator.ValidateCreate(ctx, obj)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("maxInFlight must be >= 0"))
		})

		It("rejects negative ratePerSecond in throttle policy", func() {
			neg := int32(-1)
			obj = &bubushv1alpha1.Impulse{
				ObjectMeta: metav1.ObjectMeta{Name: "impulse-throttle-rate", Namespace: "default"},
				Spec: bubushv1alpha1.ImpulseSpec{
					Throttle: &bubushv1alpha1.TriggerThrottlePolicy{RatePerSecond: &neg},
				},
			}
			_, err := validator.ValidateCreate(ctx, obj)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("ratePerSecond must be >= 0"))
		})

		// ── Delivery policy ───────────────────────────────────────────────────────

		It("rejects keyTemplate when dedupe mode is not 'key'", func() {
			tpl := "some-key"
			mode := bubushv1alpha1.TriggerDedupeToken
			obj = &bubushv1alpha1.Impulse{
				ObjectMeta: metav1.ObjectMeta{Name: "impulse-dedupe", Namespace: "default"},
				Spec: bubushv1alpha1.ImpulseSpec{
					DeliveryPolicy: &bubushv1alpha1.TriggerDeliveryPolicy{
						Dedupe: &bubushv1alpha1.TriggerDedupePolicy{
							Mode:        &mode,
							KeyTemplate: &tpl,
						},
					},
				},
			}
			_, err := validator.ValidateCreate(ctx, obj)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("keyTemplate is only valid when dedupe.mode is 'key'"))
		})

		It("rejects maxDelay less than baseDelay in retry policy", func() {
			base := "10s"
			max := "1s"
			obj = &bubushv1alpha1.Impulse{
				ObjectMeta: metav1.ObjectMeta{Name: "impulse-delays", Namespace: "default"},
				Spec: bubushv1alpha1.ImpulseSpec{
					DeliveryPolicy: &bubushv1alpha1.TriggerDeliveryPolicy{
						Retry: &bubushv1alpha1.TriggerRetryPolicy{
							BaseDelay: &base,
							MaxDelay:  &max,
						},
					},
				},
			}
			_, err := validator.ValidateCreate(ctx, obj)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("maxDelay must be greater than or equal to baseDelay"))
		})

		It("rejects an invalid duration string in baseDelay", func() {
			bad := "not-a-duration"
			obj = &bubushv1alpha1.Impulse{
				ObjectMeta: metav1.ObjectMeta{Name: "impulse-badduration", Namespace: "default"},
				Spec: bubushv1alpha1.ImpulseSpec{
					DeliveryPolicy: &bubushv1alpha1.TriggerDeliveryPolicy{
						Retry: &bubushv1alpha1.TriggerRetryPolicy{
							BaseDelay: &bad,
						},
					},
				},
			}
			_, err := validator.ValidateCreate(ctx, obj)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("invalid baseDelay"))
		})
	})
})
