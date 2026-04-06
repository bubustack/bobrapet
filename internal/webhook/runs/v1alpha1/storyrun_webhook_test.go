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
	"fmt"
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

var _ = Describe("StoryRun Webhook", func() {
	var (
		obj       *runsv1alpha1.StoryRun
		oldObj    *runsv1alpha1.StoryRun
		validator StoryRunCustomValidator
	)

	BeforeEach(func() {
		obj = &runsv1alpha1.StoryRun{}
		oldObj = &runsv1alpha1.StoryRun{}
		validator = StoryRunCustomValidator{Client: k8sClient, Config: config.DefaultControllerConfig()}
		Expect(validator).NotTo(BeNil(), "Expected validator to be initialized")
		Expect(oldObj).NotTo(BeNil(), "Expected oldObj to be initialized")
		Expect(obj).NotTo(BeNil(), "Expected obj to be initialized")
	})

	AfterEach(func() {
		// No shared teardown required; tests clean up objects individually.
	})

	Context("status invariants", func() {
		It("rejects observedGeneration regressions on status updates", func() {
			old := &runsv1alpha1.StoryRun{}
			old.Status.ObservedGeneration = 9
			updated := old.DeepCopy()
			updated.Status.ObservedGeneration = 8
			_, err := validator.ValidateUpdate(context.Background(), old, updated)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("status.observedGeneration"))
		})

		It("rejects observedGeneration reset to zero", func() {
			old := &runsv1alpha1.StoryRun{}
			old.Status.ObservedGeneration = 3
			updated := old.DeepCopy()
			updated.Status.ObservedGeneration = 0
			_, err := validator.ValidateUpdate(context.Background(), old, updated)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("status.observedGeneration"))
		})
	})

	Context("namespace scoping", func() {
		It("rejects cross-namespace storyRef", func() {
			other := "other" //nolint:goconst
			sr := &runsv1alpha1.StoryRun{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
					Name:      "run-1",
				},
				Spec: runsv1alpha1.StoryRunSpec{
					StoryRef: refs.StoryReference{
						ObjectReference: refs.ObjectReference{
							Name:      "story-a",
							Namespace: &other,
						},
					},
				},
			}

			_, err := validator.ValidateCreate(context.Background(), sr)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("cross-namespace StoryRef"))
		})
	})

	Context("version pinning", func() {
		It("rejects storyRef version mismatches", func() {
			story := &bubuv1alpha1.Story{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "versioned-story",
					Namespace: "default",
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

			sr := &runsv1alpha1.StoryRun{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
					Name:      "run-versioned",
				},
				Spec: runsv1alpha1.StoryRunSpec{
					StoryRef: refs.StoryReference{
						ObjectReference: refs.ObjectReference{
							Name: "versioned-story",
						},
						Version: "v2",
					},
				},
			}

			_, err := validator.ValidateCreate(context.Background(), sr)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("version"))
		})
	})

	Context("immutability", func() {
		It("rejects spec changes after creation", func() {
			original := &runsv1alpha1.StoryRun{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
					Name:      "immutable-run",
				},
				Spec: runsv1alpha1.StoryRunSpec{
					StoryRef: refs.StoryReference{
						ObjectReference: refs.ObjectReference{
							Name: "story-a",
						},
					},
				},
			}

			updated := original.DeepCopy()
			updated.Spec.Inputs = &runtime.RawExtension{Raw: []byte(`{"changed":true}`)}

			_, err := validator.ValidateUpdate(context.Background(), original, updated)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("spec is immutable"))
		})
	})

	Context("trigger tokens", func() {
		It("rejects adding a trigger token annotation after creation", func() {
			token := "late-token"
			inputHash, err := runsidentity.ComputeTriggerInputHashFromRawExtension(nil)
			Expect(err).NotTo(HaveOccurred())
			expectedName := runsidentity.DeriveStoryRunName("default", "late-token-story", token)

			original := &runsv1alpha1.StoryRun{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
					Name:      expectedName,
				},
				Spec: runsv1alpha1.StoryRunSpec{
					StoryRef: refs.StoryReference{
						ObjectReference: refs.ObjectReference{
							Name: "late-token-story",
						},
					},
				},
			}

			updated := original.DeepCopy()
			updated.Annotations = map[string]string{
				runsidentity.StoryRunTriggerTokenAnnotation:     token,
				runsidentity.StoryRunTriggerInputHashAnnotation: inputHash,
			}

			_, err = validator.ValidateUpdate(context.Background(), original, updated)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("immutable"))
		})

		It("rejects removing a trigger token annotation after creation", func() {
			story := &bubuv1alpha1.Story{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "token-immutability-story",
					Namespace: "default",
				},
				Spec: bubuv1alpha1.StorySpec{
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

			token := "immutability-token"
			inputHash, err := runsidentity.ComputeTriggerInputHashFromRawExtension(nil)
			Expect(err).NotTo(HaveOccurred())
			expectedName := runsidentity.DeriveStoryRunName("default", "token-immutability-story", token)

			original := &runsv1alpha1.StoryRun{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
					Name:      expectedName,
					Annotations: map[string]string{
						runsidentity.StoryRunTriggerTokenAnnotation:     token,
						runsidentity.StoryRunTriggerInputHashAnnotation: inputHash,
					},
				},
				Spec: runsv1alpha1.StoryRunSpec{
					StoryRef: refs.StoryReference{
						ObjectReference: refs.ObjectReference{
							Name: "token-immutability-story",
						},
					},
				},
			}

			updated := original.DeepCopy()
			delete(updated.Annotations, runsidentity.StoryRunTriggerTokenAnnotation)

			_, err = validator.ValidateUpdate(context.Background(), original, updated)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("immutable"))
		})

		It("rejects removing trigger input hash when trigger token is set", func() {
			story := &bubuv1alpha1.Story{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "token-hash-immutability-story",
					Namespace: "default",
				},
				Spec: bubuv1alpha1.StorySpec{
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

			token := "immutability-hash-token"
			inputHash, err := runsidentity.ComputeTriggerInputHashFromRawExtension(nil)
			Expect(err).NotTo(HaveOccurred())
			expectedName := runsidentity.DeriveStoryRunName("default", "token-hash-immutability-story", token)

			original := &runsv1alpha1.StoryRun{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
					Name:      expectedName,
					Annotations: map[string]string{
						runsidentity.StoryRunTriggerTokenAnnotation:     token,
						runsidentity.StoryRunTriggerInputHashAnnotation: inputHash,
					},
				},
				Spec: runsv1alpha1.StoryRunSpec{
					StoryRef: refs.StoryReference{
						ObjectReference: refs.ObjectReference{
							Name: "token-hash-immutability-story",
						},
					},
				},
			}

			updated := original.DeepCopy()
			delete(updated.Annotations, runsidentity.StoryRunTriggerInputHashAnnotation)

			_, err = validator.ValidateUpdate(context.Background(), original, updated)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("trigger-input-hash"))
		})

		It("rejects missing input hash when trigger token annotation is set", func() {
			story := &bubuv1alpha1.Story{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "token-story-missing-hash",
					Namespace: "default",
				},
				Spec: bubuv1alpha1.StorySpec{
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

			token := "token-missing-hash"
			expectedName := runsidentity.DeriveStoryRunName("default", "token-story-missing-hash", token)
			sr := &runsv1alpha1.StoryRun{
				ObjectMeta: metav1.ObjectMeta{
					Namespace:   "default",
					Name:        expectedName,
					Annotations: map[string]string{runsidentity.StoryRunTriggerTokenAnnotation: token},
				},
				Spec: runsv1alpha1.StoryRunSpec{
					StoryRef: refs.StoryReference{
						ObjectReference: refs.ObjectReference{
							Name: "token-story-missing-hash",
						},
					},
				},
			}

			_, err := validator.ValidateCreate(context.Background(), sr)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("trigger-input-hash"))
		})

		It("rejects mismatched name when trigger token annotation is set", func() {
			story := &bubuv1alpha1.Story{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "token-story",
					Namespace: "default",
				},
				Spec: bubuv1alpha1.StorySpec{
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

			inputHash, err := runsidentity.ComputeTriggerInputHashFromRawExtension(nil)
			Expect(err).NotTo(HaveOccurred())
			sr := &runsv1alpha1.StoryRun{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
					Name:      "wrong-name",
					Annotations: map[string]string{
						runsidentity.StoryRunTriggerTokenAnnotation:     "token-1",
						runsidentity.StoryRunTriggerInputHashAnnotation: inputHash,
					},
				},
				Spec: runsv1alpha1.StoryRunSpec{
					StoryRef: refs.StoryReference{
						ObjectReference: refs.ObjectReference{
							Name: "token-story",
						},
					},
				},
			}

			_, err = validator.ValidateCreate(context.Background(), sr)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("metadata.name must be"))
		})

		It("accepts deterministic name when trigger token annotation is set", func() {
			story := &bubuv1alpha1.Story{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "token-story-2",
					Namespace: "default",
				},
				Spec: bubuv1alpha1.StorySpec{
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

			token := "token-2"
			inputHash, err := runsidentity.ComputeTriggerInputHashFromRawExtension(nil)
			Expect(err).NotTo(HaveOccurred())
			expectedName := runsidentity.DeriveStoryRunName("default", "token-story-2", token)
			sr := &runsv1alpha1.StoryRun{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
					Name:      expectedName,
					Annotations: map[string]string{
						runsidentity.StoryRunTriggerTokenAnnotation:     token,
						runsidentity.StoryRunTriggerInputHashAnnotation: inputHash,
					},
				},
				Spec: runsv1alpha1.StoryRunSpec{
					StoryRef: refs.StoryReference{
						ObjectReference: refs.ObjectReference{
							Name: "token-story-2",
						},
					},
				},
			}

			_, err = validator.ValidateCreate(context.Background(), sr)
			Expect(err).NotTo(HaveOccurred())
		})

	})

	Context("inputs validation", func() {
		It("rejects non-object inputs", func() {
			story := &bubuv1alpha1.Story{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "inputs-shape-story",
					Namespace: "default",
				},
				Spec: bubuv1alpha1.StorySpec{
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

			sr := &runsv1alpha1.StoryRun{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
					Name:      "run-inputs-shape",
				},
				Spec: runsv1alpha1.StoryRunSpec{
					StoryRef: refs.StoryReference{
						ObjectReference: refs.ObjectReference{
							Name: "inputs-shape-story",
						},
					},
					Inputs: &runtime.RawExtension{Raw: []byte(`["not","object"]`)},
				},
			}

			_, err := validator.ValidateCreate(context.Background(), sr)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("spec.inputs must be a JSON object"))
		})

		It("rejects oversized inputs", func() {
			validator.Config.StoryRun.MaxInlineInputsSize = 16

			story := &bubuv1alpha1.Story{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "inputs-size-story",
					Namespace: "default",
				},
				Spec: bubuv1alpha1.StorySpec{
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

			payload := fmt.Sprintf(`{"data":"%s"}`, strings.Repeat("a", 64))
			sr := &runsv1alpha1.StoryRun{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
					Name:      "run-inputs-size",
				},
				Spec: runsv1alpha1.StoryRunSpec{
					StoryRef: refs.StoryReference{
						ObjectReference: refs.ObjectReference{
							Name: "inputs-size-story",
						},
					},
					Inputs: &runtime.RawExtension{Raw: []byte(payload)},
				},
			}

			_, err := validator.ValidateCreate(context.Background(), sr)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("spec.inputs too large"))
		})

		It("rejects inputs that do not match the Story schema", func() {
			schema := &runtime.RawExtension{Raw: []byte(`{
				"type": "object",
				"properties": {"name": {"type": "string"}},
				"required": ["name"]
			}`)}
			story := &bubuv1alpha1.Story{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "inputs-schema-story",
					Namespace: "default",
				},
				Spec: bubuv1alpha1.StorySpec{
					InputsSchema: schema,
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

			sr := &runsv1alpha1.StoryRun{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
					Name:      "run-inputs-schema",
				},
				Spec: runsv1alpha1.StoryRunSpec{
					StoryRef: refs.StoryReference{
						ObjectReference: refs.ObjectReference{
							Name: "inputs-schema-story",
						},
					},
					Inputs: &runtime.RawExtension{Raw: []byte(`{"missing": "name"}`)},
				},
			}

			_, err := validator.ValidateCreate(context.Background(), sr)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("spec.inputs is invalid against Story schema"))
		})

		It("accepts storage ref metadata during schema validation", func() {
			schema := &runtime.RawExtension{Raw: []byte(`{
				"type": "object",
				"properties": {
					"feeds": {
						"type": "object",
						"additionalProperties": false,
						"properties": {
							"$bubuStorageRef": {"type": "string"},
							"$bubuStoragePath": {"type": "string"}
						},
						"required": ["$bubuStorageRef"]
					}
				},
				"required": ["feeds"]
			}`)}
			story := &bubuv1alpha1.Story{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "inputs-schema-storage",
					Namespace: "default",
				},
				Spec: bubuv1alpha1.StorySpec{
					InputsSchema: schema,
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

			sr := &runsv1alpha1.StoryRun{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
					Name:      "run-inputs-storage",
				},
				Spec: runsv1alpha1.StoryRunSpec{
					StoryRef: refs.StoryReference{
						ObjectReference: refs.ObjectReference{
							Name: "inputs-schema-storage",
						},
					},
					Inputs: &runtime.RawExtension{Raw: []byte(`{
						"feeds": {
							"$bubuStorageRef": "inputs/feeds.json",
							"$bubuStorageContentType": "application/json",
							"$bubuStorageSchema": "bubu://engram/feeds",
							"$bubuStorageSchemaVersion": "v1"
						}
					}`)},
				},
			}

			_, err := validator.ValidateCreate(context.Background(), sr)
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Context("deletion short-circuit", func() {
		It("skips validation when deletionTimestamp is set", func() {
			now := metav1.Now()
			original := &runsv1alpha1.StoryRun{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
					Name:      "deleting-run",
				},
				Spec: runsv1alpha1.StoryRunSpec{
					StoryRef: refs.StoryReference{
						ObjectReference: refs.ObjectReference{
							Name: "story-a",
						},
					},
				},
			}

			updated := original.DeepCopy()
			updated.DeletionTimestamp = &now
			updated.Spec.Inputs = &runtime.RawExtension{Raw: []byte(`{"changed":true}`)}

			_, err := validator.ValidateUpdate(context.Background(), original, updated)
			Expect(err).NotTo(HaveOccurred())
		})
	})

})
