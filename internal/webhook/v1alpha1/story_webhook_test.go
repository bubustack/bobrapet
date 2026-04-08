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

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	catalogv1alpha1 "github.com/bubustack/bobrapet/api/catalog/v1alpha1"
	transportv1alpha1 "github.com/bubustack/bobrapet/api/transport/v1alpha1"
	bubushv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/pkg/enums"
	"github.com/bubustack/bobrapet/pkg/refs"
)

var _ = Describe("Story Webhook", func() {
	var (
		validator StoryCustomValidator
		defaulter StoryCustomDefaulter
	)

	BeforeEach(func() {
		validator = StoryCustomValidator{}
		validator.Client = k8sClient
		Expect(validator).NotTo(BeNil(), "Expected validator to be initialized")
		defaulter = StoryCustomDefaulter{}
		Expect(defaulter).NotTo(BeNil(), "Expected defaulter to be initialized")
	})

	Context("defaulting", func() {
		It("applies retry defaults to story policy and steps", func() {
			story := minimalStory("defaults")
			story.Spec.Policy = &bubushv1alpha1.StoryPolicy{}
			story.Spec.Steps[0].Execution = &bubushv1alpha1.ExecutionOverrides{}

			Expect(defaulter.Default(ctx, story)).To(Succeed())
			Expect(story.Spec.Policy.Retries).NotTo(BeNil())
			Expect(story.Spec.Policy.Retries.StepRetryPolicy).NotTo(BeNil())
			Expect(story.Spec.Policy.Retries.ContinueOnStepFailure).NotTo(BeNil())
			Expect(*story.Spec.Policy.Retries.ContinueOnStepFailure).To(BeFalse())
			Expect(story.Spec.Steps[0].Execution.Retry).NotTo(BeNil())
		})

		It("does not mutate status warnings while defaulting", func() {
			story := minimalStory("tpl-unknown-ref")
			story.Status.ValidationWarnings = []string{"preserve-me"}
			expr := "{{ eq .steps.missing.output.ok true }}"
			story.Spec.Steps = []bubushv1alpha1.Step{
				{
					Name: "fetch",
					Type: enums.StepTypeCondition,
				},
				{
					Name:  "process",
					Type:  enums.StepTypeCondition,
					Needs: []string{"fetch"},
					If:    &expr,
				},
			}

			Expect(defaulter.Default(ctx, story)).To(Succeed())
			Expect(story.Status.ValidationWarnings).To(Equal([]string{"preserve-me"}))
		})
	})

	Context("template reference warnings", func() {
		It("warns on unknown step references in template expressions", func() {
			story := minimalStory("tpl-unknown-ref")
			expr := "{{ eq .steps.missing.output.ok true }}"
			story.Spec.Steps = []bubushv1alpha1.Step{
				{
					Name: "fetch",
					Type: enums.StepTypeCondition,
				},
				{
					Name:  "process",
					Type:  enums.StepTypeCondition,
					Needs: []string{"fetch"},
					If:    &expr,
				},
			}

			warnings, err := validator.ValidateCreate(ctx, story)
			Expect(err).NotTo(HaveOccurred())
			Expect(warnings).To(HaveLen(1))
			Expect(warnings[0]).To(ContainSubstring("unknown step 'missing'"))
		})

		It("produces no warnings for valid upstream step references", func() {
			story := minimalStory("tpl-valid-ref")
			expr := "{{ eq .steps.fetch.output.ok true }}"
			story.Spec.Steps = []bubushv1alpha1.Step{
				{
					Name: "fetch",
					Type: enums.StepTypeCondition,
				},
				{
					Name:  "process",
					Type:  enums.StepTypeCondition,
					Needs: []string{"fetch"},
					If:    &expr,
				},
			}

			warnings, err := validator.ValidateCreate(ctx, story)
			Expect(err).NotTo(HaveOccurred())
			Expect(warnings).To(BeNil())
		})

		It("warns on unreachable step references in template expressions", func() {
			story := minimalStory("tpl-unreachable-ref")
			expr := "{{ eq .steps.stepb.output.ok true }}"
			story.Spec.Steps = []bubushv1alpha1.Step{
				{
					Name: "stepa",
					Type: enums.StepTypeCondition,
				},
				{
					Name: "stepb",
					Type: enums.StepTypeCondition,
				},
				{
					Name:  "stepc",
					Type:  enums.StepTypeCondition,
					Needs: []string{"stepa"},
					If:    &expr,
				},
			}

			warnings, err := validator.ValidateCreate(ctx, story)
			Expect(err).NotTo(HaveOccurred())
			Expect(warnings).To(HaveLen(1))
			Expect(warnings[0]).To(ContainSubstring("not in its upstream dependency chain"))
		})
	})

	Context("executeStory validation", func() {
		It("warns on executeStory steps with mismatched story version", func() {
			child := minimalStory("versioned-child")
			child.Spec.Version = "v1"
			Expect(k8sClient.Create(ctx, child)).To(Succeed())
			DeferCleanup(func() {
				_ = k8sClient.Delete(ctx, child)
			})

			story := minimalStory("parent-version")
			story.Spec.Steps = append(story.Spec.Steps, bubushv1alpha1.Step{
				Name: "invoke-child",
				Type: enums.StepTypeExecuteStory,
				With: mustRawExtension(map[string]any{
					"storyRef": map[string]any{"name": "versioned-child", "version": "v2"},
				}),
			})

			warnings, err := validator.ValidateCreate(ctx, story)
			Expect(err).NotTo(HaveOccurred())
			Expect(warnings).NotTo(BeEmpty())
			Expect(warnings[0]).To(ContainSubstring("version mismatch"))
		})

		It("rejects executeStory steps that reference stories in another namespace", func() {
			story := minimalStory("parent")
			story.Spec.Steps = append(story.Spec.Steps, bubushv1alpha1.Step{
				Name: "invoke-child",
				Type: enums.StepTypeExecuteStory,
				With: mustRawExtension(map[string]any{
					"storyRef": map[string]any{"name": "child", "namespace": "other"},
				}),
			})

			_, err := validator.ValidateCreate(ctx, story)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("cross-namespace StoryRef"))
		})

		It("warns on executeStory steps that reference missing stories", func() {
			story := minimalStory("parent")
			story.Spec.Steps = append(story.Spec.Steps, bubushv1alpha1.Step{
				Name: "invoke-child",
				Type: enums.StepTypeExecuteStory,
				With: mustRawExtension(map[string]any{
					"storyRef": map[string]any{"name": "missing"},
				}),
			})

			warnings, err := validator.ValidateCreate(ctx, story)
			Expect(err).NotTo(HaveOccurred())
			Expect(warnings).NotTo(BeEmpty())
			Expect(warnings[0]).To(ContainSubstring("not found yet"))
		})

		It("rejects executeStory steps that reference the same story", func() {
			story := minimalStory("loop")
			story.Spec.Steps = append(story.Spec.Steps, bubushv1alpha1.Step{
				Name: "self",
				Type: enums.StepTypeExecuteStory,
				With: mustRawExtension(map[string]any{
					"storyRef": map[string]any{"name": "loop"},
				}),
			})

			_, err := validator.ValidateCreate(ctx, story)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("cannot reference the same story"))
		})

		It("allows executeStory steps that reference existing stories", func() {
			child := minimalStory("child")
			Expect(k8sClient.Create(ctx, child)).To(Succeed())
			DeferCleanup(func() {
				_ = k8sClient.Delete(ctx, child)
			})

			story := minimalStory("parent")
			story.Spec.Steps = append(story.Spec.Steps, bubushv1alpha1.Step{
				Name: "invoke-child",
				Type: enums.StepTypeExecuteStory,
				With: mustRawExtension(map[string]any{
					"storyRef": map[string]any{"name": "child"},
				}),
			})

			warnings, err := validator.ValidateCreate(ctx, story)
			Expect(err).NotTo(HaveOccurred())
			Expect(warnings).To(BeNil())
		})
	})

	Context("version pinning", func() {
		It("warns on engram references with mismatched version", func() {
			template := &catalogv1alpha1.EngramTemplate{
				ObjectMeta: metav1.ObjectMeta{
					Name: "tpl-version",
				},
				Spec: catalogv1alpha1.EngramTemplateSpec{
					TemplateSpec: catalogv1alpha1.TemplateSpec{
						Version:        "v1",
						SupportedModes: []enums.WorkloadMode{enums.WorkloadModeJob},
					},
				},
			}
			Expect(k8sClient.Create(ctx, template)).To(Succeed())
			DeferCleanup(func() {
				_ = k8sClient.Delete(ctx, template)
			})

			engram := &bubushv1alpha1.Engram{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "engram-version",
					Namespace: "default",
				},
				Spec: bubushv1alpha1.EngramSpec{
					TemplateRef: refs.EngramTemplateReference{
						Name: "tpl-version",
					},
					Version: "v1",
				},
			}
			Expect(k8sClient.Create(ctx, engram)).To(Succeed())
			DeferCleanup(func() {
				_ = k8sClient.Delete(ctx, engram)
			})

			story := minimalStory("engram-versioned")
			story.Spec.Steps = []bubushv1alpha1.Step{{
				Name: "run",
				Ref: &refs.EngramReference{
					ObjectReference: refs.ObjectReference{
						Name: "engram-version",
					},
					Version: "v2",
				},
			}}

			warnings, err := validator.ValidateCreate(ctx, story)
			Expect(err).NotTo(HaveOccurred())
			Expect(warnings).NotTo(BeEmpty())
			Expect(warnings[0]).To(ContainSubstring("version mismatch"))
		})
	})

	Context("engram reference scoping", func() {
		It("rejects engram references in another namespace", func() {
			story := minimalStory("cross-engram")
			other := "other"
			story.Spec.Steps = []bubushv1alpha1.Step{{
				Name: "run",
				Ref: &refs.EngramReference{
					ObjectReference: refs.ObjectReference{
						Name:      "engram-a",
						Namespace: &other,
					},
				},
			}}

			_, err := validator.ValidateCreate(ctx, story)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("cross-namespace EngramRef"))
		})
	})

	Context("primitive validation", func() {
		It("rejects wait steps missing until", func() {
			story := minimalStory("wait-missing")
			story.Spec.Steps = []bubushv1alpha1.Step{{
				Name: "wait",
				Type: enums.StepTypeWait,
				With: mustRawExtension(map[string]any{}),
			}}

			_, err := validator.ValidateCreate(ctx, story)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("with.until"))
		})

		It("rejects wait steps with invalid timeout", func() {
			story := minimalStory("wait-timeout")
			story.Spec.Steps = []bubushv1alpha1.Step{{
				Name: "wait",
				Type: enums.StepTypeWait,
				With: mustRawExtension(map[string]any{
					"until":   "inputs.ready",
					"timeout": "-1s",
				}),
			}}

			_, err := validator.ValidateCreate(ctx, story)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("invalid wait timeout"))
		})

		It("accepts wait steps with valid until", func() {
			story := minimalStory("wait-ok")
			story.Spec.Steps = []bubushv1alpha1.Step{{
				Name: "wait",
				Type: enums.StepTypeWait,
				With: mustRawExtension(map[string]any{
					"until": "inputs.ready",
				}),
			}}

			_, err := validator.ValidateCreate(ctx, story)
			Expect(err).NotTo(HaveOccurred())
		})

		It("rejects gate steps with invalid onTimeout", func() {
			story := minimalStory("gate-invalid")
			story.Spec.Steps = []bubushv1alpha1.Step{{
				Name: "gate",
				Type: enums.StepTypeGate,
				With: mustRawExtension(map[string]any{
					"onTimeout": "hold",
				}),
			}}

			_, err := validator.ValidateCreate(ctx, story)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("invalid gate onTimeout"))
		})

		It("rejects gate steps in streaming stories", func() {
			story := minimalStory("gate-streaming")
			story.Spec.Pattern = enums.RealtimePattern
			story.Spec.Steps = []bubushv1alpha1.Step{{
				Name: "gate",
				Type: enums.StepTypeGate,
			}}

			_, err := validator.ValidateCreate(ctx, story)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("only supported for batch"))
		})

		It("rejects wait steps in streaming stories", func() {
			story := minimalStory("wait-streaming")
			story.Spec.Pattern = enums.RealtimePattern
			story.Spec.Steps = []bubushv1alpha1.Step{{
				Name: "wait",
				Type: enums.StepTypeWait,
				With: mustRawExtension(map[string]any{
					"until": "inputs.ready",
				}),
			}}

			_, err := validator.ValidateCreate(ctx, story)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("only supported for batch"))
		})

		It("allows compensations in streaming stories", func() {
			story := minimalStory("streaming-comp")
			story.Spec.Pattern = enums.RealtimePattern
			story.Spec.Compensations = []bubushv1alpha1.Step{{
				Name: "cleanup",
				Type: enums.StepTypeCondition,
			}}

			warnings, err := validator.ValidateCreate(ctx, story)
			Expect(err).NotTo(HaveOccurred())
			Expect(warnings).To(BeNil())
		})

		It("allows finally blocks in streaming stories", func() {
			story := minimalStory("streaming-finally")
			story.Spec.Pattern = enums.RealtimePattern
			story.Spec.Finally = []bubushv1alpha1.Step{{
				Name: "report",
				Type: enums.StepTypeCondition,
			}}

			warnings, err := validator.ValidateCreate(ctx, story)
			Expect(err).NotTo(HaveOccurred())
			Expect(warnings).To(BeNil())
		})

		It("allows both compensations and finally in streaming stories", func() {
			story := minimalStory("streaming-both")
			story.Spec.Pattern = enums.RealtimePattern
			story.Spec.Compensations = []bubushv1alpha1.Step{{
				Name: "rollback",
				Type: enums.StepTypeCondition,
			}}
			story.Spec.Finally = []bubushv1alpha1.Step{{
				Name: "notify",
				Type: enums.StepTypeCondition,
			}}

			warnings, err := validator.ValidateCreate(ctx, story)
			Expect(err).NotTo(HaveOccurred())
			Expect(warnings).To(BeNil())
		})
	})

	Context("Template expression context validation", func() {
		It("allows steps context in batch if expressions", func() {
			story := minimalStory("batch-if")
			expr := "{{ eq steps.prepare.output.ok true }}"
			story.Spec.Steps[0].If = &expr

			_, err := validator.ValidateCreate(ctx, story)
			Expect(err).NotTo(HaveOccurred())
		})

		It("rejects packet context in batch with blocks", func() {
			story := minimalStory("batch-packet")
			story.Spec.Steps[0].With = mustRawExtension(map[string]any{
				"value": "{{ packet.id }}",
			})

			_, err := validator.ValidateCreate(ctx, story)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("packet"))
		})

		It("rejects steps context in streaming with blocks", func() {
			story := minimalStory("streaming-steps")
			story.Spec.Pattern = enums.RealtimePattern
			story.Spec.Steps[0].With = mustRawExtension(map[string]any{
				"value": "{{ steps.prev.output }}",
			})

			_, err := validator.ValidateCreate(ctx, story)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("steps"))
		})

		It("rejects now() in streaming with blocks", func() {
			story := minimalStory("streaming-now")
			story.Spec.Pattern = enums.RealtimePattern
			story.Spec.Steps[0].With = mustRawExtension(map[string]any{
				"value": "{{ now }}",
			})

			_, err := validator.ValidateCreate(ctx, story)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("now"))
		})

		It("allows packet context in streaming runtime blocks", func() {
			story := minimalStory("streaming-runtime")
			story.Spec.Pattern = enums.RealtimePattern
			story.Spec.Steps[0].Runtime = mustRawExtension(map[string]any{
				"value": "{{ packet.id }}",
			})

			_, err := validator.ValidateCreate(ctx, story)
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Context("runtime bridging", func() {
		It("allows runtime blocks in batch stories when engram runs as deployment", func() {
			template := &catalogv1alpha1.EngramTemplate{
				ObjectMeta: metav1.ObjectMeta{
					Name: "tpl-bridge",
				},
				Spec: catalogv1alpha1.EngramTemplateSpec{
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

			engram := &bubushv1alpha1.Engram{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "engram-bridge",
					Namespace: "default",
				},
				Spec: bubushv1alpha1.EngramSpec{
					TemplateRef: refs.EngramTemplateReference{
						Name: "tpl-bridge",
					},
					Mode: enums.WorkloadModeDeployment,
				},
			}
			Expect(k8sClient.Create(ctx, engram)).To(Succeed())
			DeferCleanup(func() {
				_ = k8sClient.Delete(ctx, engram)
			})

			story := minimalStory("batch-runtime-bridge")
			story.Spec.Steps = []bubushv1alpha1.Step{{
				Name: "run",
				Ref: &refs.EngramReference{
					ObjectReference: refs.ObjectReference{
						Name: "engram-bridge",
					},
				},
				Runtime: mustRawExtension(map[string]any{
					"value": "{{ packet.id }}",
				}),
			}}

			warnings, err := validator.ValidateCreate(ctx, story)
			Expect(err).NotTo(HaveOccurred())
			Expect(warnings).To(BeNil())
		})

		It("allows runtime blocks in batch stories when a transport is assigned", func() {
			template := &catalogv1alpha1.EngramTemplate{
				ObjectMeta: metav1.ObjectMeta{
					Name: "tpl-transport",
				},
				Spec: catalogv1alpha1.EngramTemplateSpec{
					TemplateSpec: catalogv1alpha1.TemplateSpec{
						Version:        "v1",
						SupportedModes: []enums.WorkloadMode{enums.WorkloadModeJob},
					},
				},
			}
			Expect(k8sClient.Create(ctx, template)).To(Succeed())
			DeferCleanup(func() {
				_ = k8sClient.Delete(ctx, template)
			})

			engram := &bubushv1alpha1.Engram{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "engram-transport",
					Namespace: "default",
				},
				Spec: bubushv1alpha1.EngramSpec{
					TemplateRef: refs.EngramTemplateReference{
						Name: "tpl-transport",
					},
				},
			}
			Expect(k8sClient.Create(ctx, engram)).To(Succeed())
			DeferCleanup(func() {
				_ = k8sClient.Delete(ctx, engram)
			})

			story := minimalStory("batch-runtime-transport")
			story.Spec.Transports = []bubushv1alpha1.StoryTransport{
				{
					Name:         "voice",
					TransportRef: "livekit",
				},
			}
			story.Spec.Steps = []bubushv1alpha1.Step{{
				Name:      "run",
				Transport: "voice",
				Ref: &refs.EngramReference{
					ObjectReference: refs.ObjectReference{
						Name: "engram-transport",
					},
				},
				Runtime: mustRawExtension(map[string]any{
					"value": "{{ packet.id }}",
				}),
			}}

			warnings, err := validator.ValidateCreate(ctx, story)
			Expect(err).NotTo(HaveOccurred())
			Expect(warnings).To(BeNil())
		})
	})

	Context("Templating safety", func() {
		It("rejects disallowed template functions", func() {
			story := minimalStory("disallowed-template")
			expr := "{{ env \"HOME\" }}" //nolint:goconst
			story.Spec.Steps[0].If = &expr

			_, err := validator.ValidateCreate(ctx, story)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("disallowed function 'env'"))
		})
	})

	Context("idempotencyKeyTemplate validation", func() {
		It("accepts a valid idempotency key template", func() {
			story := minimalStory("idem-valid")
			tpl := "notify-{{ .inputs.orderId }}"
			story.Spec.Steps[0].IdempotencyKeyTemplate = &tpl

			_, err := validator.ValidateCreate(ctx, story)
			Expect(err).NotTo(HaveOccurred())
		})

		It("rejects an idempotency key template with invalid syntax", func() {
			story := minimalStory("idem-invalid")
			tpl := "notify-{{ .inputs.orderId"
			story.Spec.Steps[0].IdempotencyKeyTemplate = &tpl

			_, err := validator.ValidateCreate(ctx, story)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("idempotencyKeyTemplate"))
		})

		It("rejects an idempotency key template with disallowed functions", func() {
			story := minimalStory("idem-disallowed")
			tpl := "{{ env \"HOME\" }}"
			story.Spec.Steps[0].IdempotencyKeyTemplate = &tpl

			_, err := validator.ValidateCreate(ctx, story)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("idempotencyKeyTemplate"))
		})
	})

	Context("policy.with validation", func() {
		It("rejects policy.with blocks with disallowed template functions", func() {
			story := minimalStory("policy-with-bad")
			story.Spec.Policy = &bubushv1alpha1.StoryPolicy{
				With: mustRawExtension(map[string]any{
					"value": "{{ env \"HOME\" }}",
				}),
			}

			_, err := validator.ValidateCreate(ctx, story)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("disallowed function"))
		})

		It("accepts policy.with blocks with valid templates", func() {
			story := minimalStory("policy-with-ok")
			story.Spec.Policy = &bubushv1alpha1.StoryPolicy{
				With: mustRawExtension(map[string]any{
					"value": "{{ inputs.foo }}",
				}),
			}

			_, err := validator.ValidateCreate(ctx, story)
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Context("Engram input schema validation", func() {
		It("rejects engram steps missing required inputs when config is empty", func() {
			template := &catalogv1alpha1.EngramTemplate{
				ObjectMeta: metav1.ObjectMeta{
					Name: "tpl-required",
				},
				Spec: catalogv1alpha1.EngramTemplateSpec{
					TemplateSpec: catalogv1alpha1.TemplateSpec{
						Version:        "v1",
						SupportedModes: []enums.WorkloadMode{enums.WorkloadModeJob},
					},
					InputSchema: mustRawExtension(map[string]any{
						"type": "object",
						"properties": map[string]any{
							"foo": map[string]any{
								"type": "string",
							},
						},
						"required": []string{"foo"},
					}),
				},
			}
			Expect(k8sClient.Create(ctx, template)).To(Succeed())
			DeferCleanup(func() {
				_ = k8sClient.Delete(ctx, template)
			})

			engram := &bubushv1alpha1.Engram{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "engram-required",
					Namespace: "default",
				},
				Spec: bubushv1alpha1.EngramSpec{
					TemplateRef: refs.EngramTemplateReference{
						Name: "tpl-required",
					},
				},
			}
			Expect(k8sClient.Create(ctx, engram)).To(Succeed())
			DeferCleanup(func() {
				_ = k8sClient.Delete(ctx, engram)
			})

			story := minimalStory("engram-required")
			story.Spec.Steps = []bubushv1alpha1.Step{{
				Name: "run",
				Ref: &refs.EngramReference{
					ObjectReference: refs.ObjectReference{
						Name: "engram-required",
					},
				},
			}}

			_, err := validator.ValidateCreate(ctx, story)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("required"))
		})
	})

	Context("transport validation", func() {
		It("rejects duplicate transport names", func() {
			story := minimalStory("transports")
			story.Spec.Transports = []bubushv1alpha1.StoryTransport{
				{Name: "voice", TransportRef: "livekit"},
				{Name: "voice", TransportRef: "fallback"},
			}
			_, err := validator.ValidateCreate(ctx, story)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("duplicated"))
		})

		It("rejects transports missing transportRef", func() {
			story := minimalStory("transports")
			story.Spec.Transports = []bubushv1alpha1.StoryTransport{
				{Name: "voice"},
			}
			_, err := validator.ValidateCreate(ctx, story)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("must set transportRef"))
		})

		It("rejects steps referencing undeclared transports", func() {
			story := minimalStory("transports")
			story.Spec.Transports = []bubushv1alpha1.StoryTransport{
				{Name: "voice", TransportRef: "livekit"},
			}
			story.Spec.Steps[0].Transport = "video"

			_, err := validator.ValidateCreate(ctx, story)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("references unknown transport"))
		})

		It("rejects routing rules targeting unknown steps", func() {
			story := minimalStory("transport-routing-unknown-step")
			story.Spec.Pattern = enums.RealtimePattern
			story.Spec.Transports = []bubushv1alpha1.StoryTransport{
				{
					Name:         "voice",
					TransportRef: "livekit",
					Streaming: &transportv1alpha1.TransportStreamingSettings{
						Routing: &transportv1alpha1.TransportRoutingSettings{
							Rules: []transportv1alpha1.TransportRoutingRule{
								{
									Target: &transportv1alpha1.TransportRoutingRuleTarget{
										Steps: []string{"unknown"},
									},
								},
							},
						},
					},
				},
			}

			_, err := validator.ValidateCreate(ctx, story)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("unknown step"))
		})

		It("rejects routing rules with invalid expressions", func() {
			story := minimalStory("transport-routing-expression")
			story.Spec.Pattern = enums.RealtimePattern
			expr := "{{ env \"HOME\" }}"
			story.Spec.Transports = []bubushv1alpha1.StoryTransport{
				{
					Name:         "voice",
					TransportRef: "livekit",
					Streaming: &transportv1alpha1.TransportStreamingSettings{
						Routing: &transportv1alpha1.TransportRoutingSettings{
							Rules: []transportv1alpha1.TransportRoutingRule{
								{
									When: &expr,
								},
							},
						},
					},
				},
			}

			_, err := validator.ValidateCreate(ctx, story)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("disallowed function"))
		})

		It("rejects routing rules with unsupported actions", func() {
			story := minimalStory("transport-routing-action")
			story.Spec.Pattern = enums.RealtimePattern
			story.Spec.Transports = []bubushv1alpha1.StoryTransport{
				{
					Name:         "voice",
					TransportRef: "livekit",
					Streaming: &transportv1alpha1.TransportStreamingSettings{
						Routing: &transportv1alpha1.TransportRoutingSettings{
							Rules: []transportv1alpha1.TransportRoutingRule{
								{
									Action: transportv1alpha1.TransportRoutingRuleAction("block"),
								},
							},
						},
					},
				},
			}

			_, err := validator.ValidateCreate(ctx, story)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("unsupported routing action"))
		})

		It("rejects invalid transport streaming settings", func() {
			story := minimalStory("transport-streaming-invalid")
			story.Spec.Pattern = enums.RealtimePattern
			story.Spec.Transports = []bubushv1alpha1.StoryTransport{
				{
					Name:         "voice",
					TransportRef: "livekit",
					Streaming: &transportv1alpha1.TransportStreamingSettings{
						Backpressure: &transportv1alpha1.TransportBackpressureSettings{
							Buffer: &transportv1alpha1.TransportBufferSettings{
								MaxMessages: new(int32(-1)),
							},
						},
						FlowControl: &transportv1alpha1.TransportFlowControlSettings{
							Mode: transportv1alpha1.FlowControlMode("bogus"),
							AckEvery: &transportv1alpha1.TransportFlowAckSettings{
								MaxDelay: new("not-a-duration"),
							},
						},
						Delivery: &transportv1alpha1.TransportDeliverySettings{
							Replay: &transportv1alpha1.TransportReplaySettings{
								CheckpointInterval: new("bad"),
							},
						},
						Routing: &transportv1alpha1.TransportRoutingSettings{
							Mode:           transportv1alpha1.TransportRoutingMode("mesh"),
							FanOut:         transportv1alpha1.TransportFanOutMode("fanout"),
							MaxDownstreams: new(int32(0)),
						},
						Partitioning: &transportv1alpha1.TransportPartitioningSettings{
							Mode:       transportv1alpha1.TransportPartitionMode("range"),
							Key:        new(" "),
							Partitions: new(int32(0)),
						},
						Lifecycle: &transportv1alpha1.TransportLifecycleSettings{
							Strategy:            transportv1alpha1.TransportUpgradeStrategy("invalid"),
							DrainTimeoutSeconds: new(int32(-1)),
						},
						Observability: &transportv1alpha1.TransportObservabilitySettings{
							Tracing: &transportv1alpha1.TransportTracingSettings{
								SampleRate:   new(int32(200)),
								SamplePolicy: new("maybe"),
							},
						},
						Recording: &transportv1alpha1.TransportRecordingSettings{
							Mode:             transportv1alpha1.TransportRecordingMode("full"),
							SampleRate:       new(int32(200)),
							RetentionSeconds: new(int32(-1)),
							RedactFields:     []string{""},
						},
					},
				},
			}

			_, err := validator.ValidateCreate(ctx, story)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("streaming"))
		})
	})

	Context("requires validation", func() {
		It("accepts requires referencing an upstream step", func() {
			story := minimalStory("requires-valid")
			story.Spec.Steps = []bubushv1alpha1.Step{
				{Name: "fetch", Type: enums.StepTypeCondition},
				{Name: "process", Type: enums.StepTypeCondition, Needs: []string{"fetch"}, Requires: []string{"steps.fetch.output.body"}},
			}
			_, err := validator.ValidateCreate(ctx, story)
			Expect(err).NotTo(HaveOccurred())
		})

		It("rejects requires referencing an unknown step", func() {
			story := minimalStory("requires-unknown")
			story.Spec.Steps = []bubushv1alpha1.Step{
				{Name: "process", Type: enums.StepTypeCondition, Requires: []string{"steps.nonexistent.output.body"}},
			}
			_, err := validator.ValidateCreate(ctx, story)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("unknown step"))
		})

		It("rejects requires referencing a step not in the upstream chain", func() {
			story := minimalStory("requires-not-upstream")
			story.Spec.Steps = []bubushv1alpha1.Step{
				{Name: "fetch", Type: enums.StepTypeCondition},
				{Name: "process", Type: enums.StepTypeCondition, Requires: []string{"steps.fetch.output.body"}},
			}
			_, err := validator.ValidateCreate(ctx, story)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("not upstream"))
		})

		It("rejects requires with invalid prefix", func() {
			story := minimalStory("requires-bad-prefix")
			story.Spec.Steps = []bubushv1alpha1.Step{
				{Name: "process", Type: enums.StepTypeCondition, Requires: []string{"invalid-path"}},
			}
			_, err := validator.ValidateCreate(ctx, story)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("must start with 'steps.'"))
		})

		It("rejects requires with too few segments", func() {
			story := minimalStory("requires-short")
			story.Spec.Steps = []bubushv1alpha1.Step{
				{Name: "process", Type: enums.StepTypeCondition, Requires: []string{"steps.foo"}},
			}
			_, err := validator.ValidateCreate(ctx, story)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("at least 3 segments"))
		})

		It("accepts transitive upstream requires", func() {
			story := minimalStory("requires-transitive")
			story.Spec.Steps = []bubushv1alpha1.Step{
				{Name: "a", Type: enums.StepTypeCondition},
				{Name: "b", Type: enums.StepTypeCondition, Needs: []string{"a"}},
				{Name: "c", Type: enums.StepTypeCondition, Needs: []string{"b"}, Requires: []string{"steps.a.output.body"}},
			}
			_, err := validator.ValidateCreate(ctx, story)
			Expect(err).NotTo(HaveOccurred())
		})
	})

})

func minimalStory(name string) *bubushv1alpha1.Story {
	return &bubushv1alpha1.Story{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      name,
		},
		Spec: bubushv1alpha1.StorySpec{
			Steps: []bubushv1alpha1.Step{{
				Name: "prepare",
				Type: enums.StepTypeCondition,
			}},
		},
	}
}

func mustRawExtension(payload any) *runtime.RawExtension {
	raw, err := toRawExtension(payload)
	Expect(err).NotTo(HaveOccurred())
	return raw
}

func toRawExtension(payload any) (*runtime.RawExtension, error) {
	if payload == nil {
		return nil, nil
	}
	switch v := payload.(type) {
	case *runtime.RawExtension:
		return v.DeepCopy(), nil
	case []byte:
		return &runtime.RawExtension{Raw: append([]byte(nil), v...)}, nil
	default:
		data, err := json.Marshal(payload)
		if err != nil {
			return nil, err
		}
		return &runtime.RawExtension{Raw: data}, nil
	}
}
