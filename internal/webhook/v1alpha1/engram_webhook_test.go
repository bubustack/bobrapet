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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	catalogv1alpha1 "github.com/bubustack/bobrapet/api/catalog/v1alpha1"
	bubushv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/pkg/enums"
	"github.com/bubustack/bobrapet/pkg/refs"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var _ = Describe("Engram Webhook", func() {
	var (
		obj       *bubushv1alpha1.Engram
		oldObj    *bubushv1alpha1.Engram
		validator EngramCustomValidator
		defaulter EngramCustomDefaulter
	)

	BeforeEach(func() {
		obj = &bubushv1alpha1.Engram{}
		oldObj = &bubushv1alpha1.Engram{}
		validator = EngramCustomValidator{Client: k8sClient}
		Expect(validator).NotTo(BeNil(), "Expected validator to be initialized")
		defaulter = EngramCustomDefaulter{}
		Expect(defaulter).NotTo(BeNil(), "Expected defaulter to be initialized")
		Expect(oldObj).NotTo(BeNil(), "Expected oldObj to be initialized")
		Expect(obj).NotTo(BeNil(), "Expected obj to be initialized")
	})

	AfterEach(func() {
		// TODO (user): Add any teardown logic common to all tests
	})

	Context("When creating Engram under Defaulting Webhook", func() {
		// TODO (user): Add logic for defaulting webhooks
		// Example:
		// It("Should apply defaults when a required field is empty", func() {
		//     By("simulating a scenario where defaults should be applied")
		//     obj.SomeFieldWithDefault = ""
		//     By("calling the Default method to apply defaults")
		//     defaulter.Default(ctx, obj)
		//     By("checking that the default values are set")
		//     Expect(obj.SomeFieldWithDefault).To(Equal("default_value"))
		// })
	})

	Context("When creating or updating Engram under Validating Webhook", func() {
		It("warns when referenced secrets do not exist", func() {
			template := &catalogv1alpha1.EngramTemplate{
				ObjectMeta: metav1.ObjectMeta{
					Name: "tpl-secret-warn",
				},
				Spec: catalogv1alpha1.EngramTemplateSpec{
					TemplateSpec: catalogv1alpha1.TemplateSpec{
						Version:        "v1",
						SupportedModes: []enums.WorkloadMode{enums.WorkloadModeJob},
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

			engram := &bubushv1alpha1.Engram{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "engram-secret-warn",
					Namespace: "default",
				},
				Spec: bubushv1alpha1.EngramSpec{
					TemplateRef: refs.EngramTemplateReference{
						Name: "tpl-secret-warn",
					},
					Secrets: map[string]string{
						"apiKey": "missing-secret",
					},
				},
			}

			warnings, err := validator.ValidateCreate(context.Background(), engram)
			Expect(err).NotTo(HaveOccurred())
			Expect(warnings).NotTo(BeEmpty())
			Expect(warnings[0]).To(ContainSubstring("not found"))
		})

		It("rejects spec changes when deletion is in progress", func() {
			now := metav1.Now()
			oldEngram := &bubushv1alpha1.Engram{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "engram-deleting",
					Namespace: "default",
				},
				Spec: bubushv1alpha1.EngramSpec{
					TemplateRef: refs.EngramTemplateReference{
						Name: "tpl-old",
					},
				},
			}
			newEngram := oldEngram.DeepCopy()
			newEngram.DeletionTimestamp = &now
			newEngram.Spec.TemplateRef.Name = "tpl-new"

			_, err := validator.ValidateUpdate(context.Background(), oldEngram, newEngram)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("deletion"))
		})

		It("rejects template version mismatches", func() {
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
						Name:    "tpl-version",
						Version: "v2",
					},
				},
			}

			_, err := validator.ValidateCreate(context.Background(), engram)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("expected template version"))
		})

		It("rejects unsupported workload modes", func() {
			template := &catalogv1alpha1.EngramTemplate{
				ObjectMeta: metav1.ObjectMeta{
					Name: "tpl-modes",
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
					Name:      "engram-modes",
					Namespace: "default",
				},
				Spec: bubushv1alpha1.EngramSpec{
					TemplateRef: refs.EngramTemplateReference{
						Name: "tpl-modes",
					},
					Mode: enums.WorkloadModeStatefulSet,
				},
			}

			_, err := validator.ValidateCreate(context.Background(), engram)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("supportedModes"))
		})

		It("validates secrets against template secretSchema keys", func() {
			template := &catalogv1alpha1.EngramTemplate{
				ObjectMeta: metav1.ObjectMeta{
					Name: "tpl-secrets",
				},
				Spec: catalogv1alpha1.EngramTemplateSpec{
					TemplateSpec: catalogv1alpha1.TemplateSpec{
						Version:        "v1",
						SupportedModes: []enums.WorkloadMode{enums.WorkloadModeJob},
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

			engram := &bubushv1alpha1.Engram{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "engram-secrets",
					Namespace: "default",
				},
				Spec: bubushv1alpha1.EngramSpec{
					TemplateRef: refs.EngramTemplateReference{
						Name: "tpl-secrets",
					},
					Secrets: map[string]string{
						"unknown": "some-secret",
					},
				},
			}

			_, err := validator.ValidateCreate(context.Background(), engram)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("secret \"apiKey\" is required"))
			Expect(err.Error()).To(ContainSubstring("secret \"unknown\" is not defined"))
		})

		It("skips validation when spec is unchanged on update", func() {
			oldEngram := &bubushv1alpha1.Engram{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "engram-skip",
					Namespace: "default",
				},
				Spec: bubushv1alpha1.EngramSpec{
					TemplateRef: refs.EngramTemplateReference{
						Name: "missing-template",
					},
				},
			}
			newEngram := oldEngram.DeepCopy()
			newEngram.Labels = map[string]string{"note": "metadata-only"}

			_, err := validator.ValidateUpdate(context.Background(), oldEngram, newEngram)
			Expect(err).NotTo(HaveOccurred())
		})
	})

})
