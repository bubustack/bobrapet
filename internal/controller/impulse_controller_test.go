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

package controller

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	catalogv1alpha1 "github.com/bubustack/bobrapet/api/catalog/v1alpha1"
	bubushv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/internal/config"
	"github.com/bubustack/bobrapet/pkg/enums"
	"github.com/bubustack/bobrapet/pkg/refs"
)

var _ = Describe("Impulse Controller", func() {
	Context("When reconciling a resource", func() {
		const resourceName = "test-resource"
		const templateName = "test-template"
		const storyName = "test-story"

		ctx := context.Background()

		typeNamespacedName := types.NamespacedName{
			Name:      resourceName,
			Namespace: "default", // TODO(user):Modify as needed
		}
		impulse := &bubushv1alpha1.Impulse{}

		BeforeEach(func() {
			By("creating the custom resource for the Kind Impulse")

			// Create a valid ImpulseTemplate
			impulseTemplate := &catalogv1alpha1.ImpulseTemplate{
				ObjectMeta: metav1.ObjectMeta{
					Name: templateName,
				},
				Spec: catalogv1alpha1.ImpulseTemplateSpec{
					TemplateSpec: catalogv1alpha1.TemplateSpec{
						Version:        "1.0.0",
						SupportedModes: []enums.WorkloadMode{enums.WorkloadModeDeployment},
						Image:          "ghcr.io/bubustack/impulse-default:latest",
					},
				},
			}
			Expect(k8sClient.Create(ctx, impulseTemplate)).To(Succeed())

			// Create a valid Story
			story := &bubushv1alpha1.Story{
				ObjectMeta: metav1.ObjectMeta{
					Name:      storyName,
					Namespace: "default",
				},
				Spec: bubushv1alpha1.StorySpec{
					Steps: []bubushv1alpha1.Step{
						{
							Name: "step1",
							Ref: &refs.EngramReference{
								ObjectReference: refs.ObjectReference{
									Name: "some-engram",
								},
							},
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, story)).To(Succeed())

			err := k8sClient.Get(ctx, typeNamespacedName, impulse)
			if err != nil && errors.IsNotFound(err) {
				resource := &bubushv1alpha1.Impulse{
					ObjectMeta: metav1.ObjectMeta{
						Name:      resourceName,
						Namespace: "default",
					},
					Spec: bubushv1alpha1.ImpulseSpec{
						TemplateRef: refs.ImpulseTemplateReference{
							Name: templateName,
						},
						StoryRef: refs.StoryReference{
							ObjectReference: refs.ObjectReference{
								Name: storyName,
							},
						},
					},
				}
				Expect(k8sClient.Create(ctx, resource)).To(Succeed())
			}
		})

		AfterEach(func() {
			// TODO(user): Cleanup logic after each test, like removing the resource instance.
			resource := &bubushv1alpha1.Impulse{}
			err := k8sClient.Get(ctx, typeNamespacedName, resource)
			Expect(err).NotTo(HaveOccurred())

			By("Cleanup the specific resource instance Impulse")
			Expect(k8sClient.Delete(ctx, resource)).To(Succeed())

			// Delete the ImpulseTemplate
			impulseTemplate := &catalogv1alpha1.ImpulseTemplate{}
			err = k8sClient.Get(ctx, types.NamespacedName{Name: templateName}, impulseTemplate)
			Expect(err).NotTo(HaveOccurred())
			Expect(k8sClient.Delete(ctx, impulseTemplate)).To(Succeed())

			// Delete the Story
			story := &bubushv1alpha1.Story{}
			err = k8sClient.Get(ctx, types.NamespacedName{Name: storyName, Namespace: "default"}, story)
			Expect(err).NotTo(HaveOccurred())
			Expect(k8sClient.Delete(ctx, story)).To(Succeed())
		})
		It("should successfully reconcile the resource", func() {
			By("Reconciling the created resource")
			configManager := config.NewOperatorConfigManager(k8sClient, "default", "bobrapet-operator-config")
			controllerReconciler := &ImpulseReconciler{
				ControllerDependencies: config.ControllerDependencies{
					Client:         k8sClient,
					Scheme:         k8sClient.Scheme(),
					ConfigResolver: config.NewResolver(k8sClient, configManager),
				},
			}

			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())
			// TODO(user): Add more specific assertions depending on your controller's reconciliation logic.
			// Example: If you expect a certain status condition after reconciliation, verify it here.
		})
	})
})
