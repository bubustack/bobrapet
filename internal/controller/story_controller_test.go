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

	"github.com/bubustack/bobrapet/internal/config"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	bubushv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/pkg/refs"
)

var _ = Describe("Story Controller", func() {
	Context("When reconciling a resource", func() {
		const resourceName = "test-resource"

		ctx := context.Background()

		typeNamespacedName := types.NamespacedName{
			Name:      resourceName,
			Namespace: "default", // TODO(user):Modify as needed
		}
		story := &bubushv1alpha1.Story{}

		BeforeEach(func() {
			By("creating the custom resource for the Kind Story")
			err := k8sClient.Get(ctx, typeNamespacedName, story)
			if err != nil && errors.IsNotFound(err) {
				resource := &bubushv1alpha1.Story{
					ObjectMeta: metav1.ObjectMeta{
						Name:      resourceName,
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
				Expect(k8sClient.Create(ctx, resource)).To(Succeed())
			}
		})

		AfterEach(func() {
			// TODO(user): Cleanup logic after each test, like removing the resource instance.
			resource := &bubushv1alpha1.Story{}
			err := k8sClient.Get(ctx, typeNamespacedName, resource)
			Expect(err).NotTo(HaveOccurred())

			By("Cleanup the specific resource instance Story")
			Expect(k8sClient.Delete(ctx, resource)).To(Succeed())
		})
		It("should successfully reconcile the resource", func() {
			By("Reconciling the created resource")
			configManager := config.NewOperatorConfigManager(k8sClient, "default", "bobrapet-operator-config")
			controllerReconciler := &StoryReconciler{
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
