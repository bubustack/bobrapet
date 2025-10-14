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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	"github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/internal/config"
	"github.com/bubustack/bobrapet/pkg/refs"
)

var _ = Describe("StepRun Controller", func() {
	Context("When reconciling a resource", func() {
		const resourceName = "test-resource"
		const storyRunName = "test-storyrun"
		const storyName = "test-story"

		ctx := context.Background()

		typeNamespacedName := types.NamespacedName{
			Name:      resourceName,
			Namespace: "default", // TODO(user):Modify as needed
		}
		steprun := &runsv1alpha1.StepRun{}

		BeforeEach(func() {
			By("creating the custom resource for the Kind StepRun")

			// Create a valid Story and StoryRun
			story := &v1alpha1.Story{
				ObjectMeta: metav1.ObjectMeta{
					Name:      storyName,
					Namespace: "default",
				},
				Spec: v1alpha1.StorySpec{
					Steps: []v1alpha1.Step{
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

			storyRun := &runsv1alpha1.StoryRun{
				ObjectMeta: metav1.ObjectMeta{
					Name:      storyRunName,
					Namespace: "default",
				},
				Spec: runsv1alpha1.StoryRunSpec{
					StoryRef: refs.StoryReference{
						ObjectReference: refs.ObjectReference{
							Name: storyName,
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, storyRun)).To(Succeed())

			err := k8sClient.Get(ctx, typeNamespacedName, steprun)
			if err != nil && errors.IsNotFound(err) {
				resource := &runsv1alpha1.StepRun{
					ObjectMeta: metav1.ObjectMeta{
						Name:      resourceName,
						Namespace: "default",
					},
					Spec: runsv1alpha1.StepRunSpec{
						StepID: "step1",
						StoryRunRef: refs.StoryRunReference{
							ObjectReference: refs.ObjectReference{
								Name: storyRunName,
							},
						},
					},
				}
				Expect(k8sClient.Create(ctx, resource)).To(Succeed())
			}
		})

		AfterEach(func() {
			// TODO(user): Cleanup logic after each test, like removing the resource instance.
			resource := &runsv1alpha1.StepRun{}
			err := k8sClient.Get(ctx, typeNamespacedName, resource)
			Expect(err).NotTo(HaveOccurred())

			By("Cleanup the specific resource instance StepRun")
			Expect(k8sClient.Delete(ctx, resource)).To(Succeed())

			// Delete the StoryRun and Story
			storyRun := &runsv1alpha1.StoryRun{}
			err = k8sClient.Get(ctx, types.NamespacedName{Name: storyRunName, Namespace: "default"}, storyRun)
			Expect(err).NotTo(HaveOccurred())
			Expect(k8sClient.Delete(ctx, storyRun)).To(Succeed())

			story := &v1alpha1.Story{}
			err = k8sClient.Get(ctx, types.NamespacedName{Name: storyName, Namespace: "default"}, story)
			Expect(err).NotTo(HaveOccurred())
			Expect(k8sClient.Delete(ctx, story)).To(Succeed())
		})
		It("should successfully reconcile the resource", func() {
			By("Reconciling the created resource")
			configManager := config.NewOperatorConfigManager(k8sClient, "default", "bobrapet-operator-config")
			controllerReconciler := &StepRunReconciler{
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
