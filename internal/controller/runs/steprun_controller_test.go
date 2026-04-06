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
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	catalogv1alpha1 "github.com/bubustack/bobrapet/api/catalog/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	"github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/internal/config"
	"github.com/bubustack/bobrapet/pkg/enums"
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
			Namespace: "default",
		}
		steprun := &runsv1alpha1.StepRun{}
		cleanupStepRun := func() {
			existing := &runsv1alpha1.StepRun{}
			err := k8sClient.Get(ctx, typeNamespacedName, existing)
			if errors.IsNotFound(err) {
				return
			}
			Expect(err).NotTo(HaveOccurred())
			if len(existing.Finalizers) > 0 {
				patch := client.MergeFrom(existing.DeepCopy())
				existing.Finalizers = nil
				Expect(k8sClient.Patch(ctx, existing, patch)).To(Succeed())
			}
			Expect(k8sClient.Delete(ctx, existing)).To(Succeed())
			Eventually(func() bool {
				return errors.IsNotFound(k8sClient.Get(ctx, typeNamespacedName, &runsv1alpha1.StepRun{}))
			}, 5*time.Second, 100*time.Millisecond).Should(BeTrue())
		}
		cleanupStoryRun := func() {
			key := types.NamespacedName{Name: storyRunName, Namespace: "default"}
			existing := &runsv1alpha1.StoryRun{}
			err := k8sClient.Get(ctx, key, existing)
			if errors.IsNotFound(err) {
				return
			}
			Expect(err).NotTo(HaveOccurred())
			if len(existing.Finalizers) > 0 {
				patch := client.MergeFrom(existing.DeepCopy())
				existing.Finalizers = nil
				Expect(k8sClient.Patch(ctx, existing, patch)).To(Succeed())
			}
			Expect(k8sClient.Delete(ctx, existing)).To(Succeed())
			Eventually(func() bool {
				return errors.IsNotFound(k8sClient.Get(ctx, key, &runsv1alpha1.StoryRun{}))
			}, 5*time.Second, 100*time.Millisecond).Should(BeTrue())
		}
		cleanupStory := func() {
			key := types.NamespacedName{Name: storyName, Namespace: "default"}
			existing := &v1alpha1.Story{}
			err := k8sClient.Get(ctx, key, existing)
			if errors.IsNotFound(err) {
				return
			}
			Expect(err).NotTo(HaveOccurred())
			if len(existing.Finalizers) > 0 {
				patch := client.MergeFrom(existing.DeepCopy())
				existing.Finalizers = nil
				Expect(k8sClient.Patch(ctx, existing, patch)).To(Succeed())
			}
			Expect(k8sClient.Delete(ctx, existing)).To(Succeed())
			Eventually(func() bool {
				return errors.IsNotFound(k8sClient.Get(ctx, key, &v1alpha1.Story{}))
			}, 5*time.Second, 100*time.Millisecond).Should(BeTrue())
		}

		BeforeEach(func() {
			cleanupStepRun()
			cleanupStoryRun()
			cleanupStory()
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
			err := k8sClient.Create(ctx, story)
			if err != nil && !errors.IsAlreadyExists(err) {
				Expect(err).NotTo(HaveOccurred())
			}

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
			err = k8sClient.Create(ctx, storyRun)
			if err != nil && !errors.IsAlreadyExists(err) {
				Expect(err).NotTo(HaveOccurred())
			}

			err = k8sClient.Get(ctx, typeNamespacedName, steprun)
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
			cleanupStepRun()
			cleanupStoryRun()
			cleanupStory()
		})
		It("should successfully reconcile the resource", func() {
			By("Reconciling the created resource")
			configManager, err := config.NewOperatorConfigManager(k8sClient, "default", "bobrapet-operator-config")
			Expect(err).NotTo(HaveOccurred())
			controllerReconciler := &StepRunReconciler{
				ControllerDependencies: config.ControllerDependencies{
					Client:         k8sClient,
					Scheme:         k8sClient.Scheme(),
					ConfigResolver: config.NewResolver(k8sClient, configManager),
				},
			}

			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())
			// This smoke test verifies that reconcile accepts a minimal valid StepRun without error.
		})
	})

	Context("When job is missing but pods are still active", func() {
		const resourceName = "active-pod-step"
		const storyRunName = "active-pod-storyrun"
		const storyName = "active-pod-story"
		const engramName = "active-pod-engram"
		const templateName = "active-pod-template"

		ctx := context.Background()

		typeNamespacedName := types.NamespacedName{
			Name:      resourceName,
			Namespace: "default",
		}

		BeforeEach(func() {
			template := &catalogv1alpha1.EngramTemplate{
				ObjectMeta: metav1.ObjectMeta{
					Name: templateName,
				},
				Spec: catalogv1alpha1.EngramTemplateSpec{
					TemplateSpec: catalogv1alpha1.TemplateSpec{
						Version:        "1.0.0",
						SupportedModes: []enums.WorkloadMode{enums.WorkloadModeJob},
						Image:          "example/image:latest",
					},
				},
			}
			Expect(k8sClient.Create(ctx, template)).To(Succeed())

			engram := &v1alpha1.Engram{
				ObjectMeta: metav1.ObjectMeta{
					Name:      engramName,
					Namespace: "default",
				},
				Spec: v1alpha1.EngramSpec{
					TemplateRef: refs.EngramTemplateReference{
						Name: templateName,
					},
				},
			}
			Expect(k8sClient.Create(ctx, engram)).To(Succeed())

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
									Name: engramName,
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

			stepRun := &runsv1alpha1.StepRun{
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
					EngramRef: &refs.EngramReference{
						ObjectReference: refs.ObjectReference{
							Name: engramName,
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, stepRun)).To(Succeed())

			loaded := &runsv1alpha1.StepRun{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, loaded)).To(Succeed())
			loaded.Status.Phase = enums.PhaseRunning
			Expect(k8sClient.Status().Update(ctx, loaded)).To(Succeed())

			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "active-pod",
					Namespace: "default",
					Labels: map[string]string{
						"job-name": resourceName,
					},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:  "main",
							Image: "busybox",
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, pod)).To(Succeed())

			created := &corev1.Pod{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: pod.Name, Namespace: pod.Namespace}, created)).To(Succeed())
			created.Status.Phase = corev1.PodRunning
			Expect(k8sClient.Status().Update(ctx, created)).To(Succeed())
		})

		AfterEach(func() {
			stepRun := &runsv1alpha1.StepRun{}
			_ = k8sClient.Get(ctx, typeNamespacedName, stepRun)
			_ = k8sClient.Delete(ctx, stepRun)

			pod := &corev1.Pod{}
			_ = k8sClient.Get(ctx, types.NamespacedName{Name: "active-pod", Namespace: "default"}, pod)
			_ = k8sClient.Delete(ctx, pod)

			storyRun := &runsv1alpha1.StoryRun{}
			_ = k8sClient.Get(ctx, types.NamespacedName{Name: storyRunName, Namespace: "default"}, storyRun)
			_ = k8sClient.Delete(ctx, storyRun)

			story := &v1alpha1.Story{}
			_ = k8sClient.Get(ctx, types.NamespacedName{Name: storyName, Namespace: "default"}, story)
			_ = k8sClient.Delete(ctx, story)

			engram := &v1alpha1.Engram{}
			_ = k8sClient.Get(ctx, types.NamespacedName{Name: engramName, Namespace: "default"}, engram)
			_ = k8sClient.Delete(ctx, engram)

			template := &catalogv1alpha1.EngramTemplate{}
			_ = k8sClient.Get(ctx, types.NamespacedName{Name: templateName}, template)
			_ = k8sClient.Delete(ctx, template)
		})

		It("requeues without recreating the Job", func() {
			configManager, err := config.NewOperatorConfigManager(k8sClient, "default", "bobrapet-operator-config")
			Expect(err).NotTo(HaveOccurred())
			controllerReconciler := &StepRunReconciler{
				ControllerDependencies: config.ControllerDependencies{
					Client:         k8sClient,
					Scheme:         k8sClient.Scheme(),
					ConfigResolver: config.NewResolver(k8sClient, configManager),
				},
			}

			res, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(res.RequeueAfter).To(BeNumerically(">", 0))

			job := &batchv1.Job{}
			err = k8sClient.Get(ctx, types.NamespacedName{Name: resourceName, Namespace: "default"}, job)
			Expect(errors.IsNotFound(err)).To(BeTrue())
		})
	})
})
