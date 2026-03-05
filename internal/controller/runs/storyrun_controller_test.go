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
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	"github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/internal/config"
	"github.com/bubustack/bobrapet/pkg/enums"
	"github.com/bubustack/bobrapet/pkg/refs"
	runsidentity "github.com/bubustack/bobrapet/pkg/runs/identity"
	"github.com/bubustack/core/contracts"
)

var _ = Describe("StoryRun Controller", func() {
	Context("When reconciling a resource", func() {
		const resourceName = "test-resource"
		const storyName = "test-story"

		ctx := context.Background()

		typeNamespacedName := types.NamespacedName{
			Name:      resourceName,
			Namespace: "default", // TODO(user):Modify as needed
		}
		storyrun := &runsv1alpha1.StoryRun{}
		cleanupStoryRun := func() {
			existing := &runsv1alpha1.StoryRun{}
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
				return errors.IsNotFound(k8sClient.Get(ctx, typeNamespacedName, &runsv1alpha1.StoryRun{}))
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
		buildReconciler := func() *StoryRunReconciler {
			configManager := config.NewOperatorConfigManager(k8sClient, "default", "bobrapet-operator-config")
			return &StoryRunReconciler{
				ControllerDependencies: config.ControllerDependencies{
					Client:         k8sClient,
					Scheme:         k8sClient.Scheme(),
					ConfigResolver: config.NewResolver(k8sClient, configManager),
				},
				rbacManager: NewRBACManager(k8sClient, k8sClient.Scheme()),
			}
		}

		BeforeEach(func() {
			cleanupStoryRun()
			cleanupStory()
			By("creating the custom resource for the Kind StoryRun")
			// Create a valid Story
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

			err = k8sClient.Get(ctx, typeNamespacedName, storyrun)
			if err != nil && errors.IsNotFound(err) {
				resource := &runsv1alpha1.StoryRun{
					ObjectMeta: metav1.ObjectMeta{
						Name:      resourceName,
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
				Expect(k8sClient.Create(ctx, resource)).To(Succeed())
			}
		})

		AfterEach(func() {
			cleanupStoryRun()
			cleanupStory()
		})
		It("should successfully reconcile the resource", func() {
			By("Reconciling the created resource")
			controllerReconciler := buildReconciler()

			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())
			// TODO(user): Add more specific assertions depending on your controller's reconciliation logic.
			// Example: If you expect a certain status condition after reconciliation, verify it here.
		})

		It("should reset status on redrive token", func() {
			By("seeding StoryRun status with non-empty fields")
			current := &runsv1alpha1.StoryRun{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, current)).To(Succeed())
			base := current.DeepCopy()
			now := metav1.Now()
			current.Status = runsv1alpha1.StoryRunStatus{
				ObservedGeneration: 5,
				Phase:              enums.PhaseRunning,
				Message:            "running",
				Active:             []string{"step1"},
				Completed:          []string{"step0"},
				PrimitiveChildren:  map[string][]string{"parallel": {"child-1"}},
				StartedAt:          &now,
				FinishedAt:         &now,
				Duration:           "1s",
				Attempts:           2,
				Output:             &runtime.RawExtension{Raw: []byte(`{"ok":true}`)},
				Error:              &runtime.RawExtension{Raw: []byte(`{"error":"boom"}`)},
				StepStates: map[string]runsv1alpha1.StepState{
					"step1": {Phase: enums.PhaseRunning},
				},
				Gates: map[string]runsv1alpha1.GateStatus{
					"gate1": {State: runsv1alpha1.GateDecisionApproved, UpdatedAt: &now, UpdatedBy: "tester"},
				},
				Conditions: []metav1.Condition{
					{
						Type:               "Ready",
						Status:             metav1.ConditionFalse,
						Reason:             "Running",
						Message:            "in progress",
						LastTransitionTime: now,
					},
				},
				StepsTotal:    2,
				StepsComplete: 1,
				StepsFailed:   1,
				StepsSkipped:  0,
				AllowedFailures: []string{
					"step0",
				},
				TriggerTokens: []string{"story"},
			}
			Expect(k8sClient.Status().Patch(ctx, current, client.MergeFrom(base))).To(Succeed())

			By("requesting a redrive via annotation")
			Expect(k8sClient.Get(ctx, typeNamespacedName, current)).To(Succeed())
			patch := client.MergeFrom(current.DeepCopy())
			ann := current.GetAnnotations()
			if ann == nil {
				ann = map[string]string{}
			}
			ann[runsidentity.StoryRunRedriveTokenAnnotation] = "token-1"
			current.SetAnnotations(ann)
			Expect(k8sClient.Patch(ctx, current, patch)).To(Succeed())

			controllerReconciler := buildReconciler()
			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			updated := &runsv1alpha1.StoryRun{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, updated)).To(Succeed())
			Expect(updated.Status.Phase).To(BeEmpty())
			Expect(updated.Status.Message).To(BeEmpty())
			Expect(updated.Status.Active).To(BeNil())
			Expect(updated.Status.Completed).To(BeNil())
			Expect(updated.Status.PrimitiveChildren).To(BeNil())
			Expect(updated.Status.StepStates).To(BeNil())
			Expect(updated.Status.Gates).To(BeNil())
			Expect(updated.Status.Error).To(BeNil())
			Expect(updated.Status.Output).To(BeNil())
			Expect(updated.Status.ChildrenCleanedAt).To(BeNil())
			Expect(updated.Status.Duration).To(BeEmpty())
			Expect(updated.Status.StartedAt).To(BeNil())
			Expect(updated.Status.FinishedAt).To(BeNil())
			Expect(updated.Status.StepsComplete).To(BeZero())
			Expect(updated.Status.StepsFailed).To(BeZero())
			Expect(updated.Status.StepsSkipped).To(BeZero())
			Expect(updated.Status.AllowedFailures).To(BeNil())
			Expect(updated.Status.Conditions).To(BeNil())
			Expect(updated.Status.ObservedGeneration).To(BeZero())
			Expect(updated.Status.Attempts).To(Equal(int32(2)))
			Expect(updated.Status.TriggerTokens).To(ContainElement("story"))
			Expect(updated.Annotations[runsidentity.StoryRunRedriveObservedAnnotation]).To(Equal("token-1"))
		})

		It("should wait for dependents cleanup before redrive", func() {
			By("creating dependent StepRun and child StoryRun")
			stepRun := &runsv1alpha1.StepRun{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-step",
					Namespace: "default",
					Labels:    runsidentity.SelectorLabels(resourceName),
				},
				Spec: runsv1alpha1.StepRunSpec{
					StoryRunRef: refs.StoryRunReference{
						ObjectReference: refs.ObjectReference{Name: resourceName},
					},
					StepID: "step1",
				},
			}
			Expect(k8sClient.Create(ctx, stepRun)).To(Succeed())
			DeferCleanup(func() {
				_ = k8sClient.Delete(ctx, stepRun)
			})

			child := &runsv1alpha1.StoryRun{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "child-storyrun",
					Namespace: "default",
					Labels: map[string]string{
						contracts.ParentStoryRunLabel: resourceName,
					},
				},
				Spec: runsv1alpha1.StoryRunSpec{
					StoryRef: refs.StoryReference{
						ObjectReference: refs.ObjectReference{Name: storyName},
					},
				},
			}
			Expect(k8sClient.Create(ctx, child)).To(Succeed())
			DeferCleanup(func() {
				_ = k8sClient.Delete(ctx, child)
			})

			By("requesting a redrive via annotation")
			current := &runsv1alpha1.StoryRun{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, current)).To(Succeed())
			patch := client.MergeFrom(current.DeepCopy())
			ann := current.GetAnnotations()
			if ann == nil {
				ann = map[string]string{}
			}
			ann[runsidentity.StoryRunRedriveTokenAnnotation] = "token-2"
			current.SetAnnotations(ann)
			Expect(k8sClient.Patch(ctx, current, patch)).To(Succeed())

			controllerReconciler := buildReconciler()
			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			By("waiting for dependents to be deleted")
			Eventually(func() bool {
				err := k8sClient.Get(ctx, types.NamespacedName{Name: stepRun.Name, Namespace: stepRun.Namespace}, &runsv1alpha1.StepRun{})
				return errors.IsNotFound(err)
			}).Should(BeTrue())
			Eventually(func() bool {
				err := k8sClient.Get(ctx, types.NamespacedName{Name: child.Name, Namespace: child.Namespace}, &runsv1alpha1.StoryRun{})
				return errors.IsNotFound(err)
			}).Should(BeTrue())

			By("reconciling again to reset")
			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			updated := &runsv1alpha1.StoryRun{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, updated)).To(Succeed())
			Expect(updated.Annotations[runsidentity.StoryRunRedriveObservedAnnotation]).To(Equal("token-2"))
		})
	})
})
