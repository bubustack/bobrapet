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
	"sync"

	bubuv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/pkg/conditions"
	"github.com/bubustack/bobrapet/pkg/enums"
	"github.com/bubustack/bobrapet/pkg/refs"
	runsidentity "github.com/bubustack/bobrapet/pkg/runs/identity"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
)

var _ = Describe("StoryTrigger Controller", func() {
	const (
		namespace = "default"
		storyName = "test-story"
	)

	ctx := context.Background()

	buildReconciler := func() *StoryTriggerReconciler {
		return &StoryTriggerReconciler{
			Client:    k8sClient,
			APIReader: k8sClient,
			Scheme:    k8sClient.Scheme(),
		}
	}

	cleanupObject := func(obj client.Object) {
		existing := obj.DeepCopyObject().(client.Object)
		err := k8sClient.Get(ctx, client.ObjectKeyFromObject(obj), existing)
		if client.IgnoreNotFound(err) != nil {
			Expect(err).NotTo(HaveOccurred())
		}
		if err == nil {
			Expect(k8sClient.Delete(ctx, existing)).To(Succeed())
		}
	}

	ensureStory := func(name, version string) {
		story := &bubuv1alpha1.Story{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: namespace,
			},
			Spec: bubuv1alpha1.StorySpec{
				Version: version,
				Steps: []bubuv1alpha1.Step{{
					Name: "prepare",
					Type: enums.StepTypeCondition,
				}},
			},
		}
		Expect(k8sClient.Create(ctx, story)).To(Succeed())
		DeferCleanup(func() {
			cleanupObject(story)
		})
	}

	triggerHash := func(raw []byte) string {
		hash, err := runsidentity.ComputeTriggerInputHash(raw)
		Expect(err).NotTo(HaveOccurred())
		return hash
	}

	newTrigger := func(submissionID, key string, rawInputs []byte, mode bubuv1alpha1.TriggerDedupeMode) *runsv1alpha1.StoryTrigger {
		if rawInputs == nil {
			rawInputs = []byte(`{}`)
		}
		name := runsidentity.DeriveStoryTriggerName(namespace, storyName, key, submissionID)
		trigger := &runsv1alpha1.StoryTrigger{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: namespace,
			},
			Spec: runsv1alpha1.StoryTriggerSpec{
				StoryRef: refs.StoryReference{
					ObjectReference: refs.ObjectReference{
						Name: storyName,
					},
					Version: "v1",
				},
				Inputs: &runtime.RawExtension{Raw: rawInputs},
				DeliveryIdentity: runsv1alpha1.TriggerDeliveryIdentity{
					Mode:         &mode,
					SubmissionID: submissionID,
				},
			},
		}
		if key != "" {
			trigger.Spec.DeliveryIdentity.Key = key
			trigger.Spec.DeliveryIdentity.InputHash = triggerHash(rawInputs)
		}
		return trigger
	}

	createExistingStoryRun := func(trigger *runsv1alpha1.StoryTrigger, rawInputs []byte) *runsv1alpha1.StoryRun {
		inputHash := ""
		if len(rawInputs) == 0 {
			rawInputs = []byte(`{}`)
		}
		if trigger.Spec.DeliveryIdentity.Key != "" {
			inputHash = triggerHash(rawInputs)
		}
		run := desiredStoryRunForTrigger(trigger, inputHash)
		run.Spec.Inputs = &runtime.RawExtension{Raw: rawInputs}
		Expect(k8sClient.Create(ctx, run)).To(Succeed())
		DeferCleanup(func() {
			cleanupObject(run)
		})
		return run
	}

	It("creates a StoryRun and marks the trigger Created", func() {
		ensureStory(storyName, "v1")

		trigger := newTrigger("submission-created", "customer-42", []byte(`{"payload":"ok"}`), bubuv1alpha1.TriggerDedupeKey)
		Expect(k8sClient.Create(ctx, trigger)).To(Succeed())
		DeferCleanup(func() {
			cleanupObject(trigger)
		})

		reconciler := buildReconciler()
		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(trigger)})
		Expect(err).NotTo(HaveOccurred())

		updatedTrigger := &runsv1alpha1.StoryTrigger{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(trigger), updatedTrigger)).To(Succeed())
		Expect(updatedTrigger.Status.Decision).To(Equal(runsv1alpha1.StoryTriggerDecisionCreated))
		Expect(updatedTrigger.Status.Reason).To(Equal(storyTriggerReasonStoryRunCreated))
		Expect(updatedTrigger.Status.StoryRunRef).NotTo(BeNil())
		Expect(updatedTrigger.Status.AcceptedAt).NotTo(BeNil())
		Expect(updatedTrigger.Status.CompletedAt).NotTo(BeNil())
		Expect(updatedTrigger.Status.ObservedGeneration).To(Equal(updatedTrigger.Generation))
		Expect(conditions.IsReady(updatedTrigger.Status.Conditions)).To(BeTrue())

		createdRun := &runsv1alpha1.StoryRun{}
		runKey := types.NamespacedName{
			Name:      updatedTrigger.Status.StoryRunRef.Name,
			Namespace: refs.ResolveNamespace(updatedTrigger, &updatedTrigger.Status.StoryRunRef.ObjectReference),
		}
		Expect(k8sClient.Get(ctx, runKey, createdRun)).To(Succeed())
		Expect(createdRun.Spec.StoryRef.Name).To(Equal(storyName))
		Expect(createdRun.Annotations).To(HaveKeyWithValue(runsidentity.StoryRunTriggerRequestNameAnnotation, trigger.Name))
		Expect(createdRun.Annotations).To(HaveKeyWithValue(runsidentity.StoryRunTriggerRequestUIDAnnotation, string(trigger.GetUID())))
		Expect(createdRun.Annotations).To(HaveKeyWithValue(runsidentity.StoryRunTriggerTokenAnnotation, "customer-42"))
		Expect(createdRun.Annotations).To(HaveKeyWithValue(runsidentity.StoryRunTriggerInputHashAnnotation, trigger.Spec.DeliveryIdentity.InputHash))
		Expect(string(createdRun.Spec.Inputs.Raw)).To(Equal(`{"payload":"ok"}`))
	})

	It("reuses an existing matching StoryRun", func() {
		ensureStory(storyName, "v1")

		trigger := newTrigger("submission-reused", "customer-99", []byte(`{"payload":"ok"}`), bubuv1alpha1.TriggerDedupeKey)
		existingRun := createExistingStoryRun(trigger, []byte(`{"payload":"ok"}`))
		Expect(k8sClient.Create(ctx, trigger)).To(Succeed())
		DeferCleanup(func() {
			cleanupObject(trigger)
		})

		reconciler := buildReconciler()
		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(trigger)})
		Expect(err).NotTo(HaveOccurred())

		updatedTrigger := &runsv1alpha1.StoryTrigger{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(trigger), updatedTrigger)).To(Succeed())
		Expect(updatedTrigger.Status.Decision).To(Equal(runsv1alpha1.StoryTriggerDecisionReused))
		Expect(updatedTrigger.Status.Reason).To(Equal(storyTriggerReasonStoryRunReused))
		Expect(updatedTrigger.Status.StoryRunRef).NotTo(BeNil())
		Expect(updatedTrigger.Status.StoryRunRef.Name).To(Equal(existingRun.Name))
		Expect(conditions.IsReady(updatedTrigger.Status.Conditions)).To(BeTrue())
	})

	It("keeps the decision Created when the StoryRun already exists for the same trigger", func() {
		ensureStory(storyName, "v1")

		trigger := newTrigger("submission-retry", "customer-retry", []byte(`{"payload":"ok"}`), bubuv1alpha1.TriggerDedupeKey)
		Expect(k8sClient.Create(ctx, trigger)).To(Succeed())
		DeferCleanup(func() {
			cleanupObject(trigger)
		})

		// Simulate a previous reconcile that created the StoryRun but failed before patching trigger status.
		existingRun := desiredStoryRunForTrigger(trigger, trigger.Spec.DeliveryIdentity.InputHash)
		Expect(k8sClient.Create(ctx, existingRun)).To(Succeed())
		DeferCleanup(func() {
			cleanupObject(existingRun)
		})

		reconciler := buildReconciler()
		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(trigger)})
		Expect(err).NotTo(HaveOccurred())

		updatedTrigger := &runsv1alpha1.StoryTrigger{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(trigger), updatedTrigger)).To(Succeed())
		Expect(updatedTrigger.Status.Decision).To(Equal(runsv1alpha1.StoryTriggerDecisionCreated))
		Expect(updatedTrigger.Status.Reason).To(Equal(storyTriggerReasonStoryRunCreated))
		Expect(updatedTrigger.Status.StoryRunRef).NotTo(BeNil())
		Expect(updatedTrigger.Status.StoryRunRef.Name).To(Equal(existingRun.Name))
	})

	It("re-evaluates stale terminal decisions when observed generation lags", func() {
		ensureStory(storyName, "v1")

		trigger := newTrigger("submission-stale", "customer-stale", []byte(`{"payload":"ok"}`), bubuv1alpha1.TriggerDedupeKey)
		Expect(k8sClient.Create(ctx, trigger)).To(Succeed())
		DeferCleanup(func() {
			cleanupObject(trigger)
		})

		stale := &runsv1alpha1.StoryTrigger{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(trigger), stale)).To(Succeed())
		stale.Status.Decision = runsv1alpha1.StoryTriggerDecisionRejected
		stale.Status.ObservedGeneration = 0
		stale.Status.Reason = "StaleDecision"
		stale.Status.Message = "old status must not suppress current reconcile"
		Expect(k8sClient.Status().Update(ctx, stale)).To(Succeed())

		reconciler := buildReconciler()
		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(trigger)})
		Expect(err).NotTo(HaveOccurred())

		updatedTrigger := &runsv1alpha1.StoryTrigger{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(trigger), updatedTrigger)).To(Succeed())
		Expect(updatedTrigger.Status.Decision).To(Equal(runsv1alpha1.StoryTriggerDecisionCreated))
		Expect(updatedTrigger.Status.Reason).To(Equal(storyTriggerReasonStoryRunCreated))
		Expect(updatedTrigger.Status.ObservedGeneration).To(Equal(updatedTrigger.Generation))
	})

	It("rejects the trigger when an existing StoryRun conflicts", func() {
		ensureStory(storyName, "v1")

		trigger := newTrigger("submission-conflict", "customer-conflict", []byte(`{"payload":"expected"}`), bubuv1alpha1.TriggerDedupeKey)
		createExistingStoryRun(trigger, []byte(`{"payload":"different"}`))
		Expect(k8sClient.Create(ctx, trigger)).To(Succeed())
		DeferCleanup(func() {
			cleanupObject(trigger)
		})

		reconciler := buildReconciler()
		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(trigger)})
		Expect(err).NotTo(HaveOccurred())

		updatedTrigger := &runsv1alpha1.StoryTrigger{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(trigger), updatedTrigger)).To(Succeed())
		Expect(updatedTrigger.Status.Decision).To(Equal(runsv1alpha1.StoryTriggerDecisionRejected))
		Expect(updatedTrigger.Status.Reason).To(Equal(storyTriggerReasonStoryRunConflict))
		Expect(updatedTrigger.Status.StoryRunRef).To(BeNil())
		Expect(conditions.IsDegraded(updatedTrigger.Status.Conditions)).To(BeTrue())
	})

	It("rejects persisted cross-namespace story references when the controller policy denies them", func() {
		trigger := newTrigger("submission-cross-ns", "customer-cross-ns", []byte(`{"payload":"ok"}`), bubuv1alpha1.TriggerDedupeKey)
		otherNamespace := "other"
		trigger.Spec.StoryRef.Namespace = &otherNamespace
		trigger.Name = runsidentity.DeriveStoryTriggerName(otherNamespace, storyName, "customer-cross-ns", "submission-cross-ns")
		Expect(k8sClient.Create(ctx, trigger)).To(Succeed())
		DeferCleanup(func() {
			cleanupObject(trigger)
		})

		reconciler := buildReconciler()
		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(trigger)})
		Expect(err).NotTo(HaveOccurred())

		updatedTrigger := &runsv1alpha1.StoryTrigger{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(trigger), updatedTrigger)).To(Succeed())
		Expect(updatedTrigger.Status.Decision).To(Equal(runsv1alpha1.StoryTriggerDecisionRejected))
		Expect(updatedTrigger.Status.Reason).To(Equal(conditions.ReasonValidationFailed))
		Expect(updatedTrigger.Status.Message).To(ContainSubstring("cross-namespace StoryRef references are not allowed"))
	})

	It("does not reuse a StoryRun when impulse provenance differs", func() {
		ensureStory(storyName, "v1")

		trigger := newTrigger("submission-impulse", "customer-impulse", []byte(`{"payload":"ok"}`), bubuv1alpha1.TriggerDedupeKey)
		trigger.Spec.ImpulseRef = &refs.ImpulseReference{
			ObjectReference: refs.ObjectReference{Name: "impulse-a"},
		}

		existingRun := desiredStoryRunForTrigger(trigger, trigger.Spec.DeliveryIdentity.InputHash)
		existingRun.Spec.ImpulseRef = &refs.ImpulseReference{
			ObjectReference: refs.ObjectReference{Name: "impulse-b"},
		}
		Expect(k8sClient.Create(ctx, existingRun)).To(Succeed())
		DeferCleanup(func() {
			cleanupObject(existingRun)
		})

		Expect(k8sClient.Create(ctx, trigger)).To(Succeed())
		DeferCleanup(func() {
			cleanupObject(trigger)
		})

		reconciler := buildReconciler()
		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(trigger)})
		Expect(err).NotTo(HaveOccurred())

		updatedTrigger := &runsv1alpha1.StoryTrigger{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(trigger), updatedTrigger)).To(Succeed())
		Expect(updatedTrigger.Status.Decision).To(Equal(runsv1alpha1.StoryTriggerDecisionRejected))
		Expect(updatedTrigger.Status.Reason).To(Equal(storyTriggerReasonStoryRunConflict))
		Expect(updatedTrigger.Status.Message).To(ContainSubstring("does not match StoryTrigger storyRef and inputs"))
	})

	It("collapses concurrent duplicate request creation to one StoryTrigger and one StoryRun", func() {
		ensureStory(storyName, "v1")

		triggerA := newTrigger("submission-duplicate", "customer-duplicate", []byte(`{"payload":"ok"}`), bubuv1alpha1.TriggerDedupeKey)
		triggerB := triggerA.DeepCopy()
		errs := make(chan error, 2)

		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer GinkgoRecover()
			defer wg.Done()
			errs <- k8sClient.Create(ctx, triggerA)
		}()
		go func() {
			defer GinkgoRecover()
			defer wg.Done()
			errs <- k8sClient.Create(ctx, triggerB)
		}()
		wg.Wait()
		close(errs)

		var nilCount, alreadyExistsCount int
		for err := range errs {
			switch {
			case err == nil:
				nilCount++
			case apierrors.IsAlreadyExists(err):
				alreadyExistsCount++
			default:
				Expect(err).NotTo(HaveOccurred())
			}
		}
		Expect(nilCount).To(Equal(1))
		Expect(alreadyExistsCount).To(Equal(1))

		triggerKey := client.ObjectKeyFromObject(triggerA)
		DeferCleanup(func() {
			cleanupObject(triggerA)
		})

		reconciler := buildReconciler()
		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: triggerKey})
		Expect(err).NotTo(HaveOccurred())

		trigger := &runsv1alpha1.StoryTrigger{}
		Expect(k8sClient.Get(ctx, triggerKey, trigger)).To(Succeed())
		Expect(trigger.Status.Decision).To(Equal(runsv1alpha1.StoryTriggerDecisionCreated))

		runList := &runsv1alpha1.StoryRunList{}
		Expect(k8sClient.List(ctx, runList, client.InNamespace(namespace))).To(Succeed())

		expectedRunName := runsidentity.DeriveStoryRunName(namespace, storyName, "customer-duplicate")
		var matches int
		for i := range runList.Items {
			if runList.Items[i].Name == expectedRunName {
				matches++
			}
		}
		Expect(matches).To(Equal(1))
	})
})
