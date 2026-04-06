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

	"github.com/bubustack/bobrapet/pkg/conditions"
	"github.com/bubustack/bobrapet/pkg/refs"
	runsidentity "github.com/bubustack/bobrapet/pkg/runs/identity"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
)

var _ = Describe("EffectClaim Controller", func() {
	const (
		namespace   = "default"
		stepRunName = "step-effect"
		effectKey   = "notify-external"
	)

	ctx := context.Background()

	buildReconciler := func() *EffectClaimReconciler {
		return &EffectClaimReconciler{
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

	newStepRun := func(name string) *runsv1alpha1.StepRun {
		return &runsv1alpha1.StepRun{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: namespace,
			},
			Spec: runsv1alpha1.StepRunSpec{
				StoryRunRef: refs.StoryRunReference{
					ObjectReference: refs.ObjectReference{Name: "storyrun-" + name},
				},
				StepID: "deliver",
			},
		}
	}

	claimFor := func(stepRun *runsv1alpha1.StepRun) *runsv1alpha1.EffectClaim {
		return &runsv1alpha1.EffectClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      runsidentity.DeriveEffectClaimName(namespace, stepRun.Name, effectKey),
				Namespace: namespace,
			},
			Spec: runsv1alpha1.EffectClaimSpec{
				StepRunRef: refs.StepRunReference{
					ObjectReference: refs.ObjectReference{Name: stepRun.Name},
					UID:             &stepRun.UID,
				},
				EffectKey:            effectKey,
				HolderIdentity:       "worker-a",
				LeaseDurationSeconds: 60,
			},
		}
	}

	It("stamps owner reference and maps completed claims to Completed phase", func() {
		stepRun := newStepRun(stepRunName)
		Expect(k8sClient.Create(ctx, stepRun)).To(Succeed())
		DeferCleanup(func() { cleanupObject(stepRun) })

		claim := claimFor(stepRun)
		now := metav1.Now()
		claim.Spec.CompletionStatus = runsv1alpha1.EffectClaimCompletionStatusCompleted
		claim.Spec.CompletedAt = &now
		Expect(k8sClient.Create(ctx, claim)).To(Succeed())
		DeferCleanup(func() { cleanupObject(claim) })

		reconciler := buildReconciler()
		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(claim)})
		Expect(err).NotTo(HaveOccurred())

		updated := &runsv1alpha1.EffectClaim{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(claim), updated)).To(Succeed())
		Expect(updated.Status.Phase).To(Equal(runsv1alpha1.EffectClaimPhaseCompleted))
		Expect(updated.Status.ObservedGeneration).To(Equal(updated.Generation))
		Expect(conditions.IsReady(updated.Status.Conditions)).To(BeTrue())
		Expect(hasStepRunOwnerReference(updated.OwnerReferences, stepRun)).To(BeTrue())
	})

	It("maps released claims to Released phase without degrading them", func() {
		stepRun := newStepRun("step-effect-released")
		Expect(k8sClient.Create(ctx, stepRun)).To(Succeed())
		DeferCleanup(func() { cleanupObject(stepRun) })

		claim := claimFor(stepRun)
		claim.Spec.CompletionStatus = runsv1alpha1.EffectClaimCompletionStatusReleased
		Expect(k8sClient.Create(ctx, claim)).To(Succeed())
		DeferCleanup(func() { cleanupObject(claim) })

		reconciler := buildReconciler()
		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(claim)})
		Expect(err).NotTo(HaveOccurred())

		updated := &runsv1alpha1.EffectClaim{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(claim), updated)).To(Succeed())
		Expect(updated.Status.Phase).To(Equal(runsv1alpha1.EffectClaimPhaseReleased))
		Expect(conditions.IsReady(updated.Status.Conditions)).To(BeFalse())
		Expect(conditions.IsDegraded(updated.Status.Conditions)).To(BeFalse())
	})

	It("deletes orphan claims when the referenced StepRun is missing", func() {
		stepRun := newStepRun("step-effect-missing")
		claim := claimFor(stepRun)
		Expect(k8sClient.Create(ctx, claim)).To(Succeed())

		reconciler := buildReconciler()
		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(claim)})
		Expect(err).NotTo(HaveOccurred())

		remaining := &runsv1alpha1.EffectClaim{}
		err = k8sClient.Get(ctx, client.ObjectKeyFromObject(claim), remaining)
		Expect(apierrors.IsNotFound(err)).To(BeTrue(), "expected orphan EffectClaim to be deleted")
	})

	It("deletes drifted claims when the referenced StepRun UID no longer matches", func() {
		stepRun := newStepRun("step-effect-drift")
		Expect(k8sClient.Create(ctx, stepRun)).To(Succeed())
		DeferCleanup(func() { cleanupObject(stepRun) })

		claim := claimFor(stepRun)
		drifted := types.UID("drifted-step-run-uid")
		claim.Spec.StepRunRef.UID = &drifted
		Expect(k8sClient.Create(ctx, claim)).To(Succeed())

		reconciler := buildReconciler()
		_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(claim)})
		Expect(err).NotTo(HaveOccurred())

		remaining := &runsv1alpha1.EffectClaim{}
		err = k8sClient.Get(ctx, client.ObjectKeyFromObject(claim), remaining)
		Expect(apierrors.IsNotFound(err)).To(BeTrue(), "expected drifted EffectClaim to be deleted")
	})
})

func hasStepRunOwnerReference(ownerRefs []metav1.OwnerReference, stepRun *runsv1alpha1.StepRun) bool {
	if stepRun == nil {
		return false
	}
	for _, ref := range ownerRefs {
		if ref.APIVersion == runsv1alpha1.GroupVersion.String() &&
			ref.Kind == "StepRun" && //nolint:goconst
			ref.Name == stepRun.Name &&
			ref.UID == stepRun.UID {
			return true
		}
	}
	return false
}
