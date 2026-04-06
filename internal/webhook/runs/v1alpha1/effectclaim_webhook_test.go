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
	"time"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	"github.com/bubustack/bobrapet/internal/config"
	"github.com/bubustack/bobrapet/pkg/refs"
	runsidentity "github.com/bubustack/bobrapet/pkg/runs/identity"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

var _ = Describe("EffectClaim Webhook", func() {
	const (
		stepRunNamespace = "default"
		stepRunName      = "example-step"
		effectKey        = "deliver-webhook"
	)

	var (
		validator EffectClaimCustomValidator
	)

	ensureStepRun := func() *runsv1alpha1.StepRun {
		stepRun := &runsv1alpha1.StepRun{
			ObjectMeta: metav1.ObjectMeta{
				Name:      stepRunName,
				Namespace: stepRunNamespace,
			},
			Spec: runsv1alpha1.StepRunSpec{
				StoryRunRef: refs.StoryRunReference{
					ObjectReference: refs.ObjectReference{Name: "storyrun-a"},
				},
				StepID: "deliver",
			},
		}
		Expect(k8sClient.Create(ctx, stepRun)).To(Succeed())
		DeferCleanup(func() {
			_ = k8sClient.Delete(ctx, stepRun)
		})
		return stepRun
	}

	claimFor := func(stepRunRef refs.StepRunReference) *runsv1alpha1.EffectClaim {
		return &runsv1alpha1.EffectClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      runsidentity.DeriveEffectClaimName(stepRunNamespace, stepRunRef.Name, effectKey),
				Namespace: stepRunNamespace,
			},
			Spec: runsv1alpha1.EffectClaimSpec{
				StepRunRef:           stepRunRef,
				EffectKey:            effectKey,
				IdempotencyKey:       "storyrun-a/deliver",
				HolderIdentity:       "worker-a",
				LeaseDurationSeconds: 30,
			},
		}
	}

	BeforeEach(func() {
		validator = EffectClaimCustomValidator{
			Client: k8sClient,
			Config: config.DefaultControllerConfig(),
		}
	})

	It("admits creation with a deterministic name", func() {
		stepRun := ensureStepRun()
		obj := claimFor(refs.StepRunReference{
			ObjectReference: refs.ObjectReference{Name: stepRun.Name},
			UID:             &stepRun.UID,
		})

		_, err := validator.ValidateCreate(context.Background(), obj)
		Expect(err).NotTo(HaveOccurred())
	})

	It("rejects a non-deterministic metadata.name", func() {
		stepRun := ensureStepRun()
		obj := claimFor(refs.StepRunReference{
			ObjectReference: refs.ObjectReference{Name: stepRun.Name},
			UID:             &stepRun.UID,
		})
		obj.Name = "wrong-name"

		_, err := validator.ValidateCreate(context.Background(), obj)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("metadata.name must be"))
	})

	It("rejects cross-namespace stepRun references", func() {
		stepRun := ensureStepRun()
		otherNamespace := "other" //nolint:goconst
		obj := claimFor(refs.StepRunReference{
			ObjectReference: refs.ObjectReference{
				Name:      stepRun.Name,
				Namespace: &otherNamespace,
			},
		})
		obj.Name = runsidentity.DeriveEffectClaimName(otherNamespace, stepRun.Name, effectKey)

		_, err := validator.ValidateCreate(context.Background(), obj)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("cross-namespace StepRunRef"))
	})

	It("rejects cross-namespace stepRun references even when policy allows them", func() {
		otherNamespace := "other"
		validator.Config.ReferenceCrossNamespacePolicy = config.ReferenceCrossNamespacePolicyAllow
		obj := claimFor(refs.StepRunReference{
			ObjectReference: refs.ObjectReference{
				Name:      stepRunName,
				Namespace: &otherNamespace,
			},
		})
		obj.Name = runsidentity.DeriveEffectClaimName(otherNamespace, stepRunName, effectKey)

		_, err := validator.ValidateCreate(context.Background(), obj)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("cross-namespace StepRunRef"))
	})

	It("rejects a mismatched StepRun uid", func() {
		stepRun := ensureStepRun()
		wrongUID := types.UID("wrong-uid")
		obj := claimFor(refs.StepRunReference{
			ObjectReference: refs.ObjectReference{Name: stepRun.Name},
			UID:             &wrongUID,
		})

		_, err := validator.ValidateCreate(context.Background(), obj)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("expected"))
	})

	It("rejects completion status without completedAt", func() {
		stepRun := ensureStepRun()
		obj := claimFor(refs.StepRunReference{
			ObjectReference: refs.ObjectReference{Name: stepRun.Name},
			UID:             &stepRun.UID,
		})
		obj.Spec.CompletionStatus = runsv1alpha1.EffectClaimCompletionStatusReleased

		_, err := validator.ValidateCreate(context.Background(), obj)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("spec.completedAt is required when spec.completionStatus is set"))
	})

	It("rejects renewTime before acquireTime", func() {
		stepRun := ensureStepRun()
		obj := claimFor(refs.StepRunReference{
			ObjectReference: refs.ObjectReference{Name: stepRun.Name},
			UID:             &stepRun.UID,
		})
		acquireTime := metav1.NewMicroTime(time.Now().UTC())
		renewTime := metav1.NewMicroTime(acquireTime.Add(-time.Second))
		obj.Spec.AcquireTime = &acquireTime
		obj.Spec.RenewTime = &renewTime

		_, err := validator.ValidateCreate(context.Background(), obj)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("spec.renewTime cannot be before spec.acquireTime"))
	})

	It("rejects completedAt before renewTime", func() {
		stepRun := ensureStepRun()
		obj := claimFor(refs.StepRunReference{
			ObjectReference: refs.ObjectReference{Name: stepRun.Name},
			UID:             &stepRun.UID,
		})
		acquireTime := metav1.NewMicroTime(time.Now().UTC())
		renewTime := metav1.NewMicroTime(acquireTime.Add(time.Second))
		completedAt := metav1.NewTime(acquireTime.Add(500 * time.Millisecond))
		obj.Spec.AcquireTime = &acquireTime
		obj.Spec.RenewTime = &renewTime
		obj.Spec.CompletionStatus = runsv1alpha1.EffectClaimCompletionStatusCompleted
		obj.Spec.CompletedAt = &completedAt

		_, err := validator.ValidateCreate(context.Background(), obj)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("spec.completedAt cannot be before spec.renewTime"))
	})

	It("rejects identity changes after creation", func() {
		stepRun := ensureStepRun()
		oldObj := claimFor(refs.StepRunReference{
			ObjectReference: refs.ObjectReference{Name: stepRun.Name},
			UID:             &stepRun.UID,
		})
		newObj := claimFor(refs.StepRunReference{
			ObjectReference: refs.ObjectReference{Name: stepRun.Name},
			UID:             &stepRun.UID,
		})
		newObj.Spec.IdempotencyKey = "different-idempotency-key"

		_, err := validator.ValidateUpdate(context.Background(), oldObj, newObj)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("immutable"))
	})

	It("rejects holder reassignment after completion", func() {
		stepRun := ensureStepRun()
		oldObj := claimFor(refs.StepRunReference{
			ObjectReference: refs.ObjectReference{Name: stepRun.Name},
			UID:             &stepRun.UID,
		})
		completedAt := metav1.NewTime(time.Now().UTC())
		oldObj.Spec.CompletionStatus = runsv1alpha1.EffectClaimCompletionStatusCompleted
		oldObj.Spec.CompletedAt = &completedAt

		newObj := oldObj.DeepCopy()
		newObj.Spec.HolderIdentity = "worker-b"

		_, err := validator.ValidateUpdate(context.Background(), oldObj, newObj)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("completed EffectClaims cannot reassign holder"))
	})
})
