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

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	bubuv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/internal/config"
	"github.com/bubustack/bobrapet/pkg/enums"
	"github.com/bubustack/bobrapet/pkg/refs"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

var _ = Describe("StepRun Webhook", func() {
	var (
		obj       *runsv1alpha1.StepRun
		oldObj    *runsv1alpha1.StepRun
		validator StepRunCustomValidator
		defaulter StepRunCustomDefaulter
	)

	BeforeEach(func() {
		obj = &runsv1alpha1.StepRun{}
		oldObj = &runsv1alpha1.StepRun{}
		cfg := config.DefaultControllerConfig()
		validator = StepRunCustomValidator{Config: cfg}
		defaulter = StepRunCustomDefaulter{Config: cfg}
		Expect(validator).NotTo(BeNil(), "Expected validator to be initialized")
		Expect(oldObj).NotTo(BeNil(), "Expected oldObj to be initialized")
		Expect(obj).NotTo(BeNil(), "Expected obj to be initialized")
	})

	newStepRun := func(withTargets []runsv1alpha1.DownstreamTarget) *runsv1alpha1.StepRun {
		return &runsv1alpha1.StepRun{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "default",
			},
			Spec: runsv1alpha1.StepRunSpec{
				StoryRunRef: refs.StoryRunReference{
					ObjectReference: refs.ObjectReference{
						Name: "storyrun-1",
					},
				},
				StepID:            "step-a",
				DownstreamTargets: withTargets,
			},
		}
	}

	Context("When creating or updating StepRun under Validating Webhook", func() {
		It("allows a single grpc downstream target", func() {
			sr := newStepRun([]runsv1alpha1.DownstreamTarget{
				{GRPCTarget: &runsv1alpha1.GRPCTarget{Endpoint: "example:5000"}},
			})
			Expect(validator.validateStepRun(context.Background(), sr)).To(Succeed())
		})

		It("allows a single terminate downstream target", func() {
			sr := newStepRun([]runsv1alpha1.DownstreamTarget{
				{Terminate: &runsv1alpha1.TerminateTarget{StopMode: enums.StopModeSuccess}},
			})
			Expect(validator.validateStepRun(context.Background(), sr)).To(Succeed())
		})

		It("rejects a downstream target that sets both grpc and terminate", func() {
			sr := newStepRun([]runsv1alpha1.DownstreamTarget{
				{
					GRPCTarget: &runsv1alpha1.GRPCTarget{Endpoint: "example:5000"},
					Terminate:  &runsv1alpha1.TerminateTarget{StopMode: enums.StopModeSuccess},
				},
			})
			Expect(validator.validateStepRun(context.Background(), sr)).To(MatchError(ContainSubstring("spec.downstreamTargets[0] must set exactly one of grpc or terminate")))
		})

		It("rejects a downstream target that sets neither grpc nor terminate", func() {
			sr := newStepRun([]runsv1alpha1.DownstreamTarget{{}})
			Expect(validator.validateStepRun(context.Background(), sr)).To(MatchError(ContainSubstring("spec.downstreamTargets[0] must set exactly one of grpc or terminate")))
		})

		It("ValidateCreate surfaces downstream target errors", func() {
			obj = newStepRun([]runsv1alpha1.DownstreamTarget{{}})
			_, err := validator.ValidateCreate(context.Background(), obj)
			Expect(err).To(MatchError(ContainSubstring("spec.downstreamTargets[0] must set exactly one of grpc or terminate")))
		})

		It("ValidateUpdate surfaces downstream target errors when spec changes", func() {
			obj = newStepRun([]runsv1alpha1.DownstreamTarget{{}})
			oldObj = newStepRun(nil)
			_, err := validator.ValidateUpdate(context.Background(), oldObj, obj)
			Expect(err).To(MatchError(ContainSubstring("spec.downstreamTargets[0] must set exactly one of grpc or terminate")))
		})

		It("rejects observedGeneration regressions on status updates", func() {
			oldObj = newStepRun(nil)
			oldObj.Status.ObservedGeneration = 5
			obj = oldObj.DeepCopy()
			obj.Status.ObservedGeneration = 4
			_, err := validator.ValidateUpdate(context.Background(), oldObj, obj)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("status.observedGeneration"))
		})

		It("rejects cross-namespace storyRunRef", func() {
			sr := newStepRun(nil)
			other := "other" //nolint:goconst
			sr.Spec.StoryRunRef.Namespace = &other
			err := validator.validateStepRun(context.Background(), sr)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("cross-namespace StoryRunRef"))
		})

		It("rejects cross-namespace engramRef", func() {
			sr := newStepRun(nil)
			other := "other"
			sr.Spec.EngramRef = &refs.EngramReference{
				ObjectReference: refs.ObjectReference{
					Name:      "engram-a",
					Namespace: &other,
				},
			}
			err := validator.validateStepRun(context.Background(), sr)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("cross-namespace EngramRef"))
		})

		It("skips validation when DeletionTimestamp is set", func() {
			obj = newStepRun([]runsv1alpha1.DownstreamTarget{{}}) // invalid target that would normally fail
			now := metav1.Now()
			obj.DeletionTimestamp = &now
			oldObj = newStepRun(nil)
			_, err := validator.ValidateUpdate(context.Background(), oldObj, obj)
			Expect(err).NotTo(HaveOccurred())
		})

		It("skips spec validation when spec is unchanged (status-only update)", func() {
			obj = newStepRun(nil)
			obj.Status.ObservedGeneration = 1
			oldObj = obj.DeepCopy()
			oldObj.Status.ObservedGeneration = 0
			_, err := validator.ValidateUpdate(context.Background(), oldObj, obj)
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Context("ValidateDelete", func() {
		It("always allows deletion", func() {
			_, err := validator.ValidateDelete(context.Background(), newStepRun(nil))
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Context("Defaulting StepRun retry policy", func() {
		It("sets defaults when retry is nil", func() {
			sr := newStepRun(nil)
			sr.Spec.Retry = nil
			Expect(defaulter.Default(context.Background(), sr)).To(Succeed())
			Expect(sr.Spec.Retry).NotTo(BeNil())
			Expect(sr.Spec.Retry.MaxRetries).NotTo(BeNil())
			Expect(sr.Spec.Retry.Delay).NotTo(BeNil())
			Expect(sr.Spec.Retry.Backoff).NotTo(BeNil())
		})

		It("fills missing retry fields", func() {
			sr := newStepRun(nil)
			empty := ""
			sr.Spec.Retry = &bubuv1alpha1.RetryPolicy{
				MaxRetries: nil,
				Delay:      &empty,
			}
			Expect(defaulter.Default(context.Background(), sr)).To(Succeed())
			Expect(sr.Spec.Retry.MaxRetries).NotTo(BeNil())
			Expect(sr.Spec.Retry.Delay).NotTo(BeNil())
			Expect(*sr.Spec.Retry.Delay).NotTo(BeEmpty())
		})

		It("sets idempotency key when not provided", func() {
			sr := newStepRun(nil)
			sr.Spec.IdempotencyKey = ""
			Expect(defaulter.Default(context.Background(), sr)).To(Succeed())
			Expect(sr.Spec.IdempotencyKey).NotTo(BeEmpty())
		})

		It("preserves existing idempotency key", func() {
			sr := newStepRun(nil)
			sr.Spec.IdempotencyKey = "my-custom-key"
			Expect(defaulter.Default(context.Background(), sr)).To(Succeed())
			Expect(sr.Spec.IdempotencyKey).To(Equal("my-custom-key"))
		})
	})

	Context("Structured error status validation", func() {
		It("accepts valid structured errors", func() {
			sr := newStepRun(nil)
			retryable := true
			sr.Status.Error = &runsv1alpha1.StructuredError{
				Version:   runsv1alpha1.StructuredErrorVersionV1,
				Type:      runsv1alpha1.StructuredErrorTypeTimeout,
				Message:   "boom",
				Retryable: &retryable,
			}
			_, err := validator.ValidateCreate(context.Background(), sr)
			Expect(err).NotTo(HaveOccurred())
		})

		It("rejects invalid structured errors", func() {
			sr := newStepRun(nil)
			sr.Status.Error = &runsv1alpha1.StructuredError{
				Type:    runsv1alpha1.StructuredErrorTypeTimeout,
				Message: "missing version",
			}
			_, err := validator.ValidateCreate(context.Background(), sr)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("status.error"))
		})

		It("rejects unsupported structured error exit classes", func() {
			sr := newStepRun(nil)
			sr.Status.Error = &runsv1alpha1.StructuredError{
				Version:   runsv1alpha1.StructuredErrorVersionV1,
				Type:      runsv1alpha1.StructuredErrorTypeExecution,
				Message:   "bad exit class",
				ExitClass: runsv1alpha1.StructuredErrorExitClass("bogus"),
			}
			_, err := validator.ValidateCreate(context.Background(), sr)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("status.error"))
		})
	})

	Context("version pinning", func() {
		It("rejects engram references with mismatched version", func() {
			engram := &bubuv1alpha1.Engram{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "engram-versioned",
					Namespace: "default",
				},
				Spec: bubuv1alpha1.EngramSpec{
					TemplateRef: refs.EngramTemplateReference{Name: "tpl"},
					Version:     "v1",
				},
			}
			validator.Client = fake.NewClientBuilder().WithScheme(scheme.Scheme).WithObjects(engram).Build()

			sr := newStepRun(nil)
			sr.Spec.EngramRef = &refs.EngramReference{
				ObjectReference: refs.ObjectReference{Name: "engram-versioned"},
				Version:         "v2",
			}
			err := validator.validateStepRun(context.Background(), sr)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("version"))
		})

		It("allows engram references with matching version", func() {
			engram := &bubuv1alpha1.Engram{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "engram-versioned-ok",
					Namespace: "default",
				},
				Spec: bubuv1alpha1.EngramSpec{
					TemplateRef: refs.EngramTemplateReference{Name: "tpl"},
					Version:     "v1",
				},
			}
			validator.Client = fake.NewClientBuilder().WithScheme(scheme.Scheme).WithObjects(engram).Build()

			sr := newStepRun(nil)
			sr.Spec.EngramRef = &refs.EngramReference{
				ObjectReference: refs.ObjectReference{Name: "engram-versioned-ok"},
				Version:         "v1",
			}
			Expect(validator.validateStepRun(context.Background(), sr)).To(Succeed())
		})
	})

})
