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
			Expect(validator.validateStepRun(sr)).To(Succeed())
		})

		It("allows a single terminate downstream target", func() {
			sr := newStepRun([]runsv1alpha1.DownstreamTarget{
				{Terminate: &runsv1alpha1.TerminateTarget{StopMode: enums.StopModeSuccess}},
			})
			Expect(validator.validateStepRun(sr)).To(Succeed())
		})

		It("rejects a downstream target that sets both grpc and terminate", func() {
			sr := newStepRun([]runsv1alpha1.DownstreamTarget{
				{
					GRPCTarget: &runsv1alpha1.GRPCTarget{Endpoint: "example:5000"},
					Terminate:  &runsv1alpha1.TerminateTarget{StopMode: enums.StopModeSuccess},
				},
			})
			Expect(validator.validateStepRun(sr)).To(MatchError(ContainSubstring("spec.downstreamTargets[0] must set exactly one of grpc or terminate")))
		})

		It("rejects a downstream target that sets neither grpc nor terminate", func() {
			sr := newStepRun([]runsv1alpha1.DownstreamTarget{{}})
			Expect(validator.validateStepRun(sr)).To(MatchError(ContainSubstring("spec.downstreamTargets[0] must set exactly one of grpc or terminate")))
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
	})

})
