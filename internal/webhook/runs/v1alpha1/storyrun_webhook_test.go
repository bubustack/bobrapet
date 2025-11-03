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
)

var _ = Describe("StoryRun Webhook", func() {
	var (
		obj       *runsv1alpha1.StoryRun
		oldObj    *runsv1alpha1.StoryRun
		validator StoryRunCustomValidator
	)

	BeforeEach(func() {
		obj = &runsv1alpha1.StoryRun{}
		oldObj = &runsv1alpha1.StoryRun{}
		validator = StoryRunCustomValidator{}
		Expect(validator).NotTo(BeNil(), "Expected validator to be initialized")
		Expect(oldObj).NotTo(BeNil(), "Expected oldObj to be initialized")
		Expect(obj).NotTo(BeNil(), "Expected obj to be initialized")
		// TODO (user): Add any setup logic common to all tests
	})

	AfterEach(func() {
		// TODO (user): Add any teardown logic common to all tests
	})

	Context("status invariants", func() {
		It("rejects observedGeneration regressions on status updates", func() {
			old := &runsv1alpha1.StoryRun{}
			old.Status.ObservedGeneration = 9
			updated := old.DeepCopy()
			updated.Status.ObservedGeneration = 8
			_, err := validator.ValidateUpdate(context.Background(), old, updated)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("status.observedGeneration"))
		})
	})

})
