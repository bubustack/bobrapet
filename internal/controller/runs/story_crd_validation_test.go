package runs

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	bubuv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	steprunwebhook "github.com/bubustack/bobrapet/internal/webhook/runs/v1alpha1"
	storywebhook "github.com/bubustack/bobrapet/internal/webhook/v1alpha1"
	"github.com/bubustack/bobrapet/pkg/enums"
	"github.com/bubustack/bobrapet/pkg/refs"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var _ = Describe("Story CRD validations", func() {
	const namespace = "default"

	It("rejects duplicate step names", func() {
		story := &bubuv1alpha1.Story{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "validation-duplicate-names",
				Namespace: namespace,
			},
			Spec: bubuv1alpha1.StorySpec{
				Steps: []bubuv1alpha1.Step{
					{
						Name: "duplicate",
						Type: enums.StepTypeSetData,
					},
					{
						Name: "duplicate",
						Type: enums.StepTypeSetData,
					},
				},
			},
		}

		validator := &storywebhook.StoryCustomValidator{}
		_, err := validator.ValidateCreate(context.Background(), story.DeepCopyObject())
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("duplicate step name"))
	})

	It("rejects self-referential dependencies", func() {
		story := &bubuv1alpha1.Story{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "validation-self-dependency",
				Namespace: namespace,
			},
			Spec: bubuv1alpha1.StorySpec{
				Steps: []bubuv1alpha1.Step{
					{
						Name:  "self",
						Type:  enums.StepTypeSetData,
						Needs: []string{"self"},
					},
				},
			},
		}

		validator := &storywebhook.StoryCustomValidator{}
		_, err := validator.ValidateCreate(context.Background(), story.DeepCopyObject())
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("cannot depend on itself"))
	})

	It("rejects cycles by requiring dependencies to reference earlier steps", func() {
		story := &bubuv1alpha1.Story{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "validation-cycle",
				Namespace: namespace,
			},
			Spec: bubuv1alpha1.StorySpec{
				Steps: []bubuv1alpha1.Step{
					{
						Name:  "first",
						Type:  enums.StepTypeSetData,
						Needs: []string{"second"},
					},
					{
						Name: "second",
						Type: enums.StepTypeSetData,
					},
				},
			},
		}

		validator := &storywebhook.StoryCustomValidator{}
		_, err := validator.ValidateCreate(context.Background(), story.DeepCopyObject())
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("must be declared before"))
	})
})

var _ = Describe("StepRun CRD validations", func() {
	const namespace = "default"

	It("rejects status.needs entries referencing the StepRun itself", func() {
		stepRun := &runsv1alpha1.StepRun{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "self-referencing-step-run",
				Namespace: namespace,
			},
			Spec: runsv1alpha1.StepRunSpec{
				StoryRunRef: refs.StoryRunReference{
					ObjectReference: refs.ObjectReference{
						Name: "parent-storyrun",
					},
				},
				StepID: "step-id",
			},
		}

		validator := &steprunwebhook.StepRunCustomValidator{}
		_, err := validator.ValidateCreate(context.Background(), stepRun.DeepCopyObject())
		Expect(err).NotTo(HaveOccurred())

		oldObj := stepRun.DeepCopyObject()
		newStepRun := stepRun.DeepCopy()
		newStepRun.Status.Needs = []string{"self-referencing-step-run"}

		_, err = validator.ValidateUpdate(context.Background(), oldObj, newStepRun.DeepCopyObject())
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("cannot reference the StepRun itself"))
	})
})
