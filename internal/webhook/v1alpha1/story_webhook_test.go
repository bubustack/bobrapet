package v1alpha1

import (
	"encoding/json"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	bubushv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/internal/config"
	"github.com/bubustack/bobrapet/pkg/enums"
)

var _ = Describe("Story Webhook", func() {
	var (
		validator StoryCustomValidator
		defaulter StoryCustomDefaulter
	)

	BeforeEach(func() {
		cfg := config.DefaultControllerConfig()
		validator = StoryCustomValidator{Client: k8sClient, Config: cfg}
		defaulter = StoryCustomDefaulter{Config: cfg}
		Expect(defaulter).NotTo(BeNil())
	})

	Context("defaulting", func() {
		It("applies retry defaults to story policy and steps", func() {
			story := minimalStory("defaults")
			story.Spec.Policy = &bubushv1alpha1.StoryPolicy{}
			story.Spec.Steps[0].Execution = &bubushv1alpha1.ExecutionOverrides{}

			Expect(defaulter.Default(ctx, story)).To(Succeed())
			Expect(story.Spec.Policy.Retries).NotTo(BeNil())
			Expect(story.Spec.Policy.Retries.StepRetryPolicy).NotTo(BeNil())
			Expect(story.Spec.Steps[0].Execution.Retry).NotTo(BeNil())
		})
	})

	Context("executeStory validation", func() {
		It("rejects executeStory steps that reference missing stories", func() {
			story := minimalStory("parent")
			story.Spec.Steps = append(story.Spec.Steps, bubushv1alpha1.Step{
				Name: "invoke-child",
				Type: enums.StepTypeExecuteStory,
				With: mustRawExtension(map[string]any{
					"storyRef": map[string]any{"name": "missing"},
				}),
			})

			_, err := validator.ValidateCreate(ctx, story)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("does not exist"))
		})

		It("rejects executeStory steps that reference the same story", func() {
			story := minimalStory("loop")
			story.Spec.Steps = append(story.Spec.Steps, bubushv1alpha1.Step{
				Name: "self",
				Type: enums.StepTypeExecuteStory,
				With: mustRawExtension(map[string]any{
					"storyRef": map[string]any{"name": "loop"},
				}),
			})

			_, err := validator.ValidateCreate(ctx, story)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("cannot reference the same story"))
		})

		It("allows executeStory steps that reference existing stories", func() {
			child := minimalStory("child")
			Expect(k8sClient.Create(ctx, child)).To(Succeed())
			DeferCleanup(func() {
				_ = k8sClient.Delete(ctx, child)
			})

			story := minimalStory("parent")
			story.Spec.Steps = append(story.Spec.Steps, bubushv1alpha1.Step{
				Name: "invoke-child",
				Type: enums.StepTypeExecuteStory,
				With: mustRawExtension(map[string]any{
					"storyRef": map[string]any{"name": "child"},
				}),
			})

			warnings, err := validator.ValidateCreate(ctx, story)
			Expect(err).NotTo(HaveOccurred())
			Expect(warnings).To(BeNil())
		})
	})
})

func minimalStory(name string) *bubushv1alpha1.Story {
	return &bubushv1alpha1.Story{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      name,
		},
		Spec: bubushv1alpha1.StorySpec{
			Steps: []bubushv1alpha1.Step{{
				Name: "prepare",
				Type: enums.StepTypeSetData,
			}},
		},
	}
}

func mustRawExtension(payload any) *runtime.RawExtension {
	raw, err := toRawExtension(payload)
	Expect(err).NotTo(HaveOccurred())
	return raw
}

func toRawExtension(payload any) (*runtime.RawExtension, error) {
	if payload == nil {
		return nil, nil
	}
	switch v := payload.(type) {
	case *runtime.RawExtension:
		return v.DeepCopy(), nil
	case []byte:
		return &runtime.RawExtension{Raw: append([]byte(nil), v...)}, nil
	default:
		data, err := json.Marshal(payload)
		if err != nil {
			return nil, err
		}
		return &runtime.RawExtension{Raw: data}, nil
	}
}
