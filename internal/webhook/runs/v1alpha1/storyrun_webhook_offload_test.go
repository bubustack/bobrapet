package v1alpha1

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	bubuv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/internal/config"
	"github.com/bubustack/bobrapet/pkg/enums"
	"github.com/bubustack/bobrapet/pkg/refs"
	runsidentity "github.com/bubustack/bobrapet/pkg/runs/identity"
	"github.com/bubustack/bobrapet/pkg/storage"
	"github.com/bubustack/core/contracts"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestValidateCreateAcceptsOffloadedInputsWithOriginalTriggerHash(t *testing.T) {
	t.Helper()

	storage.ResetSharedManagerCacheForTests()
	t.Cleanup(storage.ResetSharedManagerCacheForTests)

	t.Setenv(contracts.StorageProviderEnv, contracts.StorageProviderFile)
	t.Setenv(contracts.StoragePathEnv, t.TempDir())

	scheme := runtime.NewScheme()
	require.NoError(t, bubuv1alpha1.AddToScheme(scheme))
	require.NoError(t, runsv1alpha1.AddToScheme(scheme))

	story := &bubuv1alpha1.Story{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "digest-story",
			Namespace: "default",
		},
		Spec: bubuv1alpha1.StorySpec{
			Steps: []bubuv1alpha1.Step{{
				Name: "prepare",
				Type: enums.StepTypeCondition,
			}},
		},
	}

	rawInputs := fmt.Appendf(nil, `{"payload":"%s"}`, strings.Repeat("a", 2048))
	inputHash, err := runsidentity.ComputeTriggerInputHash(rawInputs)
	require.NoError(t, err)

	manager, err := storage.NewManager(context.Background())
	require.NoError(t, err)
	dehydrated, err := manager.DehydrateInputs(context.Background(), map[string]any{
		"payload": strings.Repeat("a", 2048),
	}, "digest-story-trigger")
	require.NoError(t, err)
	rawOffloaded, err := jsonMarshal(dehydrated)
	require.NoError(t, err)
	require.Contains(t, string(rawOffloaded), storage.StorageRefKey)

	token := "daily-digest"
	storyRun := &runsv1alpha1.StoryRun{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      runsidentity.DeriveStoryRunName("default", story.Name, token),
			Annotations: map[string]string{
				runsidentity.StoryRunTriggerTokenAnnotation:     token,
				runsidentity.StoryRunTriggerInputHashAnnotation: inputHash,
			},
		},
		Spec: runsv1alpha1.StoryRunSpec{
			StoryRef: refs.StoryReference{
				ObjectReference: refs.ObjectReference{Name: story.Name},
			},
			Inputs: &runtime.RawExtension{Raw: rawOffloaded},
		},
	}

	validator := StoryRunCustomValidator{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(story).Build(),
		Config: config.DefaultControllerConfig(),
	}

	_, err = validator.ValidateCreate(context.Background(), storyRun)
	require.NoError(t, err)
}

func jsonMarshal(v any) ([]byte, error) {
	return json.Marshal(v)
}
