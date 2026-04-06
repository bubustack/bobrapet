package runs

import (
	"context"
	"fmt"
	"strings"
	"testing"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	bubuv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/pkg/refs"
	runsidentity "github.com/bubustack/bobrapet/pkg/runs/identity"
	"github.com/bubustack/bobrapet/pkg/storage"
	"github.com/bubustack/core/contracts"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime"
)

func TestPrepareStoryRunForCreateOffloadsOversizedInputs(t *testing.T) {
	t.Helper()

	storage.ResetSharedManagerCacheForTests()
	t.Cleanup(storage.ResetSharedManagerCacheForTests)

	t.Setenv(contracts.StorageProviderEnv, contracts.StorageProviderFile)
	t.Setenv(contracts.StoragePathEnv, t.TempDir())

	rawInputs := fmt.Appendf(nil, `{"payload":"%s"}`, strings.Repeat("a", 2048))
	inputHash, err := runsidentity.ComputeTriggerInputHash(rawInputs)
	require.NoError(t, err)

	trigger := newStoryTriggerForOffloadTest(rawInputs)
	reconciler := &StoryTriggerReconciler{}

	desired, err := reconciler.prepareStoryRunForCreate(context.Background(), trigger, inputHash)
	require.NoError(t, err)
	require.NotNil(t, desired)
	require.NotNil(t, desired.Spec.Inputs)
	require.Contains(t, string(desired.Spec.Inputs.Raw), storage.StorageRefKey)
	require.Equal(t, inputHash, desired.Annotations[runsidentity.StoryRunTriggerInputHashAnnotation])
}

func TestStoryRunMatchesTriggerUsesStoredInputHashAnnotation(t *testing.T) {
	t.Helper()

	storage.ResetSharedManagerCacheForTests()
	t.Cleanup(storage.ResetSharedManagerCacheForTests)

	t.Setenv(contracts.StorageProviderEnv, contracts.StorageProviderFile)
	t.Setenv(contracts.StoragePathEnv, t.TempDir())

	rawInputs := fmt.Appendf(nil, `{"payload":"%s"}`, strings.Repeat("a", 2048))
	inputHash, err := runsidentity.ComputeTriggerInputHash(rawInputs)
	require.NoError(t, err)

	trigger := newStoryTriggerForOffloadTest(rawInputs)
	reconciler := &StoryTriggerReconciler{}

	desired, err := reconciler.prepareStoryRunForCreate(context.Background(), trigger, inputHash)
	require.NoError(t, err)
	require.True(t, storyRunMatchesTrigger(desired, trigger, inputHash))
}

func newStoryTriggerForOffloadTest(rawInputs []byte) *runsv1alpha1.StoryTrigger {
	mode := bubuv1alpha1.TriggerDedupeKey
	return &runsv1alpha1.StoryTrigger{
		Spec: runsv1alpha1.StoryTriggerSpec{
			StoryRef: refs.StoryReference{
				ObjectReference: refs.ObjectReference{Name: "advanced-digest"},
				Version:         "v1",
			},
			Inputs: &runtime.RawExtension{Raw: rawInputs},
			DeliveryIdentity: runsv1alpha1.TriggerDeliveryIdentity{
				Mode:         &mode,
				Key:          "daily-digest",
				InputHash:    mustTriggerHash(rawInputs),
				SubmissionID: "submission-1",
			},
		},
	}
}

func mustTriggerHash(raw []byte) string {
	hash, err := runsidentity.ComputeTriggerInputHash(raw)
	if err != nil {
		panic(err)
	}
	return hash
}
