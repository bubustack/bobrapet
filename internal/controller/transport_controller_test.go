package controller

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	transportv1alpha1 "github.com/bubustack/bobrapet/api/transport/v1alpha1"
	bubuv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/internal/config"
	"github.com/bubustack/bobrapet/pkg/conditions"
	"github.com/bubustack/bobrapet/pkg/enums"
)

func TestTransportReconciler_validateTransport(t *testing.T) {
	r := &TransportReconciler{}
	t.Run("detects missing driver", func(t *testing.T) {
		transport := &transportv1alpha1.Transport{
			Spec: transportv1alpha1.TransportSpec{
				SupportedAudio: []transportv1alpha1.AudioCodec{{Name: "pcm16"}},
			},
		}
		errs := r.validateTransport(transport)
		if len(errs) == 0 {
			t.Fatalf("expected validation errors for missing driver")
		}
	})

	t.Run("requires at least one capability", func(t *testing.T) {
		transport := &transportv1alpha1.Transport{
			Spec: transportv1alpha1.TransportSpec{
				Provider: "custom",
				Driver:   "driver",
			},
		}
		errs := r.validateTransport(transport)
		if len(errs) == 0 {
			t.Fatalf("expected validation errors for missing capabilities")
		}
	})

	t.Run("passes when driver and capabilities present", func(t *testing.T) {
		transport := &transportv1alpha1.Transport{
			Spec: transportv1alpha1.TransportSpec{
				Driver:         "driver",
				SupportedAudio: []transportv1alpha1.AudioCodec{{Name: "pcm16"}},
			},
		}
		if errs := r.validateTransport(transport); len(errs) != 0 {
			t.Fatalf("expected no validation errors, got %v", errs)
		}
	})
}

func TestTransportReconciler_transportBindingToTransport(t *testing.T) {
	r := &TransportReconciler{}
	binding := &transportv1alpha1.TransportBinding{
		Spec: transportv1alpha1.TransportBindingSpec{
			TransportRef: "livekit",
		},
	}
	reqs := r.transportBindingToTransport(context.Background(), binding)
	expectRequests(t, reqs, "livekit")
}

func TestTransportReconciler_storyToTransport(t *testing.T) {
	r := &TransportReconciler{}
	story := &bubuv1alpha1.Story{
		Spec: bubuv1alpha1.StorySpec{
			Transports: []bubuv1alpha1.StoryTransport{
				{Name: "primary", TransportRef: "livekit"},
				{Name: "backup", TransportRef: "livekit"}, // duplicate should be collapsed
				{Name: "secondary", TransportRef: "nats"},
				{Name: "invalid"},
			},
		},
	}
	reqs := r.storyToTransport(context.Background(), story)
	expectRequests(t, reqs, "livekit", "nats")
}

func expectRequests(t *testing.T, reqs []reconcile.Request, expected ...string) {
	t.Helper()
	got := make([]string, 0, len(reqs))
	for _, req := range reqs {
		got = append(got, req.Name)
	}
	sort.Strings(got)
	sort.Strings(expected)
	if len(got) != len(expected) {
		t.Fatalf("unexpected requests: got %v want %v", got, expected)
	}
	for i := range got {
		if got[i] != expected[i] {
			t.Fatalf("unexpected requests: got %v want %v", got, expected)
		}
	}
	for _, req := range reqs {
		if req.NamespacedName != (types.NamespacedName{Name: req.Name}) {
			t.Fatalf("unexpected namespace on request: %+v", req.NamespacedName)
		}
	}
}

func TestTransportReconciler_collectAvailableCapabilitiesIncludesBinary(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	require.NoError(t, transportv1alpha1.AddToScheme(scheme))

	readyTime := metav1.NewTime(time.Now())
	readyBinding := &transportv1alpha1.TransportBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "ready",
			Namespace: "default",
		},
		Spec: transportv1alpha1.TransportBindingSpec{
			TransportRef: "demo",
			Driver:       "driver",
		},
		Status: transportv1alpha1.TransportBindingStatus{
			NegotiatedAudio:  &transportv1alpha1.AudioCodec{Name: "pcm16"},
			NegotiatedVideo:  &transportv1alpha1.VideoCodec{Name: "h264"},
			NegotiatedBinary: "application/json",
			Conditions: []metav1.Condition{{
				Type:               conditions.ConditionReady,
				Status:             metav1.ConditionTrue,
				LastTransitionTime: readyTime,
				Reason:             conditions.ReasonValidationPassed,
			}},
		},
	}
	failedBinding := &transportv1alpha1.TransportBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "failed",
			Namespace: "default",
		},
		Spec: transportv1alpha1.TransportBindingSpec{
			TransportRef: "demo",
			Driver:       "driver",
		},
		Status: transportv1alpha1.TransportBindingStatus{
			Conditions: []metav1.Condition{{
				Type:               conditions.ConditionReady,
				Status:             metav1.ConditionFalse,
				LastTransitionTime: metav1.NewTime(readyTime.Add(-time.Minute)),
				Reason:             conditions.ReasonTransportFailed,
				Message:            "mock failure",
			}},
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithIndex(&transportv1alpha1.TransportBinding{}, "spec.transportRef", func(obj client.Object) []string {
			tb := obj.(*transportv1alpha1.TransportBinding)
			if tb.Spec.TransportRef == "" {
				return nil
			}
			return []string{tb.Spec.TransportRef}
		}).
		WithObjects(readyBinding.DeepCopy(), failedBinding.DeepCopy()).
		Build()

	reconciler := &TransportReconciler{
		ControllerDependencies: config.ControllerDependencies{
			Client: k8sClient,
			Scheme: scheme,
		},
	}

	audio, video, binary, readyCount, totalCount, bindingErrors, lastHeartbeat, err := reconciler.collectAvailableCapabilities(context.Background(),
		&transportv1alpha1.Transport{ObjectMeta: metav1.ObjectMeta{Name: "demo"}}, logr.Discard())
	require.NoError(t, err)
	require.Len(t, audio, 1)
	require.Len(t, video, 1)
	require.Len(t, binary, 1)
	require.Equal(t, "application/json", binary[0])
	require.Equal(t, 1, readyCount)
	require.Equal(t, 2, totalCount)
	require.Len(t, bindingErrors, 1)
	require.NotNil(t, lastHeartbeat)
	require.WithinDuration(t, readyTime.Time, lastHeartbeat.Time, time.Second)
}

func TestTransportReconciler_collectAvailableCapabilitiesFlagsStale(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	require.NoError(t, transportv1alpha1.AddToScheme(scheme))

	staleBinding := &transportv1alpha1.TransportBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "stale",
			Namespace: "default",
		},
		Spec: transportv1alpha1.TransportBindingSpec{
			TransportRef: "demo",
			Driver:       "driver",
		},
		Status: transportv1alpha1.TransportBindingStatus{
			NegotiatedAudio: &transportv1alpha1.AudioCodec{Name: "pcm16"},
			Conditions: []metav1.Condition{{
				Type:               conditions.ConditionReady,
				Status:             metav1.ConditionTrue,
				LastTransitionTime: metav1.NewTime(time.Now().Add(-10 * time.Minute)),
				Reason:             conditions.ReasonValidationPassed,
			}},
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&transportv1alpha1.TransportBinding{}).
		WithIndex(&transportv1alpha1.TransportBinding{}, "spec.transportRef", func(obj client.Object) []string {
			tb := obj.(*transportv1alpha1.TransportBinding)
			if tb.Spec.TransportRef == "" {
				return nil
			}
			return []string{tb.Spec.TransportRef}
		}).
		WithObjects(staleBinding.DeepCopy()).
		Build()

	reconciler := &TransportReconciler{
		ControllerDependencies: config.ControllerDependencies{
			Client: k8sClient,
			Scheme: scheme,
		},
	}

	_, _, _, readyCount, totalCount, bindingErrors, _, err := reconciler.collectAvailableCapabilities(
		context.Background(),
		&transportv1alpha1.Transport{ObjectMeta: metav1.ObjectMeta{Name: "demo"}},
		logr.Discard(),
	)
	require.NoError(t, err)
	require.Equal(t, 0, readyCount)
	require.Equal(t, 1, totalCount)
	require.Len(t, bindingErrors, 1)
	require.Contains(t, bindingErrors[0], "stale")

	var updated transportv1alpha1.TransportBinding
	require.NoError(t, k8sClient.Get(context.Background(), types.NamespacedName{Name: "stale", Namespace: "default"}, &updated))
	cond := conditions.GetCondition(updated.Status.Conditions, conditions.ConditionReady)
	require.NotNil(t, cond)
	require.Equal(t, metav1.ConditionFalse, cond.Status)
	require.Equal(t, conditions.ReasonTransportFailed, cond.Reason)
	require.Contains(t, cond.Message, "stale")
}

func TestTransportReconciler_collectAvailableCapabilitiesReturnsErrorWhenMarkStaleFails(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	require.NoError(t, transportv1alpha1.AddToScheme(scheme))

	staleBinding := &transportv1alpha1.TransportBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "stale",
			Namespace: "default",
		},
		Spec: transportv1alpha1.TransportBindingSpec{
			TransportRef: "demo",
			Driver:       "driver",
		},
		Status: transportv1alpha1.TransportBindingStatus{
			Conditions: []metav1.Condition{{
				Type:               conditions.ConditionReady,
				Status:             metav1.ConditionTrue,
				LastTransitionTime: metav1.NewTime(time.Now().Add(-10 * time.Minute)),
				Reason:             conditions.ReasonValidationPassed,
			}},
		},
	}

	baseClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&transportv1alpha1.TransportBinding{}).
		WithIndex(&transportv1alpha1.TransportBinding{}, "spec.transportRef", func(obj client.Object) []string {
			tb := obj.(*transportv1alpha1.TransportBinding)
			if tb.Spec.TransportRef == "" {
				return nil
			}
			return []string{tb.Spec.TransportRef}
		}).
		WithObjects(staleBinding.DeepCopy()).
		Build()

	markErr := errors.New("status patch failed")
	reconciler := &TransportReconciler{
		ControllerDependencies: config.ControllerDependencies{
			Client: &failingStatusClient{
				Client: baseClient,
				err:    markErr,
			},
			Scheme: scheme,
		},
	}

	_, _, _, _, _, _, _, err := reconciler.collectAvailableCapabilities(
		context.Background(),
		&transportv1alpha1.Transport{ObjectMeta: metav1.ObjectMeta{Name: "demo"}},
		logr.Discard(),
	)
	require.Error(t, err)
	require.ErrorIs(t, err, markErr)
}

func TestTransportReconciler_updateStatusTracksHeartbeat(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	require.NoError(t, transportv1alpha1.AddToScheme(scheme))

	transport := &transportv1alpha1.Transport{
		ObjectMeta: metav1.ObjectMeta{
			Name: "demo",
		},
		Spec: transportv1alpha1.TransportSpec{
			SupportedAudio: []transportv1alpha1.AudioCodec{{Name: "pcm16"}},
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&transportv1alpha1.Transport{}).
		WithObjects(transport.DeepCopy()).
		Build()

	reconciler := &TransportReconciler{
		ControllerDependencies: config.ControllerDependencies{
			Client: k8sClient,
			Scheme: scheme,
		},
	}

	hb := metav1.NewTime(time.Now())
	err := reconciler.updateStatus(
		context.Background(),
		transport,
		nil,
		1,
		[]transportv1alpha1.AudioCodec{{Name: "pcm16"}},
		nil,
		nil,
		1,
		2,
		nil,
		&hb,
	)
	require.NoError(t, err)

	var updated transportv1alpha1.Transport
	require.NoError(t, k8sClient.Get(context.Background(), types.NamespacedName{Name: "demo"}, &updated))
	require.Equal(t, int32(1), updated.Status.PendingBindings)
	require.NotNil(t, updated.Status.LastHeartbeatTime)
	require.WithinDuration(t, hb.Time, updated.Status.LastHeartbeatTime.Time, time.Second)
}

func TestTransportReconciler_updateStatusSetsConditionsAndEvents(t *testing.T) {
	t.Parallel()

	makeTransport := func(name string) *transportv1alpha1.Transport {
		return &transportv1alpha1.Transport{
			ObjectMeta: metav1.ObjectMeta{Name: name},
			Spec: transportv1alpha1.TransportSpec{
				SupportedAudio: []transportv1alpha1.AudioCodec{{Name: "pcm16"}},
				SupportedBinary: []string{
					"application/json",
				},
			},
		}
	}

	tests := []struct {
		name            string
		validationErrs  []string
		bindingErrs     []string
		ready           int
		total           int
		lastHeartbeat   *metav1.Time
		expectStatus    enums.ValidationStatus
		expectCond      metav1.ConditionStatus
		expectReason    string
		expectPending   int32
		expectEvent     bool
		expectAudioCopy bool
	}{
		{
			name:           "validation errors surface condition",
			validationErrs: []string{"driver missing"},
			ready:          0,
			total:          0,
			expectStatus:   enums.ValidationStatusInvalid,
			expectCond:     metav1.ConditionFalse,
			expectReason:   conditions.ReasonValidationFailed,
			expectPending:  0,
			expectEvent:    true,
		},
		{
			name:          "binding errors mark transport failed",
			bindingErrs:   []string{"default/binding: connector unhealthy"},
			ready:         0,
			total:         1,
			expectStatus:  enums.ValidationStatusInvalid,
			expectCond:    metav1.ConditionFalse,
			expectReason:  conditions.ReasonTransportFailed,
			expectPending: 0,
			expectEvent:   true,
		},
		{
			name:          "pending bindings keep status unknown",
			ready:         0,
			total:         2,
			expectStatus:  enums.ValidationStatusUnknown,
			expectCond:    metav1.ConditionFalse,
			expectReason:  conditions.ReasonReconciling,
			expectPending: 2,
			expectEvent:   false,
		},
		func() struct {
			name            string
			validationErrs  []string
			bindingErrs     []string
			ready           int
			total           int
			lastHeartbeat   *metav1.Time
			expectStatus    enums.ValidationStatus
			expectCond      metav1.ConditionStatus
			expectReason    string
			expectPending   int32
			expectEvent     bool
			expectAudioCopy bool
		} {
			hb := metav1.NewTime(time.Now())
			return struct {
				name            string
				validationErrs  []string
				bindingErrs     []string
				ready           int
				total           int
				lastHeartbeat   *metav1.Time
				expectStatus    enums.ValidationStatus
				expectCond      metav1.ConditionStatus
				expectReason    string
				expectPending   int32
				expectEvent     bool
				expectAudioCopy bool
			}{
				name:            "ready transport copies spec capabilities",
				ready:           2,
				total:           2,
				lastHeartbeat:   &hb,
				expectStatus:    enums.ValidationStatusValid,
				expectCond:      metav1.ConditionTrue,
				expectReason:    conditions.ReasonValidationPassed,
				expectPending:   0,
				expectEvent:     false,
				expectAudioCopy: true,
			}
		}(),
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			scheme := runtime.NewScheme()
			require.NoError(t, transportv1alpha1.AddToScheme(scheme))

			transport := makeTransport(fmt.Sprintf("demo-%s", tc.name))

			k8sClient := fake.NewClientBuilder().
				WithScheme(scheme).
				WithStatusSubresource(&transportv1alpha1.Transport{}).
				WithObjects(transport.DeepCopy()).
				Build()

			recorder := record.NewFakeRecorder(10)

			reconciler := &TransportReconciler{
				ControllerDependencies: config.ControllerDependencies{
					Client: k8sClient,
					Scheme: scheme,
				},
				Recorder: recorder,
			}

			err := reconciler.updateStatus(
				context.Background(),
				transport,
				tc.validationErrs,
				0,
				nil,
				nil,
				nil,
				tc.ready,
				tc.total,
				tc.bindingErrs,
				tc.lastHeartbeat,
			)
			require.NoError(t, err)

			var updated transportv1alpha1.Transport
			require.NoError(t, k8sClient.Get(context.Background(), types.NamespacedName{Name: transport.Name}, &updated))

			require.Equal(t, tc.expectStatus, updated.Status.ValidationStatus)
			require.Equal(t, tc.expectPending, updated.Status.PendingBindings)

			cond := conditions.GetCondition(updated.Status.Conditions, conditions.ConditionReady)
			require.NotNil(t, cond)
			require.Equal(t, tc.expectCond, cond.Status)
			require.Equal(t, tc.expectReason, cond.Reason)

			if tc.lastHeartbeat != nil {
				require.NotNil(t, updated.Status.LastHeartbeatTime)
				require.WithinDuration(t, tc.lastHeartbeat.Time, updated.Status.LastHeartbeatTime.Time, time.Second)
			} else {
				require.Nil(t, updated.Status.LastHeartbeatTime)
			}

			if tc.expectAudioCopy {
				require.Len(t, updated.Status.AvailableAudio, 1)
				require.Equal(t, transport.Spec.SupportedAudio[0].Name, updated.Status.AvailableAudio[0].Name)
				require.Equal(t, transport.Spec.SupportedBinary, updated.Status.AvailableBinary)
			}

			event := readRecorderEvent(recorder)
			if tc.expectEvent {
				require.NotEmpty(t, event)
				require.Contains(t, event, tc.expectReason)
			} else {
				require.Empty(t, event)
			}
		})
	}
}

func TestTransportReconciler_updateStatusEmitsBothEventsAndSummaries(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	require.NoError(t, transportv1alpha1.AddToScheme(scheme))

	transport := &transportv1alpha1.Transport{
		ObjectMeta: metav1.ObjectMeta{Name: "demo"},
		Spec: transportv1alpha1.TransportSpec{
			SupportedAudio: []transportv1alpha1.AudioCodec{{Name: "pcm16"}},
		},
	}

	k8sClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&transportv1alpha1.Transport{}).
		WithObjects(transport.DeepCopy()).
		Build()

	recorder := record.NewFakeRecorder(20)

	reconciler := &TransportReconciler{
		ControllerDependencies: config.ControllerDependencies{
			Client: k8sClient,
			Scheme: scheme,
		},
		Recorder: recorder,
	}

	validationErrs := []string{"driver missing"}
	bindingErrs := []string{
		"default/binding: heartbeat stale",
		"default/binding: negotiation failed",
		"default/binding: codec unsupported",
		"prod/binding: not ready",
		"prod/binding: unhealthy",
		"prod/binding: retrying",
	}

	err := reconciler.updateStatus(
		context.Background(),
		transport,
		validationErrs,
		0,
		nil,
		nil,
		nil,
		0,
		6,
		bindingErrs,
		nil,
	)
	require.NoError(t, err)

	events := readAllRecorderEvents(recorder)
	require.Len(t, events, 2)
	require.Contains(t, events[0], conditions.ReasonValidationFailed)
	require.Contains(t, events[1], conditions.ReasonTransportFailed)
	require.Contains(t, events[1], "... (+1 more)")

	var updated transportv1alpha1.Transport
	require.NoError(t, k8sClient.Get(context.Background(), types.NamespacedName{Name: transport.Name}, &updated))
	require.Equal(t, int32(4), updated.Status.PendingBindings)
}

func readRecorderEvent(recorder *record.FakeRecorder) string {
	select {
	case evt := <-recorder.Events:
		return evt
	case <-time.After(10 * time.Millisecond):
		return ""
	}
}

func readAllRecorderEvents(recorder *record.FakeRecorder) []string {
	var events []string
	for {
		select {
		case evt := <-recorder.Events:
			events = append(events, evt)
		default:
			return events
		}
	}
}

type failingStatusClient struct {
	client.Client
	err error
}

func (c *failingStatusClient) Status() client.StatusWriter {
	return &failingStatusWriter{
		StatusWriter: c.Client.Status(),
		err:          c.err,
	}
}

type failingStatusWriter struct {
	client.StatusWriter
	err error
}

func (w *failingStatusWriter) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
	if w.err != nil {
		return w.err
	}
	return w.StatusWriter.Patch(ctx, obj, patch, opts...)
}
