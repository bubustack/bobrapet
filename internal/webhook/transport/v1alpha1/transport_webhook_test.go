package v1alpha1

import (
	"context"
	"errors"
	"slices"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	transportv1alpha1 "github.com/bubustack/bobrapet/api/transport/v1alpha1"
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// newTransportScheme returns a runtime.Scheme with the transport API types registered.
func newTransportScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := transportv1alpha1.AddToScheme(s); err != nil {
		t.Fatalf("failed to register transport scheme: %v", err)
	}
	return s
}

// validTransport returns a minimal Transport that passes all validations.
func validTransport(name string) *transportv1alpha1.Transport {
	return &transportv1alpha1.Transport{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: transportv1alpha1.TransportSpec{
			Provider:       "livekit",
			Driver:         "bobravoz-grpc",
			SupportedAudio: []transportv1alpha1.AudioCodec{{Name: "opus"}},
		},
	}
}

// validBinding returns a minimal TransportBinding that passes all validations
// against validTransport("livekit").
func validBinding() *transportv1alpha1.TransportBinding {
	return &transportv1alpha1.TransportBinding{
		ObjectMeta: metav1.ObjectMeta{Name: "b", Namespace: "default"},
		Spec: transportv1alpha1.TransportBindingSpec{
			TransportRef: "livekit",
			Driver:       "bobravoz-grpc",
			StepName:     "step-a",
			EngramName:   "engram-a",
			Audio: &transportv1alpha1.AudioBinding{
				Direction: transportv1alpha1.MediaDirectionBidirectional,
				Codecs:    []transportv1alpha1.AudioCodec{{Name: "opus"}},
			},
		},
	}
}

// failingReader is a client.Reader that always returns the configured error.
// Used to exercise transient API error paths.
type failingReader struct{ err error }

func (r *failingReader) Get(_ context.Context, _ client.ObjectKey, _ client.Object, _ ...client.GetOption) error {
	return r.err
}
func (r *failingReader) List(_ context.Context, _ client.ObjectList, _ ...client.ListOption) error {
	return r.err
}

// slowReader is a client.Reader whose Get blocks until the caller's context is done.
// Used to exercise the transport lookup timeout / cancellation path.
type slowReader struct{}

func (r *slowReader) Get(ctx context.Context, _ client.ObjectKey, _ client.Object, _ ...client.GetOption) error {
	<-ctx.Done()
	return ctx.Err()
}
func (r *slowReader) List(ctx context.Context, _ client.ObjectList, _ ...client.ListOption) error {
	<-ctx.Done()
	return ctx.Err()
}

// causeFields extracts field paths from a StatusError's causes list.
func causeFields(err error) []string {
	var statusErr *apierrors.StatusError
	if !errors.As(err, &statusErr) {
		return nil
	}
	if statusErr.ErrStatus.Details == nil {
		return nil
	}
	var fields []string
	for _, c := range statusErr.ErrStatus.Details.Causes {
		fields = append(fields, c.Field)
	}
	return fields
}

// ---------------------------------------------------------------------------
// TransportValidator — ValidateCreate
// ---------------------------------------------------------------------------

func TestTransportValidatorValidateCreate_Valid(t *testing.T) {
	tv := &TransportValidator{}
	_, err := tv.ValidateCreate(context.Background(), validTransport("t"))
	if err != nil {
		t.Fatalf("expected no error for valid transport, got %v", err)
	}
}

func TestTransportValidatorValidateCreate_MissingCapabilities(t *testing.T) {
	tv := &TransportValidator{}
	tr := &transportv1alpha1.Transport{
		Spec: transportv1alpha1.TransportSpec{
			Provider: "livekit",
			Driver:   "bobravoz-grpc",
			// no SupportedAudio / SupportedVideo / SupportedBinary
		},
	}
	_, err := tv.ValidateCreate(context.Background(), tr)
	if err == nil {
		t.Fatal("expected validation error when no capabilities declared")
	}
	if !apierrors.IsInvalid(err) {
		t.Fatalf("expected apierrors.Invalid, got %v", err)
	}
}

func TestTransportValidatorValidateCreate_RejectsPlaintextDefaultSecurityMode(t *testing.T) {
	tv := &TransportValidator{}
	tr := validTransport("t")
	tr.Spec.DefaultSettings = &runtime.RawExtension{
		Raw: []byte(`{"env":{"BUBU_TRANSPORT_SECURITY_MODE":"plaintext"}}`),
	}

	_, err := tv.ValidateCreate(context.Background(), tr)
	if err == nil {
		t.Fatal("expected validation error for plaintext default security mode")
	}
	if !apierrors.IsInvalid(err) {
		t.Fatalf("expected apierrors.Invalid, got %v", err)
	}
	fields := causeFields(err)
	if !containsField(fields, "spec.defaultSettings.env.BUBU_TRANSPORT_SECURITY_MODE") {
		t.Errorf("expected spec.defaultSettings.env.BUBU_TRANSPORT_SECURITY_MODE in causes, got %v", fields)
	}
}

// ---------------------------------------------------------------------------
// TransportValidator — ValidateUpdate
// ---------------------------------------------------------------------------

func TestTransportValidatorValidateUpdate_SpecUnchanged_Skips(t *testing.T) {
	tv := &TransportValidator{}
	old := validTransport("t")
	// Use DeepCopy so the equality check compares two distinct objects rather than
	// trivially passing because both arguments are the same pointer.
	newTr := old.DeepCopy()
	_, err := tv.ValidateUpdate(context.Background(), old, newTr)
	if err != nil {
		t.Fatalf("expected no error for spec-equal update, got %v", err)
	}
}

func TestTransportValidatorValidateUpdate_SpecChanged_Validates(t *testing.T) {
	tv := &TransportValidator{}
	old := validTransport("t")
	newTr := old.DeepCopy()
	newTr.Spec.SupportedAudio = nil // remove capabilities → should fail
	_, err := tv.ValidateUpdate(context.Background(), old, newTr)
	if err == nil {
		t.Fatal("expected validation error when capabilities removed")
	}
	if !apierrors.IsInvalid(err) {
		t.Fatalf("expected apierrors.Invalid, got %v", err)
	}
}

func TestTransportValidatorValidateUpdate_DeletionTimestamp_Skips(t *testing.T) {
	tv := &TransportValidator{}
	now := metav1.Now()
	// newObj has DeletionTimestamp set and invalid spec (no capabilities).
	// Validation must be skipped entirely.
	newTr := &transportv1alpha1.Transport{
		ObjectMeta: metav1.ObjectMeta{DeletionTimestamp: &now},
		Spec: transportv1alpha1.TransportSpec{
			Provider: "livekit",
			Driver:   "bobravoz-grpc",
		},
	}
	_, err := tv.ValidateUpdate(context.Background(), validTransport("t"), newTr)
	if err != nil {
		t.Fatalf("expected no error when DeletionTimestamp set, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// TransportValidator — ValidateDelete
// ---------------------------------------------------------------------------

func TestTransportValidatorValidateDelete_AlwaysAllows(t *testing.T) {
	tv := &TransportValidator{}
	_, err := tv.ValidateDelete(context.Background(), validTransport("t"))
	if err != nil {
		t.Fatalf("expected no error on delete, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// TransportValidator — validateTransport (direct, for streaming-settings depth)
// ---------------------------------------------------------------------------

func TestTransportValidatorRequiresCapabilities(t *testing.T) {
	tv := &TransportValidator{}
	tr := &transportv1alpha1.Transport{
		Spec: transportv1alpha1.TransportSpec{
			Provider: "livekit",
			Driver:   "bobravoz-grpc",
		},
	}
	if err := tv.validateTransport(tr); err == nil {
		t.Fatalf("expected validation error when no capabilities declared")
	}
}

// TestTransportValidatorRejectsInvalidStreamingSettings verifies each category of
// invalid streaming setting individually rather than in a single mega-fixture.
// Each error case also asserts the expected field path so that mis-anchored errors
// (e.g. reporting at "spec" instead of the real leaf path) are caught early.
func TestTransportValidatorRejectsInvalidStreamingSettings(t *testing.T) {
	base := transportv1alpha1.TransportSpec{
		Provider:       "livekit",
		Driver:         "bobravoz-grpc",
		SupportedAudio: []transportv1alpha1.AudioCodec{{Name: "opus"}},
	}
	tv := &TransportValidator{}

	cases := []struct {
		name       string
		mutate     func(*transportv1alpha1.TransportSpec)
		wantErr    bool
		wantFields []string // every entry must appear in the error causes
	}{
		{
			name:    "valid baseline",
			mutate:  func(_ *transportv1alpha1.TransportSpec) {},
			wantErr: false,
		},
		{
			name: "negative buffer maxMessages",
			mutate: func(s *transportv1alpha1.TransportSpec) {
				s.Streaming = &transportv1alpha1.TransportStreamingSettings{
					Backpressure: &transportv1alpha1.TransportBackpressureSettings{
						Buffer: &transportv1alpha1.TransportBufferSettings{
							MaxMessages: new(int32(-1)),
						},
					},
				}
			},
			wantErr:    true,
			wantFields: []string{"spec.streaming.backpressure.buffer.maxMessages"},
		},
		{
			name: "invalid flow control mode",
			mutate: func(s *transportv1alpha1.TransportSpec) {
				s.Streaming = &transportv1alpha1.TransportStreamingSettings{
					FlowControl: &transportv1alpha1.TransportFlowControlSettings{
						Mode: transportv1alpha1.FlowControlMode("bogus"),
					},
				}
			},
			wantErr:    true,
			wantFields: []string{"spec.streaming.flowControl.mode"},
		},
		{
			name: "invalid ack maxDelay duration",
			mutate: func(s *transportv1alpha1.TransportSpec) {
				s.Streaming = &transportv1alpha1.TransportStreamingSettings{
					FlowControl: &transportv1alpha1.TransportFlowControlSettings{
						AckEvery: &transportv1alpha1.TransportFlowAckSettings{
							MaxDelay: new("not-a-duration"),
						},
					},
				}
			},
			wantErr:    true,
			wantFields: []string{"spec.streaming.flowControl.ackEvery.maxDelay"},
		},
		{
			name: "pause threshold > 100",
			mutate: func(s *transportv1alpha1.TransportSpec) {
				s.Streaming = &transportv1alpha1.TransportStreamingSettings{
					FlowControl: &transportv1alpha1.TransportFlowControlSettings{
						PauseThreshold: &transportv1alpha1.TransportFlowThreshold{BufferPct: new(int32(120))},
					},
				}
			},
			wantErr:    true,
			wantFields: []string{"spec.streaming.flowControl.pauseThreshold.bufferPct"},
		},
		{
			name: "invalid delivery semantics",
			mutate: func(s *transportv1alpha1.TransportSpec) {
				s.Streaming = &transportv1alpha1.TransportStreamingSettings{
					Delivery: &transportv1alpha1.TransportDeliverySettings{
						Semantics: transportv1alpha1.DeliverySemantics("exactly_once"),
					},
				}
			},
			wantErr:    true,
			wantFields: []string{"spec.streaming.delivery.semantics"},
		},
		{
			name: "invalid replay checkpointInterval",
			mutate: func(s *transportv1alpha1.TransportSpec) {
				s.Streaming = &transportv1alpha1.TransportStreamingSettings{
					Delivery: &transportv1alpha1.TransportDeliverySettings{
						Replay: &transportv1alpha1.TransportReplaySettings{
							CheckpointInterval: new("bad"),
						},
					},
				}
			},
			wantErr:    true,
			wantFields: []string{"spec.streaming.delivery.replay.checkpointInterval"},
		},
		{
			name: "invalid routing mode",
			mutate: func(s *transportv1alpha1.TransportSpec) {
				s.Streaming = &transportv1alpha1.TransportStreamingSettings{
					Routing: &transportv1alpha1.TransportRoutingSettings{
						Mode: transportv1alpha1.TransportRoutingMode("mesh"),
					},
				}
			},
			wantErr:    true,
			wantFields: []string{"spec.streaming.routing.mode"},
		},
		{
			name: "invalid fanOut mode",
			mutate: func(s *transportv1alpha1.TransportSpec) {
				s.Streaming = &transportv1alpha1.TransportStreamingSettings{
					Routing: &transportv1alpha1.TransportRoutingSettings{
						FanOut: transportv1alpha1.TransportFanOutMode("fanout"),
					},
				}
			},
			wantErr:    true,
			wantFields: []string{"spec.streaming.routing.fanOut"},
		},
		{
			name: "maxDownstreams=0",
			mutate: func(s *transportv1alpha1.TransportSpec) {
				s.Streaming = &transportv1alpha1.TransportStreamingSettings{
					Routing: &transportv1alpha1.TransportRoutingSettings{
						MaxDownstreams: new(int32(0)),
					},
				}
			},
			wantErr:    true,
			wantFields: []string{"spec.streaming.routing.maxDownstreams"},
		},
		{
			name: "routing rule invalid action",
			mutate: func(s *transportv1alpha1.TransportSpec) {
				s.Streaming = &transportv1alpha1.TransportStreamingSettings{
					Routing: &transportv1alpha1.TransportRoutingSettings{
						Rules: []transportv1alpha1.TransportRoutingRule{
							{Action: transportv1alpha1.TransportRoutingRuleAction("block")},
						},
					},
				}
			},
			wantErr:    true,
			wantFields: []string{"spec.streaming.routing.rules[0].action"},
		},
		{
			name: "routing rule target empty step name",
			mutate: func(s *transportv1alpha1.TransportSpec) {
				s.Streaming = &transportv1alpha1.TransportStreamingSettings{
					Routing: &transportv1alpha1.TransportRoutingSettings{
						Rules: []transportv1alpha1.TransportRoutingRule{
							{
								Action: transportv1alpha1.TransportRoutingRuleAllow,
								Target: &transportv1alpha1.TransportRoutingRuleTarget{
									Steps: []string{"", "a"},
								},
							},
						},
					},
				}
			},
			wantErr:    true,
			wantFields: []string{"spec.streaming.routing.rules[0].target.steps[0]"},
		},
		{
			name: "routing rule target duplicate step name",
			mutate: func(s *transportv1alpha1.TransportSpec) {
				s.Streaming = &transportv1alpha1.TransportStreamingSettings{
					Routing: &transportv1alpha1.TransportRoutingSettings{
						Rules: []transportv1alpha1.TransportRoutingRule{
							{
								Action: transportv1alpha1.TransportRoutingRuleAllow,
								Target: &transportv1alpha1.TransportRoutingRuleTarget{
									Steps: []string{"a", "a"},
								},
							},
						},
					},
				}
			},
			wantErr:    true,
			wantFields: []string{"spec.streaming.routing.rules[0].target.steps[1]"},
		},
		{
			name: "partitioning invalid mode",
			mutate: func(s *transportv1alpha1.TransportSpec) {
				s.Streaming = &transportv1alpha1.TransportStreamingSettings{
					Partitioning: &transportv1alpha1.TransportPartitioningSettings{
						Mode: transportv1alpha1.TransportPartitionMode("range"),
					},
				}
			},
			wantErr:    true,
			wantFields: []string{"spec.streaming.partitioning.mode"},
		},
		{
			name: "partitioning blank key",
			mutate: func(s *transportv1alpha1.TransportSpec) {
				s.Streaming = &transportv1alpha1.TransportStreamingSettings{
					Partitioning: &transportv1alpha1.TransportPartitioningSettings{
						Key: new(" "),
					},
				}
			},
			wantErr:    true,
			wantFields: []string{"spec.streaming.partitioning.key"},
		},
		{
			name: "partitions=0",
			mutate: func(s *transportv1alpha1.TransportSpec) {
				s.Streaming = &transportv1alpha1.TransportStreamingSettings{
					Partitioning: &transportv1alpha1.TransportPartitioningSettings{
						Partitions: new(int32(0)),
					},
				}
			},
			wantErr:    true,
			wantFields: []string{"spec.streaming.partitioning.partitions"},
		},
		{
			name: "lifecycle invalid strategy",
			mutate: func(s *transportv1alpha1.TransportSpec) {
				s.Streaming = &transportv1alpha1.TransportStreamingSettings{
					Lifecycle: &transportv1alpha1.TransportLifecycleSettings{
						Strategy: transportv1alpha1.TransportUpgradeStrategy("invalid"),
					},
				}
			},
			wantErr:    true,
			wantFields: []string{"spec.streaming.lifecycle.strategy"},
		},
		{
			name: "negative drainTimeoutSeconds",
			mutate: func(s *transportv1alpha1.TransportSpec) {
				s.Streaming = &transportv1alpha1.TransportStreamingSettings{
					Lifecycle: &transportv1alpha1.TransportLifecycleSettings{
						DrainTimeoutSeconds: new(int32(-1)),
					},
				}
			},
			wantErr:    true,
			wantFields: []string{"spec.streaming.lifecycle.drainTimeoutSeconds"},
		},
		{
			name: "negative maxInFlight",
			mutate: func(s *transportv1alpha1.TransportSpec) {
				s.Streaming = &transportv1alpha1.TransportStreamingSettings{
					Lifecycle: &transportv1alpha1.TransportLifecycleSettings{
						MaxInFlight: new(int32(-5)),
					},
				}
			},
			wantErr:    true,
			wantFields: []string{"spec.streaming.lifecycle.maxInFlight"},
		},
		{
			name: "tracing sampleRate > 100",
			mutate: func(s *transportv1alpha1.TransportSpec) {
				s.Streaming = &transportv1alpha1.TransportStreamingSettings{
					Observability: &transportv1alpha1.TransportObservabilitySettings{
						Tracing: &transportv1alpha1.TransportTracingSettings{
							SampleRate: new(int32(200)),
						},
					},
				}
			},
			wantErr:    true,
			wantFields: []string{"spec.streaming.observability.tracing.sampleRate"},
		},
		{
			name: "tracing invalid samplePolicy",
			mutate: func(s *transportv1alpha1.TransportSpec) {
				s.Streaming = &transportv1alpha1.TransportStreamingSettings{
					Observability: &transportv1alpha1.TransportObservabilitySettings{
						Tracing: &transportv1alpha1.TransportTracingSettings{
							SamplePolicy: new("sometimes"),
						},
					},
				}
			},
			wantErr:    true,
			wantFields: []string{"spec.streaming.observability.tracing.samplePolicy"},
		},
		{
			name: "recording invalid mode",
			mutate: func(s *transportv1alpha1.TransportSpec) {
				s.Streaming = &transportv1alpha1.TransportStreamingSettings{
					Recording: &transportv1alpha1.TransportRecordingSettings{
						Mode: transportv1alpha1.TransportRecordingMode("full"),
					},
				}
			},
			wantErr:    true,
			wantFields: []string{"spec.streaming.recording.mode"},
		},
		{
			name: "recording sampleRate > 100",
			mutate: func(s *transportv1alpha1.TransportSpec) {
				s.Streaming = &transportv1alpha1.TransportStreamingSettings{
					Recording: &transportv1alpha1.TransportRecordingSettings{
						SampleRate: new(int32(200)),
					},
				}
			},
			wantErr:    true,
			wantFields: []string{"spec.streaming.recording.sampleRate"},
		},
		{
			name: "negative retentionSeconds",
			mutate: func(s *transportv1alpha1.TransportSpec) {
				s.Streaming = &transportv1alpha1.TransportStreamingSettings{
					Recording: &transportv1alpha1.TransportRecordingSettings{
						RetentionSeconds: new(int32(-1)),
					},
				}
			},
			wantErr:    true,
			wantFields: []string{"spec.streaming.recording.retentionSeconds"},
		},
		{
			name: "blank redactFields entry",
			mutate: func(s *transportv1alpha1.TransportSpec) {
				s.Streaming = &transportv1alpha1.TransportStreamingSettings{
					Recording: &transportv1alpha1.TransportRecordingSettings{
						RedactFields: []string{"", "payload.api_key"},
					},
				}
			},
			wantErr:    true,
			wantFields: []string{"spec.streaming.recording.redactFields[0]"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			spec := base
			tc.mutate(&spec)
			tr := &transportv1alpha1.Transport{
				ObjectMeta: metav1.ObjectMeta{Name: "tr"},
				Spec:       spec,
			}
			err := tv.validateTransport(tr)
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected validation error, got nil")
				}
				if !apierrors.IsInvalid(err) {
					t.Fatalf("expected apierrors.Invalid, got %v", err)
				}
				got := causeFields(err)
				for _, wf := range tc.wantFields {
					if !containsField(got, wf) {
						t.Errorf("expected field %q in error causes, got %v", wf, got)
					}
				}
			} else {
				if err != nil {
					t.Fatalf("expected no error, got %v", err)
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// TransportBindingValidator — ValidateCreate
// ---------------------------------------------------------------------------

func TestTransportBindingValidatorValidateCreate_Valid(t *testing.T) {
	transport := validTransport("livekit")
	validator := &TransportBindingValidator{
		Reader: fake.NewClientBuilder().WithScheme(newTransportScheme(t)).WithObjects(transport).Build(),
	}
	_, err := validator.ValidateCreate(context.Background(), validBinding())
	if err != nil {
		t.Fatalf("expected no error for valid binding, got %v", err)
	}
}

// validate() trims whitespace from transportRef before the Transport lookup, so a
// padded ref like "  livekit  " must resolve the same transport as "livekit".
func TestTransportBindingValidatorValidateCreate_TrimmedTransportRef(t *testing.T) {
	transport := validTransport("livekit")
	validator := &TransportBindingValidator{
		Reader: fake.NewClientBuilder().WithScheme(newTransportScheme(t)).WithObjects(transport).Build(),
	}
	b := validBinding()
	b.Spec.TransportRef = "  livekit  " // whitespace-padded; TrimSpace normalises to "livekit"
	_, err := validator.ValidateCreate(context.Background(), b)
	if err != nil {
		t.Fatalf("expected no error for whitespace-padded transportRef, got %v", err)
	}
}

func TestTransportBindingValidatorValidateCreate_RejectsPlaintextRawSecurityMode(t *testing.T) {
	transport := validTransport("livekit")
	validator := &TransportBindingValidator{
		Reader: fake.NewClientBuilder().WithScheme(newTransportScheme(t)).WithObjects(transport).Build(),
	}
	b := validBinding()
	b.Spec.RawSettings = &runtime.RawExtension{
		Raw: []byte(`{"env":{"BUBU_TRANSPORT_SECURITY_MODE":"plaintext"}}`),
	}

	_, err := validator.ValidateCreate(context.Background(), b)
	if err == nil {
		t.Fatal("expected validation error for plaintext raw security mode")
	}
	if !apierrors.IsInvalid(err) {
		t.Fatalf("expected apierrors.Invalid, got %v", err)
	}
	fields := causeFields(err)
	if !containsField(fields, "spec.rawSettings.env.BUBU_TRANSPORT_SECURITY_MODE") {
		t.Errorf("expected spec.rawSettings.env.BUBU_TRANSPORT_SECURITY_MODE in causes, got %v", fields)
	}
}

// ---------------------------------------------------------------------------
// TransportBindingValidator — ValidateUpdate
// ---------------------------------------------------------------------------

func TestTransportBindingValidatorValidateUpdate_SpecUnchanged_Skips(t *testing.T) {
	validator := &TransportBindingValidator{
		// Reader must not be hit when spec is unchanged; using an empty store to make
		// any accidental lookup fail loudly.
		Reader: fake.NewClientBuilder().WithScheme(newTransportScheme(t)).Build(),
	}
	b := validBinding()
	// Use DeepCopy so the equality check compares two distinct objects rather than
	// trivially passing because both arguments are the same pointer.
	_, err := validator.ValidateUpdate(context.Background(), b, b.DeepCopy())
	if err != nil {
		t.Fatalf("expected no error for spec-equal update, got %v", err)
	}
}

func TestTransportBindingValidatorValidateUpdate_DeletionTimestamp_Skips(t *testing.T) {
	validator := &TransportBindingValidator{
		Reader: fake.NewClientBuilder().WithScheme(newTransportScheme(t)).Build(),
	}
	now := metav1.Now()
	// Invalid spec (no lanes) but DeletionTimestamp is set — must skip validation.
	newB := &transportv1alpha1.TransportBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "b",
			Namespace:         "default",
			DeletionTimestamp: &now,
		},
		Spec: transportv1alpha1.TransportBindingSpec{
			TransportRef: "livekit",
			Driver:       "bobravoz-grpc",
			StepName:     "step-a",
			EngramName:   "engram-a",
		},
	}
	old := validBinding()
	_, err := validator.ValidateUpdate(context.Background(), old, newB)
	if err != nil {
		t.Fatalf("expected no error when DeletionTimestamp set, got %v", err)
	}
}

// transportRef is immutable after creation.
func TestTransportBindingValidatorValidateUpdate_ImmutableTransportRef(t *testing.T) {
	validator := &TransportBindingValidator{
		Reader: fake.NewClientBuilder().WithScheme(newTransportScheme(t)).Build(),
	}
	old := validBinding()
	newB := old.DeepCopy()
	newB.Spec.TransportRef = "different-transport"

	_, err := validator.ValidateUpdate(context.Background(), old, newB)
	if err == nil {
		t.Fatal("expected error when transportRef is changed")
	}
	if !apierrors.IsInvalid(err) {
		t.Fatalf("expected apierrors.Invalid, got %v", err)
	}
	fields := causeFields(err)
	if !containsField(fields, "spec.transportRef") {
		t.Errorf("expected spec.transportRef in causes, got %v", fields)
	}
}

// driver is immutable after creation.
func TestTransportBindingValidatorValidateUpdate_ImmutableDriver(t *testing.T) {
	validator := &TransportBindingValidator{
		Reader: fake.NewClientBuilder().WithScheme(newTransportScheme(t)).Build(),
	}
	old := validBinding()
	newB := old.DeepCopy()
	newB.Spec.Driver = "changed-driver"

	_, err := validator.ValidateUpdate(context.Background(), old, newB)
	if err == nil {
		t.Fatal("expected error when driver is changed")
	}
	if !apierrors.IsInvalid(err) {
		t.Fatalf("expected apierrors.Invalid, got %v", err)
	}
	fields := causeFields(err)
	if !containsField(fields, "spec.driver") {
		t.Errorf("expected spec.driver in causes, got %v", fields)
	}
}

// Changing both immutable fields simultaneously must produce two field-level causes.
func TestTransportBindingValidatorValidateUpdate_BothImmutableFieldsChanged(t *testing.T) {
	validator := &TransportBindingValidator{
		Reader: fake.NewClientBuilder().WithScheme(newTransportScheme(t)).Build(),
	}
	old := validBinding()
	newB := old.DeepCopy()
	newB.Spec.TransportRef = "different-transport"
	newB.Spec.Driver = "changed-driver"

	_, err := validator.ValidateUpdate(context.Background(), old, newB)
	if err == nil {
		t.Fatal("expected error when both transportRef and driver are changed")
	}
	if !apierrors.IsInvalid(err) {
		t.Fatalf("expected apierrors.Invalid, got %v", err)
	}
	fields := causeFields(err)
	if !containsField(fields, "spec.transportRef") {
		t.Errorf("expected spec.transportRef in causes, got %v", fields)
	}
	if !containsField(fields, "spec.driver") {
		t.Errorf("expected spec.driver in causes, got %v", fields)
	}
}

// Changing a mutable field (direction) must pass through to full validate when the
// new spec is valid — exercises the ValidateUpdate → validate call path.
func TestTransportBindingValidatorValidateUpdate_MutableSpecChange_Valid(t *testing.T) {
	transport := validTransport("livekit")
	validator := &TransportBindingValidator{
		Reader: fake.NewClientBuilder().WithScheme(newTransportScheme(t)).WithObjects(transport).Build(),
	}
	old := validBinding()
	newB := old.DeepCopy()
	// direction is mutable and the change is valid.
	newB.Spec.Audio.Direction = transportv1alpha1.MediaDirectionSubscribe

	_, err := validator.ValidateUpdate(context.Background(), old, newB)
	if err != nil {
		t.Fatalf("expected no error for valid mutable spec change, got %v", err)
	}
}

// Changing a mutable field to an invalid value must trigger full validate and return
// a field error — exercises the ValidateUpdate → validate call path for the failure branch.
func TestTransportBindingValidatorValidateUpdate_MutableSpecChange_Invalid(t *testing.T) {
	transport := validTransport("livekit")
	validator := &TransportBindingValidator{
		Reader: fake.NewClientBuilder().WithScheme(newTransportScheme(t)).WithObjects(transport).Build(),
	}
	old := validBinding()
	newB := old.DeepCopy()
	// pcm16 is not in the transport's SupportedAudio list (only opus is).
	newB.Spec.Audio.Codecs = []transportv1alpha1.AudioCodec{{Name: "pcm16"}}

	_, err := validator.ValidateUpdate(context.Background(), old, newB)
	if err == nil {
		t.Fatal("expected validation error for unsupported codec in update")
	}
	if !apierrors.IsInvalid(err) {
		t.Fatalf("expected apierrors.Invalid, got %v", err)
	}
	fields := causeFields(err)
	if !containsField(fields, "spec.audio.codecs") {
		t.Errorf("expected spec.audio.codecs in causes, got %v", fields)
	}
}

// ---------------------------------------------------------------------------
// TransportBindingValidator — ValidateDelete
// ---------------------------------------------------------------------------

func TestTransportBindingValidatorValidateDelete_AlwaysAllows(t *testing.T) {
	validator := &TransportBindingValidator{
		Reader: fake.NewClientBuilder().WithScheme(newTransportScheme(t)).Build(),
	}
	_, err := validator.ValidateDelete(context.Background(), validBinding())
	if err != nil {
		t.Fatalf("expected no error on delete, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// TransportBindingValidator — validate (internal, detailed coverage)
// ---------------------------------------------------------------------------

// A nil Reader must produce a non-field error, not an apierrors.Invalid.
func TestTransportBindingValidatorNilReader(t *testing.T) {
	v := &TransportBindingValidator{Reader: nil}
	_, err := v.validate(context.Background(), validBinding())
	if err == nil {
		t.Fatal("expected error when Reader is nil")
	}
	if apierrors.IsInvalid(err) {
		t.Fatalf("expected non-field error for nil reader, got IsInvalid: %v", err)
	}
}

func TestTransportBindingValidatorRejectsNoLanes(t *testing.T) {
	transport := validTransport("livekit")
	validator := &TransportBindingValidator{
		Reader: fake.NewClientBuilder().WithScheme(newTransportScheme(t)).WithObjects(transport).Build(),
	}
	binding := &transportv1alpha1.TransportBinding{
		ObjectMeta: metav1.ObjectMeta{Name: "b", Namespace: "default"},
		Spec: transportv1alpha1.TransportBindingSpec{
			TransportRef: "livekit",
			Driver:       "bobravoz-grpc",
			StepName:     "step-a",
			EngramName:   "engram-a",
			// Audio, Video, Binary all nil
		},
	}
	_, err := validator.validate(context.Background(), binding)
	if err == nil {
		t.Fatal("expected validation error when no lanes specified")
	}
	if !apierrors.IsInvalid(err) {
		t.Fatalf("expected apierrors.Invalid, got %v", err)
	}
	fields := causeFields(err)
	if !containsField(fields, "spec") {
		t.Errorf("expected spec in causes, got %v", fields)
	}
}

func TestTransportBindingValidatorRejectsEmptyTransportRef(t *testing.T) {
	validator := &TransportBindingValidator{
		Reader: fake.NewClientBuilder().WithScheme(newTransportScheme(t)).Build(),
	}
	binding := &transportv1alpha1.TransportBinding{
		ObjectMeta: metav1.ObjectMeta{Name: "b", Namespace: "default"},
		Spec: transportv1alpha1.TransportBindingSpec{
			TransportRef: "   ", // blank after TrimSpace
			Driver:       "bobravoz-grpc",
			StepName:     "step-a",
			EngramName:   "engram-a",
			Audio: &transportv1alpha1.AudioBinding{
				Direction: transportv1alpha1.MediaDirectionBidirectional,
			},
		},
	}
	_, err := validator.validate(context.Background(), binding)
	if err == nil {
		t.Fatal("expected validation error for blank transportRef")
	}
	if !apierrors.IsInvalid(err) {
		t.Fatalf("expected apierrors.Invalid, got %v", err)
	}
	fields := causeFields(err)
	if !containsField(fields, "spec.transportRef") {
		t.Errorf("expected spec.transportRef in causes, got %v", fields)
	}
}

// A reference to a non-existent Transport must produce a NotFound field error.
func TestTransportBindingValidatorRejectsMissingTransport(t *testing.T) {
	validator := &TransportBindingValidator{
		Reader: fake.NewClientBuilder().WithScheme(newTransportScheme(t)).Build(),
	}
	binding := &transportv1alpha1.TransportBinding{
		ObjectMeta: metav1.ObjectMeta{Name: "binding", Namespace: "default"},
		Spec: transportv1alpha1.TransportBindingSpec{
			TransportRef: "missing",
			Driver:       "bobravoz-grpc",
			StepName:     "step-a",
			EngramName:   "engram-a",
			Audio: &transportv1alpha1.AudioBinding{
				Direction: transportv1alpha1.MediaDirectionBidirectional,
				Codecs:    []transportv1alpha1.AudioCodec{{Name: "opus"}},
			},
		},
	}
	_, err := validator.validate(context.Background(), binding)
	if err == nil {
		t.Fatal("expected validation error for missing transport")
	}
	if !apierrors.IsInvalid(err) {
		t.Fatalf("expected apierrors.Invalid, got %v", err)
	}
	fields := causeFields(err)
	if !containsField(fields, "spec.transportRef") {
		t.Errorf("expected spec.transportRef in causes, got %v", fields)
	}
}

// A transient API error must surface as a non-field error (not IsInvalid) and must
// preserve the original error via wrapping so callers can inspect the root cause.
func TestTransportBindingValidatorAPIError(t *testing.T) {
	transientErr := errors.New("etcd unavailable")
	validator := &TransportBindingValidator{
		Reader: &failingReader{err: transientErr},
	}
	_, err := validator.validate(context.Background(), validBinding())
	if err == nil {
		t.Fatal("expected error for transient API failure")
	}
	if apierrors.IsInvalid(err) {
		t.Fatal("transient API error must not be surfaced as apierrors.Invalid")
	}
	if !errors.Is(err, transientErr) {
		t.Errorf("expected wrapped transient error, got %v", err)
	}
}

// When field errors are already accumulated before the Transport lookup and the lookup
// fails with a transient error, the field errors must be returned (not discarded) so
// that the user receives actionable feedback immediately.
func TestTransportBindingValidatorFieldErrorsPersistThroughAPIError(t *testing.T) {
	transientErr := errors.New("etcd unavailable")
	validator := &TransportBindingValidator{
		Reader: &failingReader{err: transientErr},
	}
	// No lanes — a deterministic field error that exists before the lookup attempt.
	binding := &transportv1alpha1.TransportBinding{
		ObjectMeta: metav1.ObjectMeta{Name: "b", Namespace: "default"},
		Spec: transportv1alpha1.TransportBindingSpec{
			TransportRef: "livekit",
			Driver:       "bobravoz-grpc",
			StepName:     "step-a",
			EngramName:   "engram-a",
			// Audio, Video, Binary all nil — no lanes
		},
	}
	_, err := validator.validate(context.Background(), binding)
	if err == nil {
		t.Fatal("expected validation error")
	}
	// Must be surfaced as a field error (apierrors.Invalid), not the raw transient error,
	// because the no-lanes error was collected before the lookup was attempted.
	if !apierrors.IsInvalid(err) {
		t.Fatalf("expected apierrors.Invalid (field errors must survive transient API error), got %v", err)
	}
	if errors.Is(err, transientErr) {
		t.Error("transient API error must not be propagated when prior field errors exist")
	}
	fields := causeFields(err)
	if !containsField(fields, "spec") {
		t.Errorf("expected spec in error causes, got %v", fields)
	}
}

// A context cancellation during the Transport lookup must surface as a non-field error
// so the admission control framework can treat it as a retriable infrastructure failure.
func TestTransportBindingValidatorTransportLookupCancelled(t *testing.T) {
	validator := &TransportBindingValidator{Reader: &slowReader{}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately so the blocked lookup returns at once
	_, err := validator.validate(ctx, validBinding())
	if err == nil {
		t.Fatal("expected error when context is cancelled during transport lookup")
	}
	if apierrors.IsInvalid(err) {
		t.Fatalf("context cancellation error must not be surfaced as apierrors.Invalid, got %v", err)
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected wrapped context.Canceled in error chain, got %v", err)
	}
}

func TestTransportBindingValidatorRejectsMismatchedDriver(t *testing.T) {
	transport := validTransport("livekit")
	validator := &TransportBindingValidator{
		Reader: fake.NewClientBuilder().WithScheme(newTransportScheme(t)).WithObjects(transport).Build(),
	}
	binding := &transportv1alpha1.TransportBinding{
		ObjectMeta: metav1.ObjectMeta{Name: "binding", Namespace: "default"},
		Spec: transportv1alpha1.TransportBindingSpec{
			TransportRef: "livekit",
			Driver:       "different",
			StepName:     "step-a",
			EngramName:   "engram-a",
			Audio: &transportv1alpha1.AudioBinding{
				Direction: transportv1alpha1.MediaDirectionBidirectional,
				Codecs:    []transportv1alpha1.AudioCodec{{Name: "opus"}},
			},
		},
	}
	_, err := validator.validate(context.Background(), binding)
	if err == nil {
		t.Fatal("expected validation error for mismatched driver")
	}
	if !apierrors.IsInvalid(err) {
		t.Fatalf("expected apierrors.Invalid, got %v", err)
	}
	fields := causeFields(err)
	if !containsField(fields, "spec.driver") {
		t.Errorf("expected spec.driver in causes, got %v", fields)
	}
}

func TestTransportBindingValidatorRejectsDriverCaseMismatch(t *testing.T) {
	transport := validTransport("livekit")
	validator := &TransportBindingValidator{
		Reader: fake.NewClientBuilder().WithScheme(newTransportScheme(t)).WithObjects(transport).Build(),
	}
	binding := &transportv1alpha1.TransportBinding{
		ObjectMeta: metav1.ObjectMeta{Name: "binding", Namespace: "default"},
		Spec: transportv1alpha1.TransportBindingSpec{
			TransportRef: "livekit",
			Driver:       "Bobravoz-grpc", // case mismatch — driver comparison is case-sensitive by design
			StepName:     "step-a",
			EngramName:   "engram-a",
			Audio: &transportv1alpha1.AudioBinding{
				Direction: transportv1alpha1.MediaDirectionBidirectional,
				Codecs:    []transportv1alpha1.AudioCodec{{Name: "opus"}},
			},
		},
	}
	_, err := validator.validate(context.Background(), binding)
	if err == nil {
		t.Fatal("expected validation error for driver case mismatch")
	}
	if !apierrors.IsInvalid(err) {
		t.Fatalf("expected apierrors.Invalid, got %v", err)
	}
}

func TestTransportBindingValidatorRejectsUnsupportedCodec(t *testing.T) {
	transport := validTransport("livekit")
	validator := &TransportBindingValidator{
		Reader: fake.NewClientBuilder().WithScheme(newTransportScheme(t)).WithObjects(transport).Build(),
	}
	binding := &transportv1alpha1.TransportBinding{
		ObjectMeta: metav1.ObjectMeta{Name: "binding", Namespace: "default"},
		Spec: transportv1alpha1.TransportBindingSpec{
			TransportRef: "livekit",
			Driver:       "bobravoz-grpc",
			StepName:     "step-a",
			EngramName:   "engram-a",
			Audio: &transportv1alpha1.AudioBinding{
				Direction: transportv1alpha1.MediaDirectionBidirectional,
				Codecs:    []transportv1alpha1.AudioCodec{{Name: "pcm16"}},
			},
		},
	}
	_, err := validator.validate(context.Background(), binding)
	if err == nil {
		t.Fatal("expected validation error for unsupported codec")
	}
	if !apierrors.IsInvalid(err) {
		t.Fatalf("expected apierrors.Invalid, got %v", err)
	}
	// Error must be anchored at spec.audio.codecs, not at the spec root.
	fields := causeFields(err)
	if !containsField(fields, "spec.audio.codecs") {
		t.Errorf("expected spec.audio.codecs in causes, got %v", fields)
	}
}

func TestTransportBindingValidatorRejectsUnsupportedVideoCodec(t *testing.T) {
	transport := &transportv1alpha1.Transport{
		ObjectMeta: metav1.ObjectMeta{Name: "livekit"},
		Spec: transportv1alpha1.TransportSpec{
			Provider:       "livekit",
			Driver:         "bobravoz-grpc",
			SupportedVideo: []transportv1alpha1.VideoCodec{{Name: "h264"}},
		},
	}
	validator := &TransportBindingValidator{
		Reader: fake.NewClientBuilder().WithScheme(newTransportScheme(t)).WithObjects(transport).Build(),
	}
	binding := &transportv1alpha1.TransportBinding{
		ObjectMeta: metav1.ObjectMeta{Name: "binding", Namespace: "default"},
		Spec: transportv1alpha1.TransportBindingSpec{
			TransportRef: "livekit",
			Driver:       "bobravoz-grpc",
			StepName:     "step-a",
			EngramName:   "engram-a",
			Video: &transportv1alpha1.VideoBinding{
				Direction: transportv1alpha1.MediaDirectionBidirectional,
				Codecs:    []transportv1alpha1.VideoCodec{{Name: "vp8"}}, // not in SupportedVideo
			},
		},
	}
	_, err := validator.validate(context.Background(), binding)
	if err == nil {
		t.Fatal("expected validation error for unsupported video codec")
	}
	if !apierrors.IsInvalid(err) {
		t.Fatalf("expected apierrors.Invalid, got %v", err)
	}
	fields := causeFields(err)
	if !containsField(fields, "spec.video.codecs") {
		t.Errorf("expected spec.video.codecs in causes, got %v", fields)
	}
}

func TestTransportBindingValidatorRejectsUnsupportedBinaryMimeType(t *testing.T) {
	transport := &transportv1alpha1.Transport{
		ObjectMeta: metav1.ObjectMeta{Name: "livekit"},
		Spec: transportv1alpha1.TransportSpec{
			Provider:        "livekit",
			Driver:          "bobravoz-grpc",
			SupportedBinary: []string{"application/json"},
		},
	}
	validator := &TransportBindingValidator{
		Reader: fake.NewClientBuilder().WithScheme(newTransportScheme(t)).WithObjects(transport).Build(),
	}
	binding := &transportv1alpha1.TransportBinding{
		ObjectMeta: metav1.ObjectMeta{Name: "binding", Namespace: "default"},
		Spec: transportv1alpha1.TransportBindingSpec{
			TransportRef: "livekit",
			Driver:       "bobravoz-grpc",
			StepName:     "step-a",
			EngramName:   "engram-a",
			Binary: &transportv1alpha1.BinaryBinding{
				Direction: transportv1alpha1.MediaDirectionBidirectional,
				MimeTypes: []string{"application/octet-stream"}, // not in SupportedBinary
			},
		},
	}
	_, err := validator.validate(context.Background(), binding)
	if err == nil {
		t.Fatal("expected validation error for unsupported binary MIME type")
	}
	if !apierrors.IsInvalid(err) {
		t.Fatalf("expected apierrors.Invalid, got %v", err)
	}
	fields := causeFields(err)
	if !containsField(fields, "spec.binary.mimeTypes") {
		t.Errorf("expected spec.binary.mimeTypes in causes, got %v", fields)
	}
}

// ---------------------------------------------------------------------------
// Test utilities
// ---------------------------------------------------------------------------

func containsField(fields []string, target string) bool {
	return slices.Contains(fields, target)
}
