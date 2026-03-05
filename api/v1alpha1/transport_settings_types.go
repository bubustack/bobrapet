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

// BufferDropPolicy defines how the hub behaves when buffers overflow.
// +kubebuilder:validation:Enum=drop_newest;drop_oldest
type BufferDropPolicy string

const (
	BufferDropNewest BufferDropPolicy = "drop_newest"
	BufferDropOldest BufferDropPolicy = "drop_oldest"
)

// FlowControlMode selects the flow-control strategy for a transport.
// +kubebuilder:validation:Enum=none;credits;window
type FlowControlMode string

const (
	FlowControlNone    FlowControlMode = "none"
	FlowControlCredits FlowControlMode = "credits"
	FlowControlWindow  FlowControlMode = "window"
)

// OrderingMode defines ordering guarantees for streaming delivery.
// +kubebuilder:validation:Enum=none;per_stream;per_partition
type OrderingMode string

const (
	OrderingNone         OrderingMode = "none"
	OrderingPerStream    OrderingMode = "per_stream"
	OrderingPerPartition OrderingMode = "per_partition"
)

// DeliverySemantics defines the delivery guarantee.
// +kubebuilder:validation:Enum=best_effort;at_least_once
type DeliverySemantics string

const (
	DeliveryBestEffort  DeliverySemantics = "best_effort"
	DeliveryAtLeastOnce DeliverySemantics = "at_least_once"
)

// ReplayMode configures stream replay behavior.
// +kubebuilder:validation:Enum=none;memory;durable
type ReplayMode string

const (
	ReplayNone    ReplayMode = "none"
	ReplayMemory  ReplayMode = "memory"
	ReplayDurable ReplayMode = "durable"
)

// TransportStreamingSettings configures streaming policies for a Story transport.
type TransportStreamingSettings struct {
	// Backpressure configures buffer limits and overflow behavior.
	// +optional
	Backpressure *TransportBackpressureSettings `json:"backpressure,omitempty"`

	// FlowControl configures credit/window-based flow control.
	// +optional
	FlowControl *TransportFlowControlSettings `json:"flowControl,omitempty"`

	// Delivery configures ordering, delivery semantics, and replay.
	// +optional
	Delivery *TransportDeliverySettings `json:"delivery,omitempty"`

	// Routing configures topology selection and fan-out policy.
	// +optional
	Routing *TransportRoutingSettings `json:"routing,omitempty"`

	// Lanes declares named streaming lanes (audio/video/binary/payload) and per-lane caps.
	// +optional
	Lanes []TransportLane `json:"lanes,omitempty"`

	// FanIn configures how multiple upstream steps are joined at fan-in points.
	// +optional
	FanIn *TransportFanInSettings `json:"fanIn,omitempty"`

	// Partitioning configures partition assignment for streaming envelopes.
	// +optional
	Partitioning *TransportPartitioningSettings `json:"partitioning,omitempty"`

	// Lifecycle configures drain/cutover and in-flight guardrails.
	// +optional
	Lifecycle *TransportLifecycleSettings `json:"lifecycle,omitempty"`

	// Observability configures metrics, tracing, and watermark tracking.
	// +optional
	Observability *TransportObservabilitySettings `json:"observability,omitempty"`

	// Recording configures optional stream recording/sampling behavior.
	// +optional
	Recording *TransportRecordingSettings `json:"recording,omitempty"`
}

// TransportLaneKind declares the media or payload lane.
// +kubebuilder:validation:Enum=audio;video;binary;payload;control
type TransportLaneKind string

const (
	TransportLaneAudio   TransportLaneKind = "audio"
	TransportLaneVideo   TransportLaneKind = "video"
	TransportLaneBinary  TransportLaneKind = "binary"
	TransportLanePayload TransportLaneKind = "payload"
	TransportLaneControl TransportLaneKind = "control"
)

// TransportLaneDirection declares the lane direction.
// +kubebuilder:validation:Enum=publish;subscribe;bidirectional
type TransportLaneDirection string

const (
	TransportLanePublish       TransportLaneDirection = "publish"
	TransportLaneSubscribe     TransportLaneDirection = "subscribe"
	TransportLaneBidirectional TransportLaneDirection = "bidirectional"
)

// TransportLane declares a named lane and optional per-lane caps.
type TransportLane struct {
	// Name identifies the lane (e.g., "audio", "telemetry").
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:Pattern=^[a-z0-9]([-a-z0-9]*[a-z0-9])?$
	Name string `json:"name"`

	// Kind declares the lane media/payload type.
	// +kubebuilder:validation:Required
	Kind TransportLaneKind `json:"kind"`

	// Direction declares whether the lane is publish/subscribe/bidirectional.
	// +optional
	Direction TransportLaneDirection `json:"direction,omitempty"`

	// Description provides optional human-readable context.
	// +optional
	Description string `json:"description,omitempty"`

	// MaxMessages caps buffered messages for this lane (applies per downstream).
	// +optional
	// +kubebuilder:validation:Minimum=0
	MaxMessages *int32 `json:"maxMessages,omitempty"`

	// MaxBytes caps buffered bytes for this lane (applies per downstream).
	// +optional
	// +kubebuilder:validation:Minimum=0
	MaxBytes *int32 `json:"maxBytes,omitempty"`
}

// TransportFanInMode selects how fan-in joins are resolved.
// +kubebuilder:validation:Enum=all;any;quorum
type TransportFanInMode string

const (
	TransportFanInAll    TransportFanInMode = "all"
	TransportFanInAny    TransportFanInMode = "any"
	TransportFanInQuorum TransportFanInMode = "quorum"
)

// TransportFanInSettings configures join behavior when multiple upstreams feed a step.
type TransportFanInSettings struct {
	// Mode controls whether all, any, or a quorum of upstreams are required.
	// +optional
	Mode TransportFanInMode `json:"mode,omitempty"`

	// Quorum sets the minimum number of upstreams required when Mode=quorum.
	// +optional
	// +kubebuilder:validation:Minimum=1
	Quorum *int32 `json:"quorum,omitempty"`

	// TimeoutSeconds caps how long fan-in will wait before giving up.
	// +optional
	// +kubebuilder:validation:Minimum=0
	TimeoutSeconds *int32 `json:"timeoutSeconds,omitempty"`

	// MaxEntries caps the number of concurrent fan-in joins retained in memory.
	// +optional
	// +kubebuilder:validation:Minimum=1
	MaxEntries *int32 `json:"maxEntries,omitempty"`
}

// TransportBackpressureSettings holds buffer configuration.
type TransportBackpressureSettings struct {
	// Buffer configures queue limits and drop policy.
	// +optional
	Buffer *TransportBufferSettings `json:"buffer,omitempty"`
}

// TransportBufferSettings defines buffer limits.
type TransportBufferSettings struct {
	// MaxMessages caps the number of buffered messages.
	// +optional
	// +kubebuilder:validation:Minimum=0
	MaxMessages *int32 `json:"maxMessages,omitempty"`

	// MaxBytes caps the total bytes buffered.
	// +optional
	// +kubebuilder:validation:Minimum=0
	MaxBytes *int32 `json:"maxBytes,omitempty"`

	// MaxAgeSeconds caps the time a message can remain buffered.
	// +optional
	// +kubebuilder:validation:Minimum=0
	MaxAgeSeconds *int32 `json:"maxAgeSeconds,omitempty"`

	// DropPolicy selects which messages to drop when the buffer is full.
	// +optional
	DropPolicy BufferDropPolicy `json:"dropPolicy,omitempty"`
}

// TransportFlowControlSettings configures flow control and ack pacing.
type TransportFlowControlSettings struct {
	// Mode selects the flow-control strategy.
	// +optional
	Mode FlowControlMode `json:"mode,omitempty"`

	// InitialCredits sets the initial credit window for credits/window mode.
	// +optional
	InitialCredits *TransportFlowCredits `json:"initialCredits,omitempty"`

	// AckEvery controls how frequently acks/credits are emitted.
	// +optional
	AckEvery *TransportFlowAckSettings `json:"ackEvery,omitempty"`

	// PauseThreshold sets the buffer utilization (0-1) that pauses upstream.
	// +optional
	PauseThreshold *TransportFlowThreshold `json:"pauseThreshold,omitempty"`

	// ResumeThreshold sets the buffer utilization (0-1) that resumes upstream.
	// +optional
	ResumeThreshold *TransportFlowThreshold `json:"resumeThreshold,omitempty"`
}

// TransportFlowCredits describes message/byte credits.
type TransportFlowCredits struct {
	// Messages is the credit window in messages.
	// +optional
	// +kubebuilder:validation:Minimum=0
	Messages *int32 `json:"messages,omitempty"`

	// Bytes is the credit window in bytes.
	// +optional
	// +kubebuilder:validation:Minimum=0
	Bytes *int32 `json:"bytes,omitempty"`
}

// TransportFlowAckSettings defines ack pacing.
type TransportFlowAckSettings struct {
	// Messages is the ack interval in messages.
	// +optional
	// +kubebuilder:validation:Minimum=0
	Messages *int32 `json:"messages,omitempty"`

	// Bytes is the ack interval in bytes.
	// +optional
	// +kubebuilder:validation:Minimum=0
	Bytes *int32 `json:"bytes,omitempty"`

	// MaxDelay caps the time between ack/credit emissions (duration string, e.g., "250ms").
	// +optional
	MaxDelay *string `json:"maxDelay,omitempty"`
}

// TransportFlowThreshold defines a buffer utilization threshold.
type TransportFlowThreshold struct {
	// BufferPct defines the buffer utilization threshold as a percent (0-100).
	// +optional
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=100
	BufferPct *int32 `json:"bufferPct,omitempty"`
}

// TransportDeliverySettings configures ordering, semantics, and replay.
type TransportDeliverySettings struct {
	// Ordering controls ordering guarantees.
	// +optional
	Ordering OrderingMode `json:"ordering,omitempty"`

	// Semantics controls delivery semantics.
	// +optional
	Semantics DeliverySemantics `json:"semantics,omitempty"`

	// Replay configures replay/retention.
	// +optional
	Replay *TransportReplaySettings `json:"replay,omitempty"`
}

// TransportReplaySettings configures replay storage/retention.
type TransportReplaySettings struct {
	// Mode selects replay storage mode.
	// +optional
	Mode ReplayMode `json:"mode,omitempty"`

	// RetentionSeconds sets how long replay data is retained.
	// +optional
	// +kubebuilder:validation:Minimum=0
	RetentionSeconds *int32 `json:"retentionSeconds,omitempty"`

	// CheckpointInterval sets how frequently checkpoints are persisted (duration string, e.g., "1s").
	// +optional
	CheckpointInterval *string `json:"checkpointInterval,omitempty"`
}

// TransportRoutingMode selects how steps connect to each other.
// +kubebuilder:validation:Enum=auto;hub;p2p
type TransportRoutingMode string

const (
	TransportRoutingAuto TransportRoutingMode = "auto"
	TransportRoutingHub  TransportRoutingMode = "hub"
	TransportRoutingP2P  TransportRoutingMode = "p2p"
)

// TransportFanOutMode controls how multiple downstreams are dispatched.
// +kubebuilder:validation:Enum=sequential;parallel
type TransportFanOutMode string

const (
	TransportFanOutSequential TransportFanOutMode = "sequential"
	TransportFanOutParallel   TransportFanOutMode = "parallel"
)

// TransportRoutingRuleAction selects how a routing rule affects downstream selection.
// +kubebuilder:validation:Enum=allow;deny
type TransportRoutingRuleAction string

const (
	TransportRoutingRuleAllow TransportRoutingRuleAction = "allow"
	TransportRoutingRuleDeny  TransportRoutingRuleAction = "deny"
)

// TransportRoutingRuleTarget selects downstream steps for a routing rule.
type TransportRoutingRuleTarget struct {
	// Steps lists downstream step names that the rule applies to. Empty means all downstream steps.
	// +optional
	Steps []string `json:"steps,omitempty"`
}

// TransportRoutingRule defines conditional routing behavior for downstream steps.
type TransportRoutingRule struct {
	// Name is an optional identifier for observability/debugging.
	// +optional
	Name string `json:"name,omitempty"`

	// When is a Go template expression evaluated against the streaming runtime scope.
	// +optional
	When *string `json:"when,omitempty"`

	// Action controls whether matching targets are allowed or denied.
	// +optional
	Action TransportRoutingRuleAction `json:"action,omitempty"`

	// Target selects which downstream steps this rule applies to.
	// +optional
	Target *TransportRoutingRuleTarget `json:"target,omitempty"`
}

// TransportRoutingSettings configures P2P vs hub and fan-out policies.
type TransportRoutingSettings struct {
	// Mode selects the routing topology (auto chooses based on DAG analysis).
	// +optional
	Mode TransportRoutingMode `json:"mode,omitempty"`

	// FanOut controls dispatch when multiple downstream steps consume a stream.
	// +optional
	FanOut TransportFanOutMode `json:"fanOut,omitempty"`

	// MaxDownstreams caps the number of downstream targets per step (guardrail).
	// +optional
	// +kubebuilder:validation:Minimum=1
	MaxDownstreams *int32 `json:"maxDownstreams,omitempty"`

	// Rules define conditional routing filters evaluated per packet.
	// +optional
	Rules []TransportRoutingRule `json:"rules,omitempty"`
}

// TransportPartitionMode configures how partitions are assigned.
// +kubebuilder:validation:Enum=none;preserve;hash
type TransportPartitionMode string

const (
	TransportPartitionNone     TransportPartitionMode = "none"
	TransportPartitionPreserve TransportPartitionMode = "preserve"
	TransportPartitionHash     TransportPartitionMode = "hash"
)

// TransportPartitioningSettings configures partition assignment for stream envelopes.
type TransportPartitioningSettings struct {
	// Mode selects how to assign partition IDs.
	// +optional
	Mode TransportPartitionMode `json:"mode,omitempty"`

	// Key selects the value used for hash partitioning (e.g., "metadata.user_id" or "payload.session").
	// +optional
	Key *string `json:"key,omitempty"`

	// Partitions sets the total number of partitions used for hash mode.
	// +optional
	// +kubebuilder:validation:Minimum=1
	Partitions *int32 `json:"partitions,omitempty"`

	// Sticky keeps an existing partition assignment if present.
	// +optional
	Sticky *bool `json:"sticky,omitempty"`
}

// TransportUpgradeStrategy defines how streaming upgrades are performed.
// +kubebuilder:validation:Enum=rolling;drain_cutover;blue_green
type TransportUpgradeStrategy string

const (
	TransportUpgradeRolling      TransportUpgradeStrategy = "rolling"
	TransportUpgradeDrainCutover TransportUpgradeStrategy = "drain_cutover"
	TransportUpgradeBlueGreen    TransportUpgradeStrategy = "blue_green"
)

// TransportLifecycleSettings configures drain/cutover behavior for streaming steps.
type TransportLifecycleSettings struct {
	// Strategy controls upgrade behavior for streaming workloads.
	// +optional
	Strategy TransportUpgradeStrategy `json:"strategy,omitempty"`

	// DrainTimeoutSeconds caps how long to drain in-flight messages before cutover.
	// +optional
	// +kubebuilder:validation:Minimum=0
	DrainTimeoutSeconds *int32 `json:"drainTimeoutSeconds,omitempty"`

	// MaxInFlight caps the number of in-flight messages allowed during upgrades.
	// +optional
	// +kubebuilder:validation:Minimum=0
	MaxInFlight *int32 `json:"maxInFlight,omitempty"`
}

// TransportObservabilitySettings configures metrics, tracing, and watermarks.
type TransportObservabilitySettings struct {
	// Metrics controls per-lane and hub-level metrics emission.
	// +optional
	Metrics *TransportMetricsSettings `json:"metrics,omitempty"`

	// Tracing controls distributed tracing hooks.
	// +optional
	Tracing *TransportTracingSettings `json:"tracing,omitempty"`

	// Watermark controls event-time watermark/lag tracking.
	// +optional
	Watermark *TransportWatermarkSettings `json:"watermark,omitempty"`
}

// TransportMetricsSettings toggles metric emission.
type TransportMetricsSettings struct {
	// Enabled toggles metrics emission (default true).
	// +optional
	Enabled *bool `json:"enabled,omitempty"`
}

// TransportTracingSettings controls tracing options.
type TransportTracingSettings struct {
	// Enabled toggles tracing emission (default true).
	// +optional
	Enabled *bool `json:"enabled,omitempty"`

	// SampleRate controls sampling percentage (0..100).
	// +optional
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=100
	SampleRate *int32 `json:"sampleRate,omitempty"`

	// SamplePolicy selects the sampling policy (e.g., "rate", "rules").
	// +optional
	SamplePolicy *string `json:"samplePolicy,omitempty"`
}

// TransportWatermarkSettings configures event-time watermark tracking.
type TransportWatermarkSettings struct {
	// Enabled toggles watermark and lag tracking.
	// +optional
	Enabled *bool `json:"enabled,omitempty"`

	// TimestampSource selects the source for event-time timestamps (e.g., "metadata.event_time_ms").
	// +optional
	TimestampSource *string `json:"timestampSource,omitempty"`
}

// TransportRecordingMode controls whether payloads are recorded.
// +kubebuilder:validation:Enum=off;metadata;payload
type TransportRecordingMode string

const (
	TransportRecordingOff      TransportRecordingMode = "off"
	TransportRecordingMetadata TransportRecordingMode = "metadata"
	TransportRecordingPayload  TransportRecordingMode = "payload"
)

// TransportRecordingSettings configures stream recording behavior.
type TransportRecordingSettings struct {
	// Mode controls whether metadata-only or full payloads are recorded.
	// +optional
	Mode TransportRecordingMode `json:"mode,omitempty"`

	// SampleRate controls recording sampling percentage (0..100).
	// +optional
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=100
	SampleRate *int32 `json:"sampleRate,omitempty"`

	// RetentionSeconds sets how long recordings should be retained.
	// +optional
	// +kubebuilder:validation:Minimum=0
	RetentionSeconds *int32 `json:"retentionSeconds,omitempty"`

	// RedactFields lists JSON field paths to redact (e.g., "payload.api_key").
	// +optional
	RedactFields []string `json:"redactFields,omitempty"`
}
