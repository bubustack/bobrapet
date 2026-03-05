package controller

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	transportv1alpha1 "github.com/bubustack/bobrapet/api/transport/v1alpha1"
	bubuv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/internal/config"
	"github.com/bubustack/bobrapet/pkg/conditions"
	"github.com/bubustack/bobrapet/pkg/enums"
	"github.com/bubustack/bobrapet/pkg/kubeutil"
	"github.com/bubustack/bobrapet/pkg/logging"
	controllermetrics "github.com/bubustack/bobrapet/pkg/metrics"
	"github.com/bubustack/bobrapet/pkg/refs"
	transportpkg "github.com/bubustack/bobrapet/pkg/transport"
	transportvalidation "github.com/bubustack/bobrapet/pkg/transport/validation"
	"github.com/bubustack/core/contracts"
)

// TransportReconciler validates Transport CRDs and tracks usage across Stories.
type TransportReconciler struct {
	config.ControllerDependencies
	Recorder record.EventRecorder
}

// +kubebuilder:rbac:groups=transport.bubustack.io,resources=transports,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=transport.bubustack.io,resources=transports/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=transport.bubustack.io,resources=transportbindings,verbs=get;list;watch;create;patch;update
// +kubebuilder:rbac:groups=transport.bubustack.io,resources=transportbindings/status,verbs=get;patch;update
// +kubebuilder:rbac:groups=bubustack.io,resources=stories,verbs=get;list;watch

// Reconcile validates the Transport spec, gathers per-binding capabilities, and
// patches status so operators see up-to-date usage counts and readiness info.
//
// Behavior:
//   - Fetches the Transport and validates its spec via validateTransport.
//   - Counts Stories referencing the Transport for usage tracking.
//   - Collects codec capabilities and binding health from TransportBindings.
//   - Updates Transport status with validation results, capabilities, and metrics.
//
// Arguments:
//   - ctx context.Context: propagated to all sub-operations.
//   - req ctrl.Request: contains the Transport name (cluster-scoped).
//
// Returns:
//   - ctrl.Result: empty on success.
//   - error: on validation, capability collection, or status update failures.
func (r *TransportReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logging.NewReconcileLogger(ctx, "transport").WithValues("transport", req.Name)

	var transport transportv1alpha1.Transport
	if err := r.Get(ctx, req.NamespacedName, &transport); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Enrich logger with transport context now that we have the object
	log = log.WithTransport(&transport)

	validationErrors := r.validateTransport(&transport)
	usageCount, usageErr := r.countStoriesReferencingTransport(ctx, &transport, log.Logr())
	if usageErr != nil {
		return ctrl.Result{}, usageErr
	}

	availableAudio, availableVideo, availableBinary, readyBindings, totalBindings, bindingErrors, lastHeartbeat, capabilityErr := r.collectAvailableCapabilities(ctx, &transport, log.Logr())
	if capabilityErr != nil {
		log.Error(capabilityErr, "Failed to collect codec capabilities for transport")
		return ctrl.Result{}, capabilityErr
	}

	if err := r.updateStatus(
		ctx,
		&transport,
		validationErrors,
		usageCount,
		availableAudio,
		availableVideo,
		availableBinary,
		readyBindings,
		totalBindings,
		bindingErrors,
		lastHeartbeat,
	); err != nil {
		log.Error(err, "Failed to update Transport status")
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

// validateTransport delegates to the shared pkg/transport validator so
// controller/runtime and admission webhooks enforce identical spec rules.
//
// Behavior:
//   - Returns ["transport resource is nil"] when transport is nil.
//   - Delegates to transportvalidation.ValidateTransport for spec validation.
//   - Converts field.Error results to human-readable strings.
//
// Arguments:
//   - transport *transportv1alpha1.Transport: the Transport to validate.
//
// Returns:
//   - []string: validation error messages, or nil when valid.
func (r *TransportReconciler) validateTransport(transport *transportv1alpha1.Transport) []string {
	if transport == nil {
		return []string{"transport resource is nil"}
	}
	errs := transportvalidation.ValidateTransport(transport)
	if len(errs) == 0 {
		return nil
	}
	messages := make([]string, 0, len(errs))
	for _, fe := range errs {
		messages = append(messages, fe.Error())
	}
	return messages
}

// countStoriesReferencingTransport returns how many Story specs reference the provided
// Transport via the `spec.transports.transportRef` field index.
//
// Arguments:
//   - ctx context.Context: propagated from reconcile for API calls.
//   - transport *transportv1alpha1.Transport: resource whose usage is counted.
//   - logger logr.Logger: receives structured errors if the indexed list fails.
//
// Returns:
//   - int32: number of Stories referencing the transport.
//   - error: bubbling list failures so the caller can surface them upstream.
//
// Side effects:
//   - Issues a field-indexed List via refs.CountByField, so the corresponding index must be registered in setup.
func (r *TransportReconciler) countStoriesReferencingTransport(
	ctx context.Context,
	transport *transportv1alpha1.Transport,
	logger logr.Logger,
) (int32, error) {
	var stories bubuv1alpha1.StoryList
	count, err := refs.CountByField(ctx, r.Client, &stories, contracts.IndexStoryTransportRefs, transport.Name)
	if err != nil {
		logger.Error(err, "Failed to list Stories referencing transport")
		return 0, err
	}
	return int32(count), nil
}

// collectAvailableCapabilities lists every TransportBinding referencing the given
// transport, derives negotiated codecs, enforces heartbeat staleness, and returns
// the aggregates/counters consumed by updateStatus.
//
// Arguments:
//   - ctx context.Context: reconcile context propagated through List/Patch calls.
//   - transport *transportv1alpha1.Transport: transport whose bindings are inspected.
//   - logger logr.Logger: logger used for stale-binding errors.
//
// Returns slices of negotiated audio/video/binary codecs, counts for ready/total bindings,
// binding error messages, the most recent heartbeat time, and any error encountered
// while listing bindings or patching stale ones.
func (r *TransportReconciler) collectAvailableCapabilities(
	ctx context.Context,
	transport *transportv1alpha1.Transport,
	logger logr.Logger,
) ([]transportv1alpha1.AudioCodec, []transportv1alpha1.VideoCodec, []string, int, int, []string, *metav1.Time, error) {
	snapshot, err := transportpkg.AggregateBindings(
		ctx,
		r.Client,
		transport,
		transportpkg.AggregationOptions{
			Logger:           logger,
			HeartbeatTimeout: r.heartbeatTimeout(),
		},
	)
	if err != nil {
		return nil, nil, nil, 0, 0, nil, nil, err
	}
	return snapshot.Audio, snapshot.Video, snapshot.Binary, snapshot.ReadyBindings, snapshot.TotalBindings, snapshot.BindingErrors, snapshot.LastHeartbeat, nil
}

const bindingErrorSummaryLimit = 5

// updateStatus emits Warning events for validation/binding problems, rewrites
// Transport.Status via patch.RetryableStatusPatch, and records the latest binding
// heartbeat/metrics so operators see an up-to-date snapshot of connector health.
//
// Behavior:
//   - Joins validation/binding error strings and records Warning events before mutating status.
//   - Computes pending/failed binding counts, refreshes capability slices, and updates
//     ObservedGeneration/UsageCount inside a retryable status patch.
//   - Sets the Ready condition + ValidationStatus according to validation/binding/pending states
//     and stores the freshest heartbeat timestamp/metrics for observability.
//
// Arguments mirror the precomputed aggregates from reconcile: ctx, target transport,
// validationErrors, usageCount, negotiated capability slices, ready/total binding counts,
// bindingErrors summaries, and an optional lastHeartbeat timestamp.
//
// Returns an error only when the status patch fails so callers can requeue.
func (r *TransportReconciler) updateStatus(
	ctx context.Context,
	transport *transportv1alpha1.Transport,
	validationErrors []string,
	usageCount int32,
	availableAudio []transportv1alpha1.AudioCodec,
	availableVideo []transportv1alpha1.VideoCodec,
	availableBinary []string,
	readyBindings int,
	totalBindings int,
	bindingErrors []string,
	lastHeartbeat *metav1.Time,
) error {
	validationSummary := strings.Join(validationErrors, "; ")
	bindingSummary := summarizeMessages(bindingErrors, bindingErrorSummaryLimit)
	log := logging.NewControllerLogger(ctx, "transport-status").WithTransport(transport)
	if len(validationErrors) > 0 && strings.TrimSpace(validationSummary) != "" {
		kubeutil.RecordEvent(
			r.Recorder,
			log.Logr(),
			transport,
			corev1.EventTypeWarning,
			conditions.ReasonValidationFailed,
			validationSummary,
		)
	}
	if len(bindingErrors) > 0 && strings.TrimSpace(bindingSummary) != "" {
		kubeutil.RecordEvent(
			r.Recorder,
			log.Logr(),
			transport,
			corev1.EventTypeWarning,
			conditions.ReasonTransportFailed,
			bindingSummary,
		)
	}

	failedBindings := uniqueBindingFailures(bindingErrors)
	pendingBindings := max(totalBindings-readyBindings-failedBindings, 0)

	outcome := kubeutil.ComputeValidationOutcome(kubeutil.ValidationParams{
		ValidationErrors:       validationErrors,
		BindingErrors:          bindingErrors,
		BindingSummary:         bindingSummary,
		ReadyBindings:          readyBindings,
		TotalBindings:          totalBindings,
		PendingBindings:        pendingBindings,
		SuccessReason:          conditions.ReasonValidationPassed,
		SuccessMessage:         "transport validated",
		ValidationFailedReason: conditions.ReasonValidationFailed,
		BindingFailedReason:    conditions.ReasonTransportFailed,
		PendingReason:          conditions.ReasonReconciling,
		PendingStatus:          enums.ValidationStatusUnknown,
		PendingMessageFormatter: func(p int) string {
			return fmt.Sprintf("awaiting connector negotiation (%d pending bindings)", p)
		},
	})

	err := kubeutil.RetryableStatusPatch(ctx, r.Client, transport, func(obj client.Object) {
		t := obj.(*transportv1alpha1.Transport)
		t.Status.ObservedGeneration = transport.Generation
		t.Status.UsageCount = usageCount

		cm := conditions.NewConditionManager(transport.Generation)
		if len(availableAudio) == 0 {
			availableAudio = transportpkg.CloneAudioCodecs(transport.Spec.SupportedAudio)
		}
		if len(availableVideo) == 0 {
			availableVideo = transportpkg.CloneVideoCodecs(transport.Spec.SupportedVideo)
		}
		if len(availableBinary) == 0 {
			availableBinary = transportpkg.CloneBinaryCapabilities(transport.Spec.SupportedBinary)
		}
		t.Status.AvailableAudio = transportpkg.CloneAudioCodecs(availableAudio)
		t.Status.AvailableVideo = transportpkg.CloneVideoCodecs(availableVideo)
		t.Status.AvailableBinary = transportpkg.CloneBinaryCapabilities(availableBinary)

		if aggregated := outcome.AggregatedErrors(); len(aggregated) == 0 {
			t.Status.ValidationErrors = nil
		} else {
			t.Status.ValidationErrors = append([]string(nil), aggregated...)
		}

		t.Status.ValidationStatus = outcome.ValidationStatus
		t.Status.PendingBindings = int32(pendingBindings)
		if lastHeartbeat != nil {
			hb := *lastHeartbeat
			t.Status.LastHeartbeatTime = &hb
		} else {
			t.Status.LastHeartbeatTime = nil
		}

		cm.SetCondition(&t.Status.Conditions, conditions.ConditionReady, outcome.ReadyStatus, outcome.ReadyReason, outcome.ReadyMessage)
	})

	if err != nil {
		return err
	}

	controllermetrics.RecordTransportBindingSnapshot(
		transport.Name,
		readyBindings,
		totalBindings,
		pendingBindings,
		failedBindings,
	)
	return nil
}

// heartbeatTimeout reads the operator configuration to determine how long a
// TransportBinding can stay Ready without a new heartbeat before being marked stale.
//
// Behavior:
//   - Reads HeartbeatTimeout from operator config via ConfigResolver.
//   - Falls back to 2 minutes when config is unavailable or zero.
//
// Returns:
//   - time.Duration: the heartbeat timeout for binding staleness checks.
func (r *TransportReconciler) heartbeatTimeout() time.Duration {
	var timeout time.Duration
	if r.ConfigResolver != nil {
		if cfg := r.ConfigResolver.GetOperatorConfig(); cfg != nil {
			timeout = cfg.Controller.TransportController.HeartbeatTimeout
		}
	}
	return kubeutil.PositiveDurationOrDefault(timeout, 2*time.Minute)
}

// summarizeMessages compacts the provided binding or capability messages into a
// semicolon-delimited summary honoring the requested limit.
//
// Arguments:
//   - messages []string: raw message slice that may include whitespace entries.
//   - limit int: maximum number of messages to include; values <= 0 include all.
//
// Returns:
//   - string: trimmed summary or empty string when no non-empty messages exist.
func summarizeMessages(messages []string, limit int) string {
	cleaned := normalizeBindingMessages(messages)
	if len(cleaned) == 0 {
		return ""
	}
	if limit <= 0 || limit >= len(cleaned) {
		return strings.Join(cleaned, "; ")
	}
	parts := cleaned[:limit]
	summary := strings.Join(parts, "; ")
	if len(cleaned) > limit {
		summary = fmt.Sprintf("%s; ... (+%d more)", summary, len(cleaned)-limit)
	}
	return summary
}

// uniqueBindingFailures counts the distinct binding identifiers embedded in the
// provided failure list (entries follow `name: reason` semantics).
//
// Arguments:
//   - bindingErrors []string: messages describing per-binding failures.
//
// Returns:
//   - int: number of unique binding names referenced in the list.
func uniqueBindingFailures(bindingErrors []string) int {
	cleaned := normalizeBindingMessages(bindingErrors)
	if len(cleaned) == 0 {
		return 0
	}
	seen := make(map[string]struct{}, len(cleaned))
	for _, entry := range cleaned {
		name := entry
		if before, _, ok := strings.Cut(entry, ":"); ok {
			name = before
		}
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		seen[name] = struct{}{}
	}
	return len(seen)
}

// normalizeBindingMessages trims whitespace-only entries from a message slice
// and returns the cleaned list.
//
// Arguments:
//   - messages []string: arbitrary binding/capability messages.
//
// Returns:
//   - []string: trimmed, non-empty messages (nil when none remain).
func normalizeBindingMessages(messages []string) []string {
	if len(messages) == 0 {
		return nil
	}
	cleaned := make([]string, 0, len(messages))
	for _, msg := range messages {
		if trimmed := strings.TrimSpace(msg); trimmed != "" {
			cleaned = append(cleaned, trimmed)
		}
	}
	if len(cleaned) == 0 {
		return nil
	}
	return cleaned
}

// SetupWithManager wires the controller into controller-runtime.
//
// Behavior:
//   - Sets MaxConcurrentReconciles to 2 if not specified.
//   - Registers the event recorder for transport-controller.
//   - Watches TransportBindings to requeue parent Transports.
//   - Watches Stories to requeue Transports referenced in their specs.
//
// Arguments:
//   - mgr ctrl.Manager: the controller-runtime manager.
//   - opts controller.Options: controller configuration.
//
// Returns:
//   - error: nil on success, or controller setup error.
func (r *TransportReconciler) SetupWithManager(mgr ctrl.Manager, opts controller.Options) error {
	if opts.MaxConcurrentReconciles == 0 {
		opts.MaxConcurrentReconciles = 2
	}
	r.Recorder = mgr.GetEventRecorderFor("transport-controller")
	return ctrl.NewControllerManagedBy(mgr).
		For(&transportv1alpha1.Transport{}).
		Watches(&transportv1alpha1.TransportBinding{}, handler.EnqueueRequestsFromMapFunc(r.transportBindingToTransport)).
		Watches(&bubuv1alpha1.Story{}, handler.EnqueueRequestsFromMapFunc(r.storyToTransport)).
		WithOptions(opts).
		Complete(r)
}

// transportBindingToTransport maps a TransportBinding watch event to the
// parent Transport reconcile request by reading Spec.TransportRef.
//
// Arguments:
//   - _ context.Context: unused; mapper is synchronous.
//   - obj client.Object: expected to be *transportv1alpha1.TransportBinding.
//
// Returns:
//   - []reconcile.Request: nil when the object or reference is invalid; one entry when valid.
func (r *TransportReconciler) transportBindingToTransport(_ context.Context, obj client.Object) []reconcile.Request {
	binding, ok := obj.(*transportv1alpha1.TransportBinding)
	if !ok {
		return nil
	}
	return refs.RequestsFromNames(binding.Spec.TransportRef)
}

// storyToTransport maps Story watch events to Transport reconcile requests for
// every referenced transport listed in story.Spec.Transports.
//
// Arguments:
//   - _ context.Context: unused.
//   - obj client.Object: expected Story carrying transport references.
//
// Returns:
//   - []reconcile.Request: reconciles each unique referenced Transport, or nil when none found.
func (r *TransportReconciler) storyToTransport(_ context.Context, obj client.Object) []reconcile.Request {
	story, ok := obj.(*bubuv1alpha1.Story)
	if !ok || len(story.Spec.Transports) == 0 {
		return nil
	}
	names := make([]string, 0, len(story.Spec.Transports))
	for i := range story.Spec.Transports {
		names = append(names, story.Spec.Transports[i].TransportRef)
	}
	return refs.RequestsFromNames(names...)
}
