package capabilities

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/go-logr/logr"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	transportv1alpha1 "github.com/bubustack/bobrapet/api/transport/v1alpha1"
	"github.com/bubustack/bobrapet/pkg/conditions"
	"github.com/bubustack/bobrapet/pkg/transport"
	"github.com/bubustack/bobrapet/pkg/transport/binding"
)

const (
	transportBindingIndex = "spec.transportRef"
)

// Aggregation captures the negotiated codec slices and binding health counters
// discovered while inspecting TransportBindings for a given Transport.
type Aggregation struct {
	Audio         []transportv1alpha1.AudioCodec
	Video         []transportv1alpha1.VideoCodec
	Binary        []string
	ReadyBindings int
	TotalBindings int
	BindingErrors []string
	LastHeartbeat *metav1.Time
}

// AggregationOptions configures how AggregateBindings inspects and patches
// TransportBindings.
type AggregationOptions struct {
	Logger           logr.Logger
	HeartbeatTimeout time.Duration
}

// AggregateBindings lists TransportBindings referencing the provided Transport,
// derives negotiated capability slices, enforces heartbeat freshness, and
// returns the aggregated results.
func AggregateBindings(
	ctx context.Context,
	c client.Client,
	transportObj *transportv1alpha1.Transport,
	opts AggregationOptions,
) (*Aggregation, error) {
	var bindingList transportv1alpha1.TransportBindingList
	if err := c.List(
		ctx,
		&bindingList,
		client.MatchingFields{transportBindingIndex: transportObj.Name},
	); err != nil {
		return nil, err
	}

	timeout := opts.HeartbeatTimeout
	if timeout <= 0 {
		timeout = 2 * time.Minute
	}

	result := &Aggregation{
		Audio:         make([]transportv1alpha1.AudioCodec, 0),
		Video:         make([]transportv1alpha1.VideoCodec, 0),
		Binary:        make([]string, 0),
		ReadyBindings: 0,
		TotalBindings: len(bindingList.Items),
		BindingErrors: make([]string, 0),
	}

	now := time.Now()

	for i := range bindingList.Items {
		bindingObj := bindingList.Items[i]
		transport.DeriveNegotiatedCapabilities(&bindingObj, &bindingObj.Status)

		if isStale, msg := heartbeatStale(&bindingObj, timeout, now); isStale {
			if err := binding.PatchReadyCondition(
				ctx,
				c,
				&bindingObj,
				false,
				conditions.ReasonTransportFailed,
				msg,
			); err != nil {
				logError(opts.Logger, err, "Failed to mark TransportBinding stale", bindingObj.Name)
				return nil, fmt.Errorf("mark binding %s/%s stale: %w", bindingObj.Namespace, bindingObj.Name, err)
			}
			result.BindingErrors = append(
				result.BindingErrors,
				fmt.Sprintf("%s/%s: %s", bindingObj.Namespace, bindingObj.Name, msg),
			)
			continue
		}

		if hb := readyConditionTime(&bindingObj); hb != nil {
			if result.LastHeartbeat == nil || hb.After(result.LastHeartbeat.Time) {
				hbCopy := hb.DeepCopy()
				result.LastHeartbeat = hbCopy
			}
		}

		isReady, failed, reason := binding.ReadyState(&bindingObj)
		if isReady {
			result.ReadyBindings++
		} else if failed {
			result.BindingErrors = append(
				result.BindingErrors,
				fmt.Sprintf("%s/%s: %s", bindingObj.Namespace, bindingObj.Name, reason),
			)
		}

		if bindingObj.Status.NegotiatedAudio != nil {
			result.Audio = AppendUniqueAudioCodec(result.Audio, *bindingObj.Status.NegotiatedAudio)
		}
		if bindingObj.Status.NegotiatedVideo != nil {
			result.Video = AppendUniqueVideoCodec(result.Video, *bindingObj.Status.NegotiatedVideo)
		}
		if val := bindingObj.Status.NegotiatedBinary; strings.TrimSpace(val) != "" {
			result.Binary = AppendUniqueBinary(result.Binary, val)
		}
	}

	return result, nil
}

func heartbeatStale(
	bindingObj *transportv1alpha1.TransportBinding,
	timeout time.Duration,
	now time.Time,
) (bool, string) {
	cond := conditions.GetCondition(bindingObj.Status.Conditions, conditions.ConditionReady)
	if cond == nil {
		return false, ""
	}
	if timeout <= 0 {
		return false, ""
	}
	if now.Sub(cond.LastTransitionTime.Time) <= timeout {
		return false, ""
	}
	return true, fmt.Sprintf(
		"connector heartbeat stale for %s",
		now.Sub(cond.LastTransitionTime.Time).Round(time.Second),
	)
}

func readyConditionTime(bindingObj *transportv1alpha1.TransportBinding) *metav1.Time {
	cond := conditions.GetCondition(bindingObj.Status.Conditions, conditions.ConditionReady)
	if cond == nil {
		return nil
	}
	return &cond.LastTransitionTime
}

func logError(logger logr.Logger, err error, msg string, bindingName string) {
	if logger.GetSink() == nil {
		return
	}
	logger.Error(err, msg, "binding", bindingName)
}
