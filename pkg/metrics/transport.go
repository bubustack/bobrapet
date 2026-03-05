package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

var (
	transportBindingsReady = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "bobrapet_transport_bindings_ready",
			Help: "Number of ready transport bindings per transport",
		},
		[]string{"transport"},
	)

	transportBindingsTotal = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "bobrapet_transport_bindings_total",
			Help: "Total number of transport bindings per transport",
		},
		[]string{"transport"},
	)

	transportBindingsPending = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "bobrapet_transport_bindings_pending",
			Help: "Pending bindings awaiting negotiation per transport",
		},
		[]string{"transport"},
	)

	transportBindingsFailed = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "bobrapet_transport_bindings_failed",
			Help: "Bindings reporting errors per transport",
		},
		[]string{"transport"},
	)
)

// init registers the transport binding gauge collectors with the controller-runtime
// metrics registry so every controller uses the same Prometheus series.
func init() {
	metrics.Registry.MustRegister(
		transportBindingsReady,
		transportBindingsTotal,
		transportBindingsPending,
		transportBindingsFailed,
	)
}

// RecordTransportBindingSnapshot records the current ready/total/pending/failed
// binding counts for a Transport so controllers and background workers emit the
// same metric series.
func RecordTransportBindingSnapshot(name string, ready, total, pending, failed int) {
	if !shouldRecord() {
		return
	}
	labels := prometheus.Labels{"transport": name}
	transportBindingsReady.With(labels).Set(float64(ready))
	transportBindingsTotal.With(labels).Set(float64(total))
	transportBindingsPending.With(labels).Set(float64(pending))
	transportBindingsFailed.With(labels).Set(float64(failed))
}
