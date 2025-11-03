package logging

import (
	"sync/atomic"

	"github.com/go-logr/logr"
)

var (
	verboseLoggingEnabled    atomic.Bool
	stepOutputLoggingEnabled atomic.Bool
)

func init() {
	verboseLoggingEnabled.Store(false)
	stepOutputLoggingEnabled.Store(false)
}

// EnableVerboseLogging toggles whether verbosity>0 logs should be emitted.
func EnableVerboseLogging(enabled bool) {
	verboseLoggingEnabled.Store(enabled)
}

// VerboseLoggingEnabled reports whether verbosity-gated logs should be emitted.
func VerboseLoggingEnabled() bool {
	return verboseLoggingEnabled.Load()
}

// EnableStepOutputLogging toggles whether step outputs should be logged when aggregated.
func EnableStepOutputLogging(enabled bool) {
	stepOutputLoggingEnabled.Store(enabled)
}

// StepOutputLoggingEnabled reports whether step outputs should be logged during DAG reconciliation.
func StepOutputLoggingEnabled() bool {
	return stepOutputLoggingEnabled.Load()
}

// mutedLogger returns a discard logger used when verbosity is disabled.
func mutedLogger() logr.Logger {
	return logr.Discard()
}
