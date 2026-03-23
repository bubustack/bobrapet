package chain

import (
	"context"
	"fmt"
	"time"

	"github.com/go-logr/logr"
)

// Mode controls how a stage reacts to errors.
type Mode int

const (
	// ModeFailFast stops execution when the stage returns an error.
	ModeFailFast Mode = iota
	// ModeLogAndSkip records the error and proceeds to the next stage.
	ModeLogAndSkip
)

const unknownValue = "unknown"

// String returns the canonical identifier for the mode.
func (m Mode) String() string {
	switch m {
	case ModeFailFast:
		return "fail_fast"
	case ModeLogAndSkip:
		return "log_and_skip"
	default:
		return unknownValue
	}
}

// StageFunc encapsulates stage logic.
type StageFunc func(context.Context) error

// Stage defines a resolver stage driven by the executor.
type Stage struct {
	Name string
	Mode Mode
	Fn   StageFunc
}

// StageError captures log-and-skip failures for later inspection.
type StageError struct {
	Stage string
	Err   error
}

type StageOutcome string

const (
	OutcomeSuccess StageOutcome = "success"
	OutcomeSkipped StageOutcome = "skipped"
	OutcomeFailed  StageOutcome = "failed"
)

type Observer interface {
	StageStarted(ctx context.Context, stage Stage)
	StageCompleted(ctx context.Context, stage Stage, outcome StageOutcome, err error, duration time.Duration)
}

// Executor runs staged callbacks with shared logging.
type Executor struct {
	log       logr.Logger
	stages    []Stage
	warnings  []StageError
	observers []Observer
}

// NewExecutor creates a resolver chain executor.
func NewExecutor(logger logr.Logger) *Executor {
	return &Executor{log: logger}
}

func (e *Executor) WithObserver(observer Observer) *Executor {
	if observer != nil {
		e.observers = append(e.observers, observer)
	}
	return e
}

// Add registers a stage in execution order.
func (e *Executor) Add(stage Stage) *Executor {
	if stage.Fn == nil {
		return e
	}
	e.stages = append(e.stages, stage)
	return e
}

// Run executes stages sequentially, honoring the configured mode.
func (e *Executor) Run(ctx context.Context) error {
	e.warnings = nil
	for _, stage := range e.stages {
		for _, observer := range e.observers {
			observer.StageStarted(ctx, stage)
		}

		e.log.V(1).Info("resolver stage start", "stage", stage.Name)
		start := time.Now()
		err := stage.Fn(ctx)
		outcome := OutcomeSuccess
		if err != nil {
			switch stage.Mode {
			case ModeLogAndSkip:
				e.log.Info("resolver stage skipped", "stage", stage.Name, "error", err.Error())
				e.warnings = append(e.warnings, StageError{Stage: stage.Name, Err: err})
				outcome = OutcomeSkipped
			default:
				outcome = OutcomeFailed
			}
		}

		for _, observer := range e.observers {
			observer.StageCompleted(ctx, stage, outcome, err, time.Since(start))
		}

		if err != nil && stage.Mode != ModeLogAndSkip {
			return fmt.Errorf("resolver stage %s: %w", stage.Name, err)
		}
		if outcome == OutcomeSuccess {
			e.log.V(1).Info("resolver stage complete", "stage", stage.Name)
		}
	}
	return nil
}

// Warnings exposes log-and-skip failures recorded during the most recent Run.
func (e *Executor) Warnings() []StageError {
	out := make([]StageError, len(e.warnings))
	copy(out, e.warnings)
	return out
}
