package chain

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/go-logr/logr/testr"
)

func TestExecutorLogAndSkip(t *testing.T) {
	logger := testr.New(t)
	exec := NewExecutor(logger)
	order := make([]string, 0)

	exec.
		Add(Stage{
			Name: "first",
			Mode: ModeFailFast,
			Fn: func(context.Context) error {
				order = append(order, "first")
				return nil
			},
		}).
		Add(Stage{
			Name: "warning",
			Mode: ModeLogAndSkip,
			Fn: func(context.Context) error {
				order = append(order, "warning")
				return errors.New("boom")
			},
		}).
		Add(Stage{
			Name: "last",
			Mode: ModeFailFast,
			Fn: func(context.Context) error {
				order = append(order, "last")
				return nil
			},
		})

	if err := exec.Run(context.Background()); err != nil {
		t.Fatalf("Run() unexpected error: %v", err)
	}
	if want, got := 3, len(order); want != got {
		t.Fatalf("unexpected execution order length: want %d got %d (%v)", want, got, order)
	}
	if warnings := exec.Warnings(); len(warnings) != 1 || warnings[0].Stage != "warning" {
		t.Fatalf("unexpected warnings: %v", warnings)
	}
}

func TestExecutorFailFastStops(t *testing.T) {
	logger := testr.New(t)
	exec := NewExecutor(logger)
	calls := 0

	exec.
		Add(Stage{
			Name: "fail",
			Mode: ModeFailFast,
			Fn: func(context.Context) error {
				calls++
				return errors.New("fail")
			},
		}).
		Add(Stage{
			Name: "never",
			Mode: ModeFailFast,
			Fn: func(context.Context) error {
				calls++
				return nil
			},
		})

	if err := exec.Run(context.Background()); err == nil {
		t.Fatalf("expected error")
	}
	if calls != 1 {
		t.Fatalf("expected only first stage to run, got %d calls", calls)
	}
	if warnings := exec.Warnings(); len(warnings) != 0 {
		t.Fatalf("expected no warnings, got %v", warnings)
	}
}

func TestExecutorObserverReceivesOutcomes(t *testing.T) {
	logger := testr.New(t)
	observer := &recordingObserver{}
	exec := NewExecutor(logger).WithObserver(observer)

	exec.
		Add(Stage{
			Name: "ok",
			Mode: ModeFailFast,
			Fn: func(context.Context) error {
				return nil
			},
		}).
		Add(Stage{
			Name: "skip",
			Mode: ModeLogAndSkip,
			Fn: func(context.Context) error {
				return errors.New("skip-me")
			},
		}).
		Add(Stage{
			Name: "boom",
			Mode: ModeFailFast,
			Fn: func(context.Context) error {
				return errors.New("boom")
			},
		})

	if err := exec.Run(context.Background()); err == nil {
		t.Fatalf("expected error from fail-fast stage")
	}

	if want, got := []string{"ok", "skip", "boom"}, observer.starts; len(got) != len(want) {
		t.Fatalf("unexpected start events %v", got)
	} else {
		for i, stage := range want {
			if got[i] != stage {
				t.Fatalf("start event %d, want %s, got %s", i, stage, got[i])
			}
		}
	}

	if len(observer.completions) != 3 {
		t.Fatalf("expected 3 completion events, got %d", len(observer.completions))
	}
	if observer.completions[0].outcome != OutcomeSuccess {
		t.Fatalf("expected success outcome for ok stage, got %s", observer.completions[0].outcome)
	}
	if observer.completions[1].outcome != OutcomeSkipped {
		t.Fatalf("expected skipped outcome for log-and-skip stage, got %s", observer.completions[1].outcome)
	}
	if observer.completions[2].outcome != OutcomeFailed {
		t.Fatalf("expected failed outcome for fail-fast stage, got %s", observer.completions[2].outcome)
	}
}

type completionRecord struct {
	stage   string
	outcome StageOutcome
}

type recordingObserver struct {
	starts      []string
	completions []completionRecord
}

func (r *recordingObserver) StageStarted(_ context.Context, stage Stage) {
	r.starts = append(r.starts, stage.Name)
}

func (r *recordingObserver) StageCompleted(
	_ context.Context,
	stage Stage,
	outcome StageOutcome,
	_ error,
	_ time.Duration,
) {
	r.completions = append(r.completions, completionRecord{stage: stage.Name, outcome: outcome})
}
