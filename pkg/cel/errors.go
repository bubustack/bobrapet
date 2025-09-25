package cel

import "fmt"

// ErrEvaluationBlocked is a custom error to indicate that CEL evaluation
// cannot proceed because it is waiting on an external dependency, such as
// the output from a previous step.
type ErrEvaluationBlocked struct {
	Reason string
}

func (e *ErrEvaluationBlocked) Error() string {
	return fmt.Sprintf("CEL evaluation blocked: %s", e.Reason)
}
