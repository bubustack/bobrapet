package observability

import "time"

// Logger defines the interface for a structured logger that can be used by shared packages.
type Logger interface {
	CacheHit(expression, expressionType string)
	EvaluationStart(expression, expressionType string)
	EvaluationError(err error, expression, expressionType string, duration time.Duration)
	EvaluationSuccess(expression, expressionType string, duration time.Duration, result any)
}
