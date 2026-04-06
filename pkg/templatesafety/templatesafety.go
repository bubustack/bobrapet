package templatesafety

import (
	"fmt"
	"regexp"
	"strings"
)

var deniedFunctions = []string{"env", "expandenv", "getHostByName"}

var deniedFuncPattern = regexp.MustCompile(`\b(env|expandenv|getHostByName)\b`)

var deniedFuncMatchers = map[string]*regexp.Regexp{
	"env":           regexp.MustCompile(`\benv\b`),
	"expandenv":     regexp.MustCompile(`\bexpandenv\b`),
	"getHostByName": regexp.MustCompile(`\bgetHostByName\b`),
}

// ValidateTemplateString rejects template strings that reference denied functions.
// It scans all {{ ... }} expressions and returns an error on the first forbidden use.
func ValidateTemplateString(value string) error {
	exprs := extractTemplateExpressions(value)
	for _, expr := range exprs {
		if err := validateExpression(expr); err != nil {
			return err
		}
	}
	return nil
}

// ValidateTemplateJSON rejects templates embedded in JSON by scanning for {{ ... }} blocks.
func ValidateTemplateJSON(raw []byte) error {
	if len(raw) == 0 {
		return nil
	}
	return ValidateTemplateString(string(raw))
}

func extractTemplateExpressions(value string) []string {
	var exprs []string
	remaining := value
	for {
		start := strings.Index(remaining, "{{")
		if start == -1 {
			break
		}
		remaining = remaining[start+2:]
		end := strings.Index(remaining, "}}")
		if end == -1 {
			break
		}
		expr := strings.TrimSpace(remaining[:end])
		expr = strings.TrimPrefix(expr, "-")
		expr = strings.TrimSuffix(expr, "-")
		expr = strings.TrimSpace(expr)
		if expr != "" {
			exprs = append(exprs, expr)
		}
		remaining = remaining[end+2:]
	}
	return exprs
}

func validateExpression(expr string) error {
	if deniedFuncPattern.MatchString(expr) {
		for _, fn := range deniedFunctions {
			if matcher, ok := deniedFuncMatchers[fn]; ok && matcher.MatchString(expr) {
				return fmt.Errorf("template expression uses disallowed function '%s'", fn)
			}
		}
		return fmt.Errorf("template expression uses disallowed function")
	}
	return nil
}
