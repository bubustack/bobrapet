package runs

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"github.com/bubustack/bobrapet/pkg/storage"
	"github.com/bubustack/core/templating"
)

// stepOutputRefPattern matches step output references in template expressions:
// - steps.name.output(s), steps["name"].output(s), and (index .steps "name").output(s).
var stepOutputRefPattern = regexp.MustCompile(
	`steps\s*\.\s*([a-zA-Z0-9_\-]+)\s*\.(output|outputs)\b|` +
		`steps\s*\[\s*['"]([a-zA-Z0-9_\-]+)['"]\s*\]\s*\.(output|outputs)\b|` +
		`\(index\s+\.steps\s+["']([a-zA-Z0-9_\-]+)["']\)\s*\.(output|outputs)\b`,
)

// detectOffloadedOutputRefs returns ErrOffloadedDataUsage when an expression
// references an offloaded step output.
func detectOffloadedOutputRefs(expr string, steps map[string]any) *templating.ErrOffloadedDataUsage {
	if expr == "" || len(steps) == 0 {
		return nil
	}
	trimmed := strings.TrimSpace(expr)
	if trimmed == "" {
		return nil
	}
	if isLikelyJSON(trimmed) {
		var node any
		if err := json.Unmarshal([]byte(trimmed), &node); err == nil {
			return detectOffloadedInJSON(node, steps)
		}
	}
	return detectOffloadedInTemplateString(trimmed, steps)
}

func stepOutputHasStorageRef(steps map[string]any, stepName string) bool {
	if steps == nil || stepName == "" {
		return false
	}
	stepCtx, ok := steps[stepName].(map[string]any)
	if !ok || stepCtx == nil {
		return false
	}
	if output, ok := stepCtx["output"]; ok && containsStorageRef(output, 0) {
		return true
	}
	if outputs, ok := stepCtx["outputs"]; ok && containsStorageRef(outputs, 0) {
		return true
	}
	return false
}

// filterStepsForTemplate returns a filtered steps map containing only the step
// contexts actually referenced by the template expression. This keeps the
// materialize request small by excluding unreferenced step outputs.
//
// If no step references are found, the original map is returned unchanged.
func filterStepsForTemplate(expr string, steps map[string]any) map[string]any {
	if steps == nil || expr == "" {
		return steps
	}
	referenced := extractReferencedSteps(expr)
	if len(referenced) == 0 {
		return steps
	}
	filtered := make(map[string]any)
	for name, val := range steps {
		if referenced[name] {
			filtered[name] = val
		}
	}
	return filtered
}

func detectOffloadedInJSON(node any, steps map[string]any) *templating.ErrOffloadedDataUsage {
	switch typed := node.(type) {
	case map[string]any:
		for _, value := range typed {
			if err := detectOffloadedInJSON(value, steps); err != nil {
				return err
			}
		}
	case []any:
		for _, value := range typed {
			if err := detectOffloadedInJSON(value, steps); err != nil {
				return err
			}
		}
	case string:
		if !strings.Contains(typed, "{{") {
			return nil
		}
		return detectOffloadedInTemplateString(typed, steps)
	}
	return nil
}

func detectOffloadedInTemplateString(expr string, steps map[string]any) *templating.ErrOffloadedDataUsage {
	if expr == "" || len(steps) == 0 {
		return nil
	}
	matches := stepOutputRefPattern.FindAllStringSubmatch(expr, -1)
	for _, match := range matches {
		stepName := ""
		if len(match) > 5 && match[5] != "" {
			stepName = match[5] // (index .steps "name")
		} else if len(match) > 3 && match[3] != "" {
			stepName = match[3] // steps["name"]
		} else if len(match) > 1 && match[1] != "" {
			stepName = match[1] // steps.name
		}
		if stepName == "" {
			continue
		}
		if stepOutputHasStorageRef(steps, stepName) {
			return &templating.ErrOffloadedDataUsage{
				Reason: fmt.Sprintf("template references offloaded output from step %q", stepName),
			}
		}
	}
	return nil
}

func extractReferencedSteps(expr string) map[string]bool {
	trimmed := strings.TrimSpace(expr)
	if trimmed == "" {
		return nil
	}
	referenced := make(map[string]bool)
	if isLikelyJSON(trimmed) {
		var node any
		if err := json.Unmarshal([]byte(trimmed), &node); err == nil {
			collectReferencedStepsFromJSON(node, referenced)
			if len(referenced) > 0 {
				return referenced
			}
		}
	}
	collectReferencedStepsFromTemplate(trimmed, referenced)
	return referenced
}

func collectReferencedStepsFromJSON(node any, referenced map[string]bool) {
	switch typed := node.(type) {
	case map[string]any:
		for _, value := range typed {
			collectReferencedStepsFromJSON(value, referenced)
		}
	case []any:
		for _, value := range typed {
			collectReferencedStepsFromJSON(value, referenced)
		}
	case string:
		if !strings.Contains(typed, "{{") {
			return
		}
		collectReferencedStepsFromTemplate(typed, referenced)
	}
}

func collectReferencedStepsFromTemplate(expr string, referenced map[string]bool) {
	if expr == "" || referenced == nil {
		return
	}
	matches := stepOutputRefPattern.FindAllStringSubmatch(expr, -1)
	for _, match := range matches {
		stepName := ""
		if len(match) > 5 && match[5] != "" {
			stepName = match[5]
		} else if len(match) > 3 && match[3] != "" {
			stepName = match[3]
		} else if len(match) > 1 && match[1] != "" {
			stepName = match[1]
		}
		if stepName != "" {
			referenced[stepName] = true
			referenced[normalizeStepIdentifier(stepName)] = true
		}
	}
}

func isLikelyJSON(raw string) bool {
	if raw == "" {
		return false
	}
	switch raw[0] {
	case '{', '[':
		return true
	default:
		return false
	}
}

func containsStorageRef(value any, depth int) bool {
	if depth > 8 || value == nil {
		return false
	}
	switch v := value.(type) {
	case map[string]any:
		if _, ok := v[storage.StorageRefKey]; ok {
			return true
		}
		for _, entry := range v {
			if containsStorageRef(entry, depth+1) {
				return true
			}
		}
	case map[any]any:
		if _, ok := v[storage.StorageRefKey]; ok {
			return true
		}
		for _, entry := range v {
			if containsStorageRef(entry, depth+1) {
				return true
			}
		}
	case []any:
		for _, entry := range v {
			if containsStorageRef(entry, depth+1) {
				return true
			}
		}
	}
	return false
}
