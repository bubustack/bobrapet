/*
Copyright 2025 BubuStack.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package cel

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
)

// ExpressionResolver handles GitHub Actions-style expression resolution
type ExpressionResolver struct {
	client    client.Client
	namespace string
	storyRun  string
}

// NewExpressionResolver creates a new expression resolver
func NewExpressionResolver(k8sClient client.Client, namespace, storyRun string) *ExpressionResolver {
	return &ExpressionResolver{
		client:    k8sClient,
		namespace: namespace,
		storyRun:  storyRun,
	}
}

// ResolveInputs resolves GitHub Actions-style expressions in step inputs
func (r *ExpressionResolver) ResolveInputs(ctx context.Context, inputs map[string]any) (map[string]any, error) {
	resolved := make(map[string]any)

	for key, value := range inputs {
		resolvedValue, err := r.resolveValue(ctx, value)
		if err != nil {
			return nil, fmt.Errorf("failed to resolve input %s: %w", key, err)
		}
		resolved[key] = resolvedValue
	}

	return resolved, nil
}

// resolveValue recursively resolves expressions in any value type
func (r *ExpressionResolver) resolveValue(ctx context.Context, value any) (any, error) {
	switch v := value.(type) {
	case string:
		return r.resolveString(ctx, v)
	case map[string]any:
		return r.resolveMap(ctx, v)
	case []any:
		return r.resolveArray(ctx, v)
	default:
		return value, nil
	}
}

// resolveString resolves expressions in string values
func (r *ExpressionResolver) resolveString(ctx context.Context, str string) (any, error) {
	// Handle ${{ steps.step-name.outputs.field }} expressions
	if strings.Contains(str, "${{ steps.") {
		return r.resolveStepsExpression(ctx, str)
	}

	// Handle ${{ inputs.field }} expressions
	if strings.Contains(str, "${{ inputs.") {
		return r.resolveInputsExpression(ctx, str)
	}

	// Handle ${{ secrets.secret-name }} expressions (GitHub Actions style)
	if strings.Contains(str, "${{ secrets.") {
		return r.resolveSecretsExpression(ctx, str)
	}

	// Handle environment variable expressions ${VAR}
	if strings.Contains(str, "${") {
		return r.resolveEnvExpression(str), nil
	}

	return str, nil
}

// resolveMap resolves expressions in map values
func (r *ExpressionResolver) resolveMap(ctx context.Context, m map[string]any) (map[string]any, error) {
	resolved := make(map[string]any)

	for key, value := range m {
		resolvedValue, err := r.resolveValue(ctx, value)
		if err != nil {
			return nil, err
		}
		resolved[key] = resolvedValue
	}

	return resolved, nil
}

// resolveArray resolves expressions in array values
func (r *ExpressionResolver) resolveArray(ctx context.Context, arr []any) ([]any, error) {
	resolved := make([]any, len(arr))

	for i, value := range arr {
		resolvedValue, err := r.resolveValue(ctx, value)
		if err != nil {
			return nil, err
		}
		resolved[i] = resolvedValue
	}

	return resolved, nil
}

// resolveStepsExpression resolves ${{ steps.step-name.outputs.field }} expressions
func (r *ExpressionResolver) resolveStepsExpression(ctx context.Context, expression string) (any, error) {
	// Parse expression: ${{ steps.extract-data.outputs.data }}
	re := regexp.MustCompile(`\$\{\{\s*steps\.([^.]+)\.outputs\.([^}\s]+)\s*\}\}`)
	matches := re.FindStringSubmatch(expression)

	if len(matches) != 3 {
		return nil, fmt.Errorf("invalid steps expression: %s", expression)
	}

	stepID := matches[1]
	outputField := matches[2]

	// Find the StepRun with this stepID in the same StoryRun
	var stepRuns runsv1alpha1.StepRunList
	err := r.client.List(ctx, &stepRuns,
		client.InNamespace(r.namespace),
		client.MatchingLabels{
			"bubustack.io/storyrun": r.storyRun,
		})

	if err != nil {
		return nil, fmt.Errorf("failed to list StepRuns: %w", err)
	}

	for _, stepRun := range stepRuns.Items {
		if stepRun.Spec.StepID == stepID {
			if stepRun.Status.Output == nil {
				return nil, fmt.Errorf("step %s has no output", stepID)
			}

			// Parse output JSON
			var output map[string]any
			if err := json.Unmarshal(stepRun.Status.Output.Raw, &output); err != nil {
				return nil, fmt.Errorf("failed to parse output from step %s: %w", stepID, err)
			}

			value, exists := output[outputField]
			if !exists {
				return nil, fmt.Errorf("output field %s not found in step %s", outputField, stepID)
			}

			return value, nil
		}
	}

	return nil, fmt.Errorf("step %s not found or not completed", stepID)
}

// resolveInputsExpression resolves ${{ inputs.field }} expressions
func (r *ExpressionResolver) resolveInputsExpression(ctx context.Context, expression string) (any, error) {
	// Parse expression: ${{ inputs.user_data }}
	re := regexp.MustCompile(`\$\{\{\s*inputs\.([^}\s]+)\s*\}\}`)
	matches := re.FindStringSubmatch(expression)

	if len(matches) != 2 {
		return nil, fmt.Errorf("invalid inputs expression: %s", expression)
	}

	inputField := matches[1]

	// Get StoryRun inputs
	var storyRun runsv1alpha1.StoryRun
	err := r.client.Get(ctx, types.NamespacedName{
		Name:      r.storyRun,
		Namespace: r.namespace,
	}, &storyRun)

	if err != nil {
		return nil, fmt.Errorf("failed to get StoryRun: %w", err)
	}

	if storyRun.Spec.Inputs == nil {
		return nil, fmt.Errorf("StoryRun has no inputs")
	}

	// Parse inputs JSON
	var inputs map[string]any
	if err := json.Unmarshal(storyRun.Spec.Inputs.Raw, &inputs); err != nil {
		return nil, fmt.Errorf("failed to parse StoryRun inputs: %w", err)
	}

	value, exists := inputs[inputField]
	if !exists {
		return nil, fmt.Errorf("input field %s not found", inputField)
	}

	return value, nil
}

// resolveSecretsExpression resolves ${{ secrets.secret-name }} or ${{ secrets.secret-name.key }} expressions
func (r *ExpressionResolver) resolveSecretsExpression(ctx context.Context, expression string) (any, error) {
	// Parse expression: ${{ secrets.api-key }} or ${{ secrets.database-creds.PASSWORD }}
	re := regexp.MustCompile(`\$\{\{\s*secrets\.([^.}\s]+)(?:\.([^}\s]+))?\s*\}\}`)
	matches := re.FindStringSubmatch(expression)

	if len(matches) < 2 {
		return nil, fmt.Errorf("invalid secrets expression: %s", expression)
	}

	secretName := matches[1]
	secretKey := ""
	if len(matches) > 2 {
		secretKey = matches[2]
	}

	// Get the Secret from the same namespace as the StoryRun
	var secret corev1.Secret
	err := r.client.Get(ctx, types.NamespacedName{
		Name:      secretName,
		Namespace: r.namespace,
	}, &secret)

	if err != nil {
		return nil, fmt.Errorf("failed to get secret '%s': %w", secretName, err)
	}

	// If specific key is requested
	if secretKey != "" {
		value, exists := secret.Data[secretKey]
		if !exists {
			return nil, fmt.Errorf("key '%s' not found in secret '%s'", secretKey, secretName)
		}
		return string(value), nil
	}

	// If no specific key, return based on secret structure
	if len(secret.Data) == 1 {
		// Single key secret - return the value directly
		for _, value := range secret.Data {
			return string(value), nil
		}
	}

	// Multiple keys - return as map for flexible access
	secretMap := make(map[string]string)
	for key, value := range secret.Data {
		secretMap[key] = string(value)
	}

	return secretMap, nil
}

// resolveEnvExpression resolves ${VAR} environment variable expressions
func (r *ExpressionResolver) resolveEnvExpression(expression string) string {
	// Simple environment variable substitution
	re := regexp.MustCompile(`\$\{([^}]+)\}`)

	return re.ReplaceAllStringFunc(expression, func(match string) string {
		// Extract variable name
		varName := match[2 : len(match)-1] // Remove ${ and }

		// Get environment variable
		if value := os.Getenv(varName); value != "" {
			return value
		}

		// Return original if not found
		return match
	})
}

// CheckDependencies checks if all step dependencies are satisfied
func (r *ExpressionResolver) CheckDependencies(ctx context.Context, dependencies []string) (bool, []string, error) {
	if len(dependencies) == 0 {
		return true, nil, nil
	}

	var stepRuns runsv1alpha1.StepRunList
	err := r.client.List(ctx, &stepRuns,
		client.InNamespace(r.namespace),
		client.MatchingLabels{
			"bubustack.io/storyrun": r.storyRun,
		})

	if err != nil {
		return false, nil, fmt.Errorf("failed to list StepRuns: %w", err)
	}

	// Create a map of completed steps
	completedSteps := make(map[string]bool)
	for _, stepRun := range stepRuns.Items {
		if stepRun.Status.Phase == "Succeeded" {
			completedSteps[stepRun.Spec.StepID] = true
		}
	}

	// Check if all dependencies are completed
	var pendingDeps []string
	for _, dep := range dependencies {
		if !completedSteps[dep] {
			pendingDeps = append(pendingDeps, dep)
		}
	}

	return len(pendingDeps) == 0, pendingDeps, nil
}
