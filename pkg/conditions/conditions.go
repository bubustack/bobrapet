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

package conditions

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Standard condition types for all bobrapet resources
const (
	// Universal conditions (all resources)
	ConditionReady       = "Ready"       // Resource is ready for use
	ConditionProgressing = "Progressing" // Resource is progressing towards ready state
	ConditionDegraded    = "Degraded"    // Resource is degraded but still operational
	ConditionTerminating = "Terminating" // Resource is being terminated

	// Template-specific conditions
	ConditionValidated        = "Validated"        // Template/spec validation passed
	ConditionTemplateResolved = "TemplateResolved" // Template reference resolved successfully

	// Data handling conditions
	ConditionLargeDataDelegated = "LargeDataDelegated" // Large input/output delegated to engram/impulse

	// Story-specific conditions
	ConditionCompiled = "Compiled" // Story compiled and ready for execution

	// Execution-specific conditions
	ConditionScheduled      = "Scheduled"      // StepRun scheduled for execution
	ConditionResolvedInputs = "ResolvedInputs" // StoryRun inputs resolved successfully
	ConditionStepsCompleted = "StepsCompleted" // All steps completed

	// Impulse-specific conditions
	ConditionListening     = "Listening"     // Impulse is listening for triggers
	ConditionStoryResolved = "StoryResolved" // Story reference resolved successfully

	// Transport-specific conditions
	ConditionTransportReady = "TransportReady" // Transport is configured and ready
)

// Standard condition reasons
const (
	// Success reasons
	ReasonValidationPassed   = "ValidationPassed"
	ReasonTemplateResolved   = "TemplateResolved"
	ReasonStoryResolved      = "StoryResolved"
	ReasonCompiled           = "Compiled"
	ReasonScheduled          = "Scheduled"
	ReasonListening          = "Listening"
	ReasonCompleted          = "Completed"
	ReasonLargeDataDelegated = "LargeDataDelegated"

	// Error reasons
	ReasonValidationFailed         = "ValidationFailed"
	ReasonTemplateNotFound         = "TemplateNotFound"
	ReasonTemplateResolutionFailed = "TemplateResolutionFailed"
	ReasonStoryNotFound            = "StoryNotFound"
	ReasonStoryReferenceInvalid    = "StoryReferenceInvalid"
	ReasonCompilationFailed        = "CompilationFailed"
	ReasonSchedulingFailed         = "SchedulingFailed"
	ReasonExecutionFailed          = "ExecutionFailed"
	ReasonReferenceNotFound        = "ReferenceNotFound"
	ReasonInvalidConfiguration     = "InvalidConfiguration"
	ReasonDeploymentReady          = "DeploymentReady"

	// Progressing reasons
	ReasonValidating        = "Validating"
	ReasonResolvingTemplate = "ResolvingTemplate"
	ReasonResolvingStory    = "ResolvingStory"
	ReasonCompiling         = "Compiling"
	ReasonStartingExecution = "StartingExecution"
	ReasonProcessingSteps   = "ProcessingSteps"

	// Terminating reasons
	ReasonDeletionRequested = "DeletionRequested"
	ReasonCleaningUp        = "CleaningUp"
	ReasonInputTooLarge     = "InputTooLarge"
	ReasonOutputTooLarge    = "OutputTooLarge"

	// Transport-specific reasons
	ReasonTransportReady  = "TransportReady"
	ReasonTransportFailed = "TransportFailed"
	ReasonReconciling     = "Reconciling"
)

// ConditionManager provides standard condition management
type ConditionManager struct {
	generation int64
}

// NewConditionManager creates a new condition manager
func NewConditionManager(generation int64) *ConditionManager {
	return &ConditionManager{generation: generation}
}

// UpdateGeneration updates the generation for condition tracking
func (cm *ConditionManager) UpdateGeneration(generation int64) {
	cm.generation = generation
}

// SetCondition sets a condition on the conditions slice
func (cm *ConditionManager) SetCondition(conditions *[]metav1.Condition, conditionType string, status metav1.ConditionStatus, reason, message string) {
	now := metav1.Now()
	condition := metav1.Condition{
		Type:               conditionType,
		Status:             status,
		Reason:             reason,
		Message:            message,
		LastTransitionTime: now,
		ObservedGeneration: cm.generation,
	}

	cm.setConditionInternal(conditions, condition)
}

// SetReadyCondition sets the Ready condition
func (cm *ConditionManager) SetReadyCondition(conditions *[]metav1.Condition, ready bool, reason, message string) {
	status := metav1.ConditionFalse
	if ready {
		status = metav1.ConditionTrue
	}
	cm.SetCondition(conditions, ConditionReady, status, reason, message)
}

// SetProgressingCondition sets the Progressing condition
func (cm *ConditionManager) SetProgressingCondition(conditions *[]metav1.Condition, progressing bool, reason, message string) {
	status := metav1.ConditionFalse
	if progressing {
		status = metav1.ConditionTrue
	}
	cm.SetCondition(conditions, ConditionProgressing, status, reason, message)
}

// SetDegradedCondition sets the Degraded condition
func (cm *ConditionManager) SetDegradedCondition(conditions *[]metav1.Condition, degraded bool, reason, message string) {
	status := metav1.ConditionFalse
	if degraded {
		status = metav1.ConditionTrue
	}
	cm.SetCondition(conditions, ConditionDegraded, status, reason, message)
}

// SetTerminatingCondition sets the Terminating condition
func (cm *ConditionManager) SetTerminatingCondition(conditions *[]metav1.Condition, terminating bool, reason, message string) {
	status := metav1.ConditionFalse
	if terminating {
		status = metav1.ConditionTrue
	}
	cm.SetCondition(conditions, ConditionTerminating, status, reason, message)
}

// GetCondition returns a condition by type
func GetCondition(conditions []metav1.Condition, conditionType string) *metav1.Condition {
	for i, condition := range conditions {
		if condition.Type == conditionType {
			return &conditions[i]
		}
	}
	return nil
}

// IsConditionTrue returns true if the condition is True
func IsConditionTrue(conditions []metav1.Condition, conditionType string) bool {
	condition := GetCondition(conditions, conditionType)
	return condition != nil && condition.Status == metav1.ConditionTrue
}

// IsConditionFalse returns true if the condition is False
func IsConditionFalse(conditions []metav1.Condition, conditionType string) bool {
	condition := GetCondition(conditions, conditionType)
	return condition != nil && condition.Status == metav1.ConditionFalse
}

// IsReady returns true if the Ready condition is True
func IsReady(conditions []metav1.Condition) bool {
	return IsConditionTrue(conditions, ConditionReady)
}

// IsProgressing returns true if the Progressing condition is True
func IsProgressing(conditions []metav1.Condition) bool {
	return IsConditionTrue(conditions, ConditionProgressing)
}

// IsDegraded returns true if the Degraded condition is True
func IsDegraded(conditions []metav1.Condition) bool {
	return IsConditionTrue(conditions, ConditionDegraded)
}

// IsTerminating returns true if the Terminating condition is True
func IsTerminating(conditions []metav1.Condition) bool {
	return IsConditionTrue(conditions, ConditionTerminating)
}

// setConditionInternal implements the core condition setting logic
func (cm *ConditionManager) setConditionInternal(conditions *[]metav1.Condition, newCondition metav1.Condition) {
	for i, condition := range *conditions {
		if condition.Type == newCondition.Type {
			// Only update LastTransitionTime if status changed
			if condition.Status != newCondition.Status {
				(*conditions)[i] = newCondition
			} else {
				// Keep original transition time but update other fields
				newCondition.LastTransitionTime = condition.LastTransitionTime
				(*conditions)[i] = newCondition
			}
			return
		}
	}
	// Condition not found, append it
	*conditions = append(*conditions, newCondition)
}

// TransitionConditions handles standard resource state transitions
func (cm *ConditionManager) TransitionConditions(conditions *[]metav1.Condition, targetState string, reason, message string) {
	switch targetState {
	case "Ready":
		cm.SetReadyCondition(conditions, true, reason, message)
		cm.SetProgressingCondition(conditions, false, ReasonCompleted, "Resource is ready")
		cm.SetDegradedCondition(conditions, false, ReasonCompleted, "Resource is healthy")

	case "Progressing":
		cm.SetReadyCondition(conditions, false, reason, message)
		cm.SetProgressingCondition(conditions, true, reason, message)
		cm.SetDegradedCondition(conditions, false, ReasonCompleted, "Resource is progressing normally")

	case "Degraded":
		cm.SetReadyCondition(conditions, false, reason, message)
		cm.SetProgressingCondition(conditions, false, reason, "Progress halted due to degradation")
		cm.SetDegradedCondition(conditions, true, reason, message)

	case "Terminating":
		cm.SetTerminatingCondition(conditions, true, reason, message)
		cm.SetProgressingCondition(conditions, false, ReasonDeletionRequested, "Resource is being deleted")

	case "Failed":
		cm.SetReadyCondition(conditions, false, reason, message)
		cm.SetProgressingCondition(conditions, false, reason, "Resource failed")
		cm.SetDegradedCondition(conditions, true, reason, message)
	}
}

// SetLargeDataDelegatedCondition sets the LargeDataDelegated condition
func (cm *ConditionManager) SetLargeDataDelegatedCondition(conditions *[]metav1.Condition, delegated bool, reason, message string) {
	status := metav1.ConditionFalse
	if delegated {
		status = metav1.ConditionTrue
	}
	cm.SetCondition(conditions, ConditionLargeDataDelegated, status, reason, message)
}
