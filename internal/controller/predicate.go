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

package controller

import (
	"reflect"
	"strings"

	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

// SpecChangedPredicate filters events to only reconcile on meaningful changes
// (generation, annotations, labels) and ignores status-only updates
func SpecChangedPredicate() predicate.Predicate {
	return predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			return true // Always process creates
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			// Reconcile on generation changes (spec updates)
			if e.ObjectOld.GetGeneration() != e.ObjectNew.GetGeneration() {
				return true
			}

			// Reconcile on annotation changes (for manual triggers, sleep timers, etc.)
			if !reflect.DeepEqual(e.ObjectOld.GetAnnotations(), e.ObjectNew.GetAnnotations()) {
				return true
			}

			// Reconcile on label changes (for selectors, tenant changes, etc.)
			if !reflect.DeepEqual(e.ObjectOld.GetLabels(), e.ObjectNew.GetLabels()) {
				return true
			}

			// Ignore status-only updates
			return false
		},
		DeleteFunc: func(e event.DeleteEvent) bool {
			return true // Always process deletes for finalizer handling
		},
		GenericFunc: func(e event.GenericEvent) bool {
			return true // Process generic events (e.g., external triggers)
		},
	}
}

// OwnerControlledPredicate filters events for resources that are owned by another controller
// This prevents reconciliation loops when child resources are managed by parent controllers
func OwnerControlledPredicate() predicate.Predicate {
	return predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			// Don't reconcile owned resources on create
			return len(e.Object.GetOwnerReferences()) == 0
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			// Don't reconcile owned resources unless generation changed
			if len(e.ObjectNew.GetOwnerReferences()) > 0 {
				return e.ObjectOld.GetGeneration() != e.ObjectNew.GetGeneration()
			}
			return true
		},
		DeleteFunc: func(e event.DeleteEvent) bool {
			return true // Always handle deletes for cleanup
		},
		GenericFunc: func(e event.GenericEvent) bool {
			return len(e.Object.GetOwnerReferences()) == 0
		},
	}
}

// TenantScopedPredicate filters events to only process resources for specific tenants
// Useful for tenant-scoped controller instances
func TenantScopedPredicate(allowedTenants []string) predicate.Predicate {
	tenantSet := make(map[string]bool)
	for _, tenant := range allowedTenants {
		tenantSet[tenant] = true
	}

	getTenant := func(obj interface{}) string {
		if labels := obj.(interface{ GetLabels() map[string]string }).GetLabels(); labels != nil {
			if tenant := labels["bobrapet.bubu.sh/tenant"]; tenant != "" {
				return tenant
			}
		}
		return "default"
	}

	checkTenant := func(obj interface{}) bool {
		if len(allowedTenants) == 0 {
			return true // No filtering if no tenants specified
		}
		return tenantSet[getTenant(obj)]
	}

	return predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			return checkTenant(e.Object)
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			return checkTenant(e.ObjectNew)
		},
		DeleteFunc: func(e event.DeleteEvent) bool {
			return checkTenant(e.Object)
		},
		GenericFunc: func(e event.GenericEvent) bool {
			return checkTenant(e.Object)
		},
	}
}

// AnnotationChangedPredicate triggers on specific annotation changes
// More efficient than SpecChangedPredicate when you only care about specific annotations
func AnnotationChangedPredicate(annotations ...string) predicate.Predicate {
	return predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			return true
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			oldAnnotations := e.ObjectOld.GetAnnotations()
			newAnnotations := e.ObjectNew.GetAnnotations()

			for _, annotation := range annotations {
				oldVal := ""
				newVal := ""
				if oldAnnotations != nil {
					oldVal = oldAnnotations[annotation]
				}
				if newAnnotations != nil {
					newVal = newAnnotations[annotation]
				}
				if oldVal != newVal {
					return true
				}
			}
			return false
		},
		DeleteFunc: func(e event.DeleteEvent) bool {
			return true
		},
		GenericFunc: func(e event.GenericEvent) bool {
			return true
		},
	}
}

// LabelChangedPredicate triggers on specific label changes
// More efficient than SpecChangedPredicate when you only care about specific labels
func LabelChangedPredicate(labels ...string) predicate.Predicate {
	return predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			return true
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			oldLabels := e.ObjectOld.GetLabels()
			newLabels := e.ObjectNew.GetLabels()

			for _, label := range labels {
				oldVal := ""
				newVal := ""
				if oldLabels != nil {
					oldVal = oldLabels[label]
				}
				if newLabels != nil {
					newVal = newLabels[label]
				}
				if oldVal != newVal {
					return true
				}
			}
			return false
		},
		DeleteFunc: func(e event.DeleteEvent) bool {
			return true
		},
		GenericFunc: func(e event.GenericEvent) bool {
			return true
		},
	}
}

// ResourceVersionChangedPredicate filters based on resource version changes
// Useful for controllers that need to react to any change, including status updates
func ResourceVersionChangedPredicate() predicate.Predicate {
	return predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			return true
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			return e.ObjectOld.GetResourceVersion() != e.ObjectNew.GetResourceVersion()
		},
		DeleteFunc: func(e event.DeleteEvent) bool {
			return true
		},
		GenericFunc: func(e event.GenericEvent) bool {
			return true
		},
	}
}

// PausedResourcePredicate filters for paused/unpaused resources
// Useful for controllers that need to handle pause/resume functionality
func PausedResourcePredicate() predicate.Predicate {
	isPaused := func(obj interface{}) bool {
		if annotations := obj.(interface{ GetAnnotations() map[string]string }).GetAnnotations(); annotations != nil {
			return annotations["runs.bubu.sh/pause"] == "true"
		}
		return false
	}

	return predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			return true // Always process creates
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			oldPaused := isPaused(e.ObjectOld)
			newPaused := isPaused(e.ObjectNew)

			// Trigger on pause state changes or generation changes for non-paused resources
			if oldPaused != newPaused {
				return true
			}

			// For non-paused resources, use standard generation check
			if !newPaused {
				return e.ObjectOld.GetGeneration() != e.ObjectNew.GetGeneration()
			}

			// For paused resources, only trigger on resume
			return false
		},
		DeleteFunc: func(e event.DeleteEvent) bool {
			return true
		},
		GenericFunc: func(e event.GenericEvent) bool {
			return true
		},
	}
}

// NamespaceFilterPredicate filters events by namespace patterns
// Useful for multi-tenant deployments or namespace-scoped controllers
func NamespaceFilterPredicate(allowedPatterns ...string) predicate.Predicate {
	matchesPattern := func(namespace string) bool {
		if len(allowedPatterns) == 0 {
			return true // No filtering if no patterns specified
		}

		for _, pattern := range allowedPatterns {
			if pattern == "*" || pattern == namespace {
				return true
			}
			// Simple wildcard matching
			if strings.HasSuffix(pattern, "*") {
				prefix := strings.TrimSuffix(pattern, "*")
				if strings.HasPrefix(namespace, prefix) {
					return true
				}
			}
		}
		return false
	}

	return predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			return matchesPattern(e.Object.GetNamespace())
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			return matchesPattern(e.ObjectNew.GetNamespace())
		},
		DeleteFunc: func(e event.DeleteEvent) bool {
			return matchesPattern(e.Object.GetNamespace())
		},
		GenericFunc: func(e event.GenericEvent) bool {
			return matchesPattern(e.Object.GetNamespace())
		},
	}
}

// FinalizerChangedPredicate triggers on finalizer changes
// Useful for controllers that need to react to finalizer additions/removals
func FinalizerChangedPredicate() predicate.Predicate {
	return predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			return true
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			// Check if finalizers changed
			oldFinalizers := e.ObjectOld.GetFinalizers()
			newFinalizers := e.ObjectNew.GetFinalizers()

			if len(oldFinalizers) != len(newFinalizers) {
				return true
			}

			// Compare finalizer slices
			finalizerMap := make(map[string]bool)
			for _, f := range oldFinalizers {
				finalizerMap[f] = true
			}

			for _, f := range newFinalizers {
				if !finalizerMap[f] {
					return true // New finalizer found
				}
			}

			return false
		},
		DeleteFunc: func(e event.DeleteEvent) bool {
			return true
		},
		GenericFunc: func(e event.GenericEvent) bool {
			return true
		},
	}
}

// CombinedPredicates creates commonly used predicate combinations for bobrapet controllers
func CombinedPredicates() map[string]predicate.Predicate {
	return map[string]predicate.Predicate{
		// Standard spec changes with pause handling - for StoryRun/StepRun controllers
		"specOrPause": predicate.Or(
			SpecChangedPredicate(),
			AnnotationChangedPredicate("runs.bubu.sh/pause", "runs.bubu.sh/resume"),
		),

		// Spec changes but ignore owned resources - for parent controllers
		"specNotOwned": predicate.And(
			SpecChangedPredicate(),
			predicate.Not(OwnerControlledPredicate()),
		),

		// Full changes including status updates - for monitoring controllers
		"anyChange": ResourceVersionChangedPredicate(),

		// Only finalizer and deletion handling - for cleanup controllers
		"cleanupOnly": predicate.Or(
			FinalizerChangedPredicate(),
			predicate.Funcs{
				CreateFunc:  func(e event.CreateEvent) bool { return false },
				UpdateFunc:  func(e event.UpdateEvent) bool { return e.ObjectNew.GetDeletionTimestamp() != nil },
				DeleteFunc:  func(e event.DeleteEvent) bool { return true },
				GenericFunc: func(e event.GenericEvent) bool { return false },
			},
		),

		// Template changes - for template-dependent controllers
		"templateChanges": predicate.Or(
			SpecChangedPredicate(),
			LabelChangedPredicate("bobrapet.bubu.sh/template-version"),
		),
	}
}
