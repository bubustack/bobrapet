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

package setup

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	catalogv1alpha1 "github.com/bubustack/bobrapet/api/catalog/v1alpha1"
	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	transportv1alpha1 "github.com/bubustack/bobrapet/api/transport/v1alpha1"
	bubushv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/pkg/enums"
	"github.com/bubustack/bobrapet/pkg/kubeutil"
	"github.com/bubustack/bobrapet/pkg/refs"
	"github.com/bubustack/core/contracts"
)

var setupLog = log.Log.WithName("setup")

// Index sentinel values for special cases.
const (
	// IndexSentinelNoDescription is used when an EngramTemplate has no description,
	// allowing explicit queries for templates without descriptions.
	IndexSentinelNoDescription = "no-description"

	// IndexSentinelNoVersion is used when an EngramTemplate has no version set.
	IndexSentinelNoVersion = "no-version"
)

var registeredIndexes sync.Map

// IndexRegistration describes a single field index requirement.
type IndexRegistration struct {
	Object    client.Object
	Field     string
	Extractor func(client.Object) []string
	ErrMsg    string
}

var (
	IndexEngramTemplateRef = IndexRegistration{
		Object:    &bubushv1alpha1.Engram{},
		Field:     contracts.IndexEngramTemplateRef,
		Extractor: extractEngramTemplateName,
		ErrMsg:    "failed to index Engram " + contracts.IndexEngramTemplateRef,
	}
	IndexStepRunEngramRef = IndexRegistration{
		Object:    &runsv1alpha1.StepRun{},
		Field:     contracts.IndexStepRunEngramRef,
		Extractor: extractStepRunEngramRef,
		ErrMsg:    "failed to index StepRun " + contracts.IndexStepRunEngramRef,
	}
	IndexStepRunStoryRunRef = IndexRegistration{
		Object:    &runsv1alpha1.StepRun{},
		Field:     contracts.IndexStepRunStoryRunRef,
		Extractor: extractStepRunStoryRunRef,
		ErrMsg:    "failed to index StepRun " + contracts.IndexStepRunStoryRunRef,
	}
	IndexStoryRunImpulseRef = IndexRegistration{
		Object:    &runsv1alpha1.StoryRun{},
		Field:     contracts.IndexStoryRunImpulseRef,
		Extractor: extractStoryRunImpulseRef,
		ErrMsg:    "failed to index StoryRun " + contracts.IndexStoryRunImpulseRef,
	}
	IndexStoryRunStoryRefName = IndexRegistration{
		Object:    &runsv1alpha1.StoryRun{},
		Field:     contracts.IndexStoryRunStoryRefName,
		Extractor: extractStoryRunStoryRefName,
		ErrMsg:    "failed to index StoryRun " + contracts.IndexStoryRunStoryRefName,
	}
	IndexStoryRunStoryRefKey = IndexRegistration{
		Object:    &runsv1alpha1.StoryRun{},
		Field:     contracts.IndexStoryRunStoryRefKey,
		Extractor: extractStoryRunStoryRefKey,
		ErrMsg:    "failed to index StoryRun " + contracts.IndexStoryRunStoryRefKey,
	}
	IndexImpulseTemplateRef = IndexRegistration{
		Object:    &bubushv1alpha1.Impulse{},
		Field:     contracts.IndexImpulseTemplateRef,
		Extractor: extractImpulseTemplateName,
		ErrMsg:    "failed to index Impulse " + contracts.IndexImpulseTemplateRef,
	}
	IndexImpulseStoryRef = IndexRegistration{
		Object:    &bubushv1alpha1.Impulse{},
		Field:     contracts.IndexImpulseStoryRef,
		Extractor: extractImpulseStoryRefName,
		ErrMsg:    "failed to index Impulse " + contracts.IndexImpulseStoryRef,
	}
	IndexStoryStepEngramRefs = IndexRegistration{
		Object:    &bubushv1alpha1.Story{},
		Field:     contracts.IndexStoryStepEngramRefs,
		Extractor: extractStoryStepEngramRefs,
		ErrMsg:    "failed to index Story " + contracts.IndexStoryStepEngramRefs,
	}
	IndexStoryStepStoryRefs = IndexRegistration{
		Object:    &bubushv1alpha1.Story{},
		Field:     contracts.IndexStoryStepStoryRefs,
		Extractor: extractStoryExecuteStoryRefs,
		ErrMsg:    "failed to index Story " + contracts.IndexStoryStepStoryRefs,
	}
	IndexStoryTransportRefs = IndexRegistration{
		Object:    &bubushv1alpha1.Story{},
		Field:     contracts.IndexStoryTransportRefs,
		Extractor: extractStoryTransportRefs,
		ErrMsg:    "failed to index Story " + contracts.IndexStoryTransportRefs,
	}
	IndexTransportBindingTransportRef = IndexRegistration{
		Object:    &transportv1alpha1.TransportBinding{},
		Field:     contracts.IndexTransportBindingTransportRef,
		Extractor: extractTransportBindingTransportRef,
		ErrMsg:    "failed to index TransportBinding " + contracts.IndexTransportBindingTransportRef,
	}
	IndexEngramTemplateDescription = IndexRegistration{
		Object:    &catalogv1alpha1.EngramTemplate{},
		Field:     contracts.IndexEngramTemplateDescription,
		Extractor: extractEngramTemplateDescription,
		ErrMsg:    "failed to index EngramTemplate " + contracts.IndexEngramTemplateDescription,
	}
	IndexEngramTemplateVersion = IndexRegistration{
		Object:    &catalogv1alpha1.EngramTemplate{},
		Field:     contracts.IndexEngramTemplateVersion,
		Extractor: extractEngramTemplateVersion,
		ErrMsg:    "failed to index EngramTemplate " + contracts.IndexEngramTemplateVersion,
	}
)

var allIndexRegistrations = []IndexRegistration{
	IndexEngramTemplateRef,
	IndexStepRunEngramRef,
	IndexStepRunStoryRunRef,
	IndexStoryRunImpulseRef,
	IndexStoryRunStoryRefName,
	IndexStoryRunStoryRefKey,
	IndexImpulseTemplateRef,
	IndexImpulseStoryRef,
	IndexStoryStepEngramRefs,
	IndexStoryStepStoryRefs,
	IndexStoryTransportRefs,
	IndexTransportBindingTransportRef,
	IndexEngramTemplateDescription,
	IndexEngramTemplateVersion,
}

// SetupIndexers sets up the field indexers for all the controllers.
// The provided context must be tied to the manager lifecycle to ensure
// proper cancellation on shutdown.
func SetupIndexers(ctx context.Context, mgr manager.Manager) {
	setupLog.Info("setting up field indexes", "count", len(allIndexRegistrations))

	if err := EnsureIndexes(ctx, mgr, allIndexRegistrations); err != nil {
		setupLog.Error(err, "failed to register one or more field indexes; continuing without full index coverage")
	}

	LogRegisteredIndexes()
}

// LogRegisteredIndexes emits a summary log of indexes that were actually registered
// in the current process. Only entries present in the registeredIndexes sync.Map
// (i.e. those that completed without error) are reported, so the log accurately
// reflects the live index state rather than the declared registration list.
//
// Side Effects:
//   - Writes a log entry to the setup logger.
func LogRegisteredIndexes() {
	var fields []string
	registeredIndexes.Range(func(key, _ any) bool {
		fields = append(fields, key.(string))
		return true
	})
	sort.Strings(fields)

	setupLog.Info("field indexes registered",
		"count", len(fields),
		"indexes", fields,
	)
}

// IsIndexRegistered checks if a field index was registered at startup.
//
// Behavior:
//   - Returns true if the index was registered via EnsureIndexes.
//   - Can be used by controllers to verify index availability before relying on indexed lookups.
//
// Arguments:
//   - obj client.Object: the object type the index applies to.
//   - field string: the field path for the index.
//
// Returns:
//   - bool: true if registered, false otherwise.
func IsIndexRegistered(obj client.Object, field string) bool {
	key := fmt.Sprintf("%T/%s", obj, field)
	_, exists := registeredIndexes.Load(key)
	return exists
}

// RequiredControllerIndexes returns a map of controller names to their required indexes.
// This can be used for runtime verification or documentation.
func RequiredControllerIndexes() map[string][]string {
	return map[string][]string{
		"EngramController": {
			contracts.IndexStoryStepEngramRefs,
		},
		"ImpulseController": {
			contracts.IndexStoryRunImpulseRef,
			contracts.IndexImpulseTemplateRef,
			contracts.IndexImpulseStoryRef,
		},
		"StoryController": {
			contracts.IndexStoryStepEngramRefs,
			contracts.IndexStoryStepStoryRefs,
			contracts.IndexStoryTransportRefs,
		},
		"StoryRunController": {
			contracts.IndexStepRunStoryRunRef,
			contracts.IndexStoryRunStoryRefName,
			contracts.IndexStoryRunStoryRefKey,
		},
		"StepRunController": {
			contracts.IndexStepRunEngramRef,
			contracts.IndexStepRunStoryRunRef,
		},
		"TransportController": {
			contracts.IndexStoryTransportRefs,
			contracts.IndexTransportBindingTransportRef,
		},
	}
}

// EnsureIndexes registers each requested field index once per manager process.
func EnsureIndexes(ctx context.Context, mgr manager.Manager, regs []IndexRegistration) error {
	var errs []error
	for _, reg := range regs {
		if err := ensureIndex(ctx, mgr, reg); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", reg.ErrMsg, err))
		}
	}
	return errors.Join(errs...)
}

func ensureIndex(ctx context.Context, mgr manager.Manager, reg IndexRegistration) error {
	key := fmt.Sprintf("%T/%s", reg.Object, reg.Field)
	if _, exists := registeredIndexes.Load(key); exists {
		return nil
	}
	if err := indexFieldWithRetry(ctx, mgr, reg.Object, reg.Field, reg.Extractor); err != nil {
		return err
	}
	registeredIndexes.Store(key, struct{}{})
	return nil
}

// indexFieldWithRetry attempts to register a field index with exponential backoff.
//
// Behavior:
//   - Retries up to maxAttempts (20) times if the CRD is not yet registered.
//   - Uses exponential backoff starting at 250ms with 1.5x multiplier.
//   - Respects context cancellation between retry attempts.
//   - Returns immediately on non-transient errors (anything except NoMatchError).
//
// Arguments:
//   - ctx context.Context: used for cancellation during retry waits.
//   - mgr manager.Manager: provides the field indexer via GetFieldIndexer().
//   - obj client.Object: the object type to index.
//   - field string: the virtual field path for the index.
//   - extractor func(client.Object) []string: extracts index values from objects.
//
// Returns:
//   - nil on successful index registration.
//   - Wrapped context error if cancelled during retries.
//   - Timeout error if max attempts exceeded waiting for CRD.
//   - Original error for non-transient failures.
func indexFieldWithRetry(
	ctx context.Context,
	mgr manager.Manager,
	obj client.Object,
	field string,
	extractor func(client.Object) []string,
) error {
	const (
		maxAttempts   = 20
		initialDelay  = 250 * time.Millisecond
		delayMultiple = 1.5
	)

	attempt := 0
	retryable := func(err error) bool {
		return meta.IsNoMatchError(err)
	}

	err := kubeutil.Retry(
		ctx,
		kubeutil.RetryConfig{
			MaxAttempts:  maxAttempts,
			InitialDelay: initialDelay,
			Multiplier:   delayMultiple,
		},
		func(ctx context.Context) error {
			attempt++
			err := mgr.GetFieldIndexer().IndexField(ctx, obj, field, extractor)
			if err != nil && meta.IsNoMatchError(err) {
				setupLog.V(1).Info(
					"CRD not yet registered; retrying field index",
					"field", field,
					"attempt", attempt,
					"type", fmt.Sprintf("%T", obj),
				)
			}
			return err
		},
		retryable,
	)
	if err != nil {
		if meta.IsNoMatchError(err) {
			return fmt.Errorf("timed out waiting for CRD to index %s on %T: %w", field, obj, err)
		}
		return fmt.Errorf("failed to index %s on %T: %w", field, obj, err)
	}
	return nil
}

// indexStringValue wraps a non-empty value into a singleton slice for controller-runtime field indexing.
func indexStringValue(value string) []string {
	if value == "" {
		return nil
	}
	return []string{value}
}

// extractEngramTemplateName emits the Engram's spec.templateRef.name so the
// manager can index Engrams by their referenced template
// (internal/setup/indexing.go:49,131-136,163-166).
//
// Behavior:
//   - Returns nil when the Engram has no template reference, preventing empty
//     values from entering the index (internal/setup/indexing.go:132-134).
//   - Emits a single-element slice containing the template name when present
//     (internal/setup/indexing.go:134-135).
//
// Arguments:
//   - rawObj client.Object: expected to be *bubushv1alpha1.Engram because
//     SetupIndexers wires this extractor to Engram objects
//     (internal/setup/indexing.go:49,163-166). The return value feeds into
//     controller-runtime's `IndexField`.
//
// Returns:
//   - []string: nil when no template reference exists, otherwise one entry with
//     the template name.
//
// Side Effects:
//   - None; the helper simply reads the Engram spec.
//
// Notes / Gotchas:
//   - Template names are cluster-scoped, so no namespace prefix is applied.
func extractEngramTemplateName(rawObj client.Object) []string {
	engram := rawObj.(*bubushv1alpha1.Engram)
	return indexStringValue(engram.Spec.TemplateRef.Name)
}

// indexNamespacedReferenceKey builds a `<namespace>/<name>` index key from an ObjectReference.
//
// Behavior:
//   - Returns nil if ref is nil or ref.Name is empty, preventing empty index entries.
//   - Uses refs.ResolveNamespace to determine the target namespace, falling back
//     to the referencing object's namespace if ref.Namespace is unset.
//
// Arguments:
//   - referencing client.Object: the object containing the reference (provides default namespace).
//   - ref *refs.ObjectReference: the reference to extract namespace/name from.
//
// Returns:
//   - []string: nil if reference is invalid; otherwise a single-element slice
//     containing the namespaced key formatted as `<namespace>/<name>`.
func indexNamespacedReferenceKey(referencing client.Object, ref *refs.ObjectReference) []string {
	if ref == nil || ref.Name == "" {
		return nil
	}
	namespace := refs.ResolveNamespace(referencing, ref)
	return []string{refs.NamespacedKey(namespace, ref.Name)}
}

// indexReferenceName returns the reference name when it exists, ensuring callers
// emit nil for empty names while reusing indexStringValue for slice allocation.
func indexReferenceName(ref *refs.ObjectReference) []string {
	if ref == nil {
		return nil
	}
	return indexStringValue(ref.Name)
}

// addNamespacedReference inserts the `<namespace>/<name>` key for ref into target.
func addNamespacedReference(
	target map[string]struct{},
	referencing client.Object,
	ref *refs.ObjectReference,
) {
	if ref == nil || ref.Name == "" {
		return
	}
	namespace := refs.ResolveNamespace(referencing, ref)
	target[refs.NamespacedKey(namespace, ref.Name)] = struct{}{}
}

// sortedKeysFromSet converts a set of strings into a deterministically ordered slice.
func sortedKeysFromSet(values map[string]struct{}) []string {
	if len(values) == 0 {
		return nil
	}
	out := make([]string, 0, len(values))
	for v := range values {
		out = append(out, v)
	}
	sort.Strings(out)
	return out
}

func collectStoryStepReferenceKeys(
	story *bubushv1alpha1.Story,
	collect func(*bubushv1alpha1.Story, *bubushv1alpha1.Step, map[string]struct{}),
) []string {
	if len(story.Spec.Steps) == 0 {
		return nil
	}
	nameSet := make(map[string]struct{})
	for i := range story.Spec.Steps {
		collect(story, &story.Spec.Steps[i], nameSet)
	}
	return sortedKeysFromSet(nameSet)
}

// extractStepRunEngramRef returns the namespaced Engram reference for a
// StepRun so controller-runtime can build the spec.engramRef.key index
// (internal/setup/indexing.go:52).
//
// Behavior:
//   - Skips indexing when the StepRun lacks an EngramRef or name.
//   - Resolves the namespace via refs.ResolveNamespace and formats the key
//     using refs.NamespacedKey.
//
// Arguments:
//   - rawObj client.Object: expected to be *runsv1alpha1.StepRun.
//
// Returns:
//   - []string: nil when no EngramRef is set; otherwise one entry formatted as
//     `<namespace>/<name>`.
func extractStepRunEngramRef(rawObj client.Object) []string {
	stepRun := rawObj.(*runsv1alpha1.StepRun)
	if stepRun.Spec.EngramRef == nil {
		return nil
	}
	return indexNamespacedReferenceKey(stepRun, &stepRun.Spec.EngramRef.ObjectReference)
}

// extractStepRunStoryRunRef returns the StepRun's namespaced StoryRun reference
// for indexing, emitting nil when the reference is unset
// (internal/setup/indexing.go:53,202-205).
//
// Behavior:
//   - Returns nil for missing StoryRun references so the indexer skips empty
//     entries (internal/setup/indexing.go:131-135,203-204).
//   - Emits a single-element slice containing the `<namespace>/<name>` key.
//
// Arguments:
//   - rawObj client.Object: expected to be *runsv1alpha1.StepRun. The returned
//     slice feeds controller-runtime's IndexField registration
//     (internal/setup/indexing.go:53,202-205).
//
// Returns:
//   - []string: nil when no StoryRunRef is set; otherwise one entry with the
//     referenced StoryRun namespaced key.
//
// Side Effects:
//   - None.
func extractStepRunStoryRunRef(rawObj client.Object) []string {
	stepRun := rawObj.(*runsv1alpha1.StepRun)
	return indexNamespacedReferenceKey(stepRun, &stepRun.Spec.StoryRunRef.ObjectReference)
}

// extractStoryRunImpulseRef exposes spec.impulseRef as a namespaced key so StoryRuns can be
// indexed by their triggering Impulse (internal/setup/indexing.go:55,230-236).
//
// Behavior:
//   - Returns nil when ImpulseRef is nil or lacks a name; otherwise emits a
//     singleton slice with the referenced Impulse namespaced key (internal/setup/indexing.go:230-236).
//
// Arguments:
//   - rawObj client.Object: expected to be *runsv1alpha1.StoryRun.
//     controller-runtime invokes this extractor while registering the
//     storyRunImpulseIndexField (internal/setup/indexing.go:55,155-161).
//
// Returns:
//   - []string: nil for StoryRuns without an ImpulseRef; otherwise one entry
//     containing the Impulse namespaced key.
//
// Side Effects:
//   - None.
func extractStoryRunImpulseRef(rawObj client.Object) []string {
	storyRun := rawObj.(*runsv1alpha1.StoryRun)
	var ref *refs.ObjectReference
	if storyRun.Spec.ImpulseRef != nil {
		ref = &storyRun.Spec.ImpulseRef.ObjectReference
	}
	return indexNamespacedReferenceKey(storyRun, ref)
}

// extractImpulseTemplateName exposes an Impulse's spec.templateRef.name so the
// manager can index Impulses by their referenced template
// (internal/setup/indexing.go:57,131-136,259-262).
//
// Behavior:
//   - Returns nil when the Impulse has no template reference.
//   - Emits a single-element slice containing the cluster-scoped template name
//     when present (internal/setup/indexing.go:131-136,259-262).
//
// Arguments:
//   - rawObj client.Object: expected to be *bubushv1alpha1.Impulse. The
//     returned slice feeds mgr.GetFieldIndexer().IndexField.
//
// Returns:
//   - []string with zero or one entry representing the template name.
//
// Side Effects:
//   - None; the helper only reads the Impulse spec.
//
// Notes / Gotchas:
//   - ImpulseTemplates are cluster-scoped, so no namespace prefix is added.
func extractImpulseTemplateName(rawObj client.Object) []string {
	impulse := rawObj.(*bubushv1alpha1.Impulse)
	return indexStringValue(impulse.Spec.TemplateRef.Name)
}

// extractStoryRunStoryRefName returns spec.storyRef.name for StoryRuns so the
// manager can index runs by their parent Story (internal/setup/indexing.go:59,283-286).
//
// Behavior:
//   - Returns nil when StoryRef.Name is empty.
//   - Emits a single-element slice containing the Story name; namespace is
//     currently omitted.
//
// Arguments:
//   - rawObj client.Object: expected to be *runsv1alpha1.StoryRun.
//
// Returns:
//   - []string with zero or one entry.
//
// Side Effects:
//   - None.
//
// Notes / Gotchas:
//   - No namespace is included, so cross-namespace references would collide.
func extractStoryRunStoryRefName(rawObj client.Object) []string {
	storyRun := rawObj.(*runsv1alpha1.StoryRun)
	return indexReferenceName(&storyRun.Spec.StoryRef.ObjectReference)
}

// extractStoryRunStoryRefKey builds a `<namespace>/<name>` key for a StoryRun's
// StoryRef so controllers can index StoryRuns by their parent story
// (internal/setup/indexing.go:61).
//
// Behavior:
//   - Skips indexing when StoryRef.Name is empty.
//   - Resolves the namespace via refs.ResolveNamespace and formats the key
//     using refs.NamespacedKey.
//
// Arguments:
//   - rawObj client.Object: expected to be *runsv1alpha1.StoryRun.
//
// Returns:
//   - []string: nil when no StoryRef is present; otherwise one namespaced key.
func extractStoryRunStoryRefKey(rawObj client.Object) []string {
	storyRun := rawObj.(*runsv1alpha1.StoryRun)
	return indexNamespacedReferenceKey(storyRun, &storyRun.Spec.StoryRef.ObjectReference)
}

// extractImpulseStoryRefName exposes an Impulse's spec.storyRef as a namespaced key so Stories
// can enqueue dependent Impulses via a field index
// (internal/setup/indexing.go:62,317-320).
//
// Behavior:
//   - Returns nil when StoryRef.Name is empty.
//   - Emits a single-element slice with the Story namespaced key.
//
// Arguments:
//   - rawObj client.Object: expected to be *bubushv1alpha1.Impulse.
//
// Returns:
//   - []string: nil or one entry containing the Story namespaced key.
//
// Side Effects:
//   - None.
func extractImpulseStoryRefName(rawObj client.Object) []string {
	impulse := rawObj.(*bubushv1alpha1.Impulse)
	return indexNamespacedReferenceKey(impulse, &impulse.Spec.StoryRef.ObjectReference)
}

// extractStoryStepEngramRefs collects namespaced Engram references from all
// story steps so controllers can index Stories by the Engrams they invoke
// (internal/setup/indexing.go:65).
//
// Behavior:
//   - Returns nil when a Story has no steps or none reference Engrams.
//   - Deduplicates references by namespace/name before emitting the slice.
//   - Sorts the final slice so index output ordering is deterministic.
//
// Arguments:
//   - rawObj client.Object: expected to be *bubushv1alpha1.Story.
//
// Returns:
//   - []string containing each `<namespace>/<name>` Engram reference, or nil.
//
// Side Effects:
//   - None; the helper only reads Story spec data.
func extractStoryStepEngramRefs(rawObj client.Object) []string {
	story := rawObj.(*bubushv1alpha1.Story)
	return collectStoryStepReferenceKeys(story, func(
		story *bubushv1alpha1.Story,
		step *bubushv1alpha1.Step,
		nameSet map[string]struct{},
	) {
		if step.Ref != nil {
			addNamespacedReference(nameSet, story, &step.Ref.ObjectReference)
		}
	})
}

// extractStoryExecuteStoryRefs scans execute-story steps and emits the
// namespaced storyRef targets so controller-runtime can index cascading stories
// (internal/setup/indexing.go:66).
//
// Behavior:
//   - Returns nil when no execute-story steps exist.
//   - Skips steps with malformed JSON or blank storyRef names, logging JSON
//     failures at V(1) for operator visibility.
//   - Sorts the final slice so index output ordering is deterministic.
//
// Arguments:
//   - rawObj client.Object: expected to be *bubushv1alpha1.Story.
//
// Returns:
//   - []string containing `<namespace>/<name>` keys for each referenced Story,
//     or nil.
func extractStoryExecuteStoryRefs(rawObj client.Object) []string {
	story := rawObj.(*bubushv1alpha1.Story)
	return collectStoryStepReferenceKeys(story, func(
		story *bubushv1alpha1.Story,
		step *bubushv1alpha1.Step,
		nameSet map[string]struct{},
	) {
		if step.Type != enums.StepTypeExecuteStory || step.With == nil || len(step.With.Raw) == 0 {
			return
		}
		var withBlock struct {
			StoryRef struct {
				Name      string `json:"name"`
				Namespace string `json:"namespace,omitempty"`
			} `json:"storyRef"`
		}
		if err := json.Unmarshal(step.With.Raw, &withBlock); err != nil {
			setupLog.V(1).Info(
				"Skipping execute-story step with invalid with JSON",
				"story", client.ObjectKeyFromObject(story).String(),
				"step", step.Name,
				"error", err.Error(),
			)
			return
		}
		if withBlock.StoryRef.Name == "" {
			return
		}
		ref := refs.ObjectReference{Name: withBlock.StoryRef.Name}
		if withBlock.StoryRef.Namespace != "" {
			ns := withBlock.StoryRef.Namespace
			ref.Namespace = &ns
		}
		addNamespacedReference(nameSet, story, &ref)
	})
}

// extractStoryTransportRefs collects all transport references from a Story's
// spec.transports array for field indexing.
//
// Behavior:
//   - Returns nil when the Story has no transports defined.
//   - Trims whitespace from each transportRef before including it.
//   - Skips empty transportRef values after trimming.
//
// Arguments:
//   - rawObj client.Object: expected to be *bubushv1alpha1.Story.
//
// Returns:
//   - []string: nil when no valid transport refs exist; otherwise a slice of
//     trimmed transport reference names.
func extractStoryTransportRefs(rawObj client.Object) []string {
	story := rawObj.(*bubushv1alpha1.Story)
	if len(story.Spec.Transports) == 0 {
		return nil
	}
	transportSet := make(map[string]struct{})
	for i := range story.Spec.Transports {
		ref := strings.TrimSpace(story.Spec.Transports[i].TransportRef)
		if ref != "" {
			transportSet[ref] = struct{}{}
		}
	}
	return sortedKeysFromSet(transportSet)
}

// extractTransportBindingTransportRef extracts the transport reference from a
// TransportBinding for field indexing.
//
// Behavior:
//   - Trims whitespace from spec.transportRef before returning.
//   - Returns nil if the transportRef is empty after trimming.
//
// Arguments:
//   - rawObj client.Object: expected to be *transportv1alpha1.TransportBinding.
//
// Returns:
//   - []string: nil when transportRef is empty; otherwise a single-element slice
//     containing the trimmed transport reference name.
func extractTransportBindingTransportRef(rawObj client.Object) []string {
	binding := rawObj.(*transportv1alpha1.TransportBinding)
	ref := strings.TrimSpace(binding.Spec.TransportRef)
	if ref == "" {
		return nil
	}
	refObj := refs.ObjectReference{Name: ref}
	return indexNamespacedReferenceKey(binding, &refObj)
}

// extractEngramTemplateDescription extracts the description from an EngramTemplate
// for field indexing, using a sentinel value when no description is set.
//
// Behavior:
//   - Returns the template's Spec.Description if non-empty.
//   - Returns the sentinel value "no-description" when Description is empty,
//     allowing queries to find templates without descriptions.
//
// Arguments:
//   - obj client.Object: expected to be *catalogv1alpha1.EngramTemplate.
//
// Returns:
//   - []string: always returns a single-element slice containing either the
//     description or the sentinel value.
//
// Notes:
//   - The sentinel approach differs from other extractors that return nil for
//     empty values; this enables explicit filtering on "no description" templates.
func extractEngramTemplateDescription(obj client.Object) []string {
	template := obj.(*catalogv1alpha1.EngramTemplate)
	if template.Spec.Description != "" {
		return []string{template.Spec.Description}
	}
	return []string{IndexSentinelNoDescription}
}

// extractEngramTemplateVersion extracts the version from an EngramTemplate
// for field indexing, using a sentinel value when no version is set.
//
// Behavior:
//   - Returns the template's Spec.Version if non-empty.
//   - Returns the sentinel value "no-version" when Version is empty,
//     allowing queries to find templates without versions.
//
// Arguments:
//   - obj client.Object: expected to be *catalogv1alpha1.EngramTemplate.
//
// Returns:
//   - []string: always returns a single-element slice containing either the
//     version or the sentinel value.
func extractEngramTemplateVersion(obj client.Object) []string {
	template := obj.(*catalogv1alpha1.EngramTemplate)
	if template.Spec.Version != "" {
		return []string{template.Spec.Version}
	}
	return []string{IndexSentinelNoVersion}
}
