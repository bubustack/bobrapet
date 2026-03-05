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

package runs

import (
	"context"
	"fmt"
	"maps"
	"sort"
	"strings"

	catalogv1alpha1 "github.com/bubustack/bobrapet/api/catalog/v1alpha1"
	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	bubuv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/pkg/kubeutil"
	"github.com/bubustack/bobrapet/pkg/logging"
	"github.com/bubustack/bobrapet/pkg/metrics"
	runsidentity "github.com/bubustack/bobrapet/pkg/runs/identity"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

const managedStorageAnnotationKey = "runs.bubustack.io/managed-storage-annotations"

// RBACManager handles the reconciliation of RBAC resources for a StoryRun.
type RBACManager struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
}

// NewRBACManager creates a new RBACManager.
//
// Behavior:
//   - Wraps the provided client and scheme for RBAC resource management.
//
// Arguments:
//   - k8sClient client.Client: Kubernetes client for CRUD operations.
//   - scheme *runtime.Scheme: scheme for owner reference handling.
//
// Returns:
//   - *RBACManager: ready for RBAC reconciliation.
func NewRBACManager(k8sClient client.Client, scheme *runtime.Scheme) *RBACManager {
	return &RBACManager{Client: k8sClient, Scheme: scheme}
}

// Reconcile ensures the necessary ServiceAccount, Role, and RoleBinding exist for the StoryRun.
//
// Behavior:
//   - Fetches the parent Story for storage annotations (ignores NotFound).
//   - Reconciles ServiceAccount, Role, and RoleBinding with StoryRun as owner.
//   - Reconciles additional RoleBindings requested by Engram templates/overrides.
//
// Arguments:
//   - ctx context.Context: propagated to all client operations.
//   - storyRun *runsv1alpha1.StoryRun: the StoryRun requiring RBAC.
//
// Returns:
//   - error: nil on success, or first encountered error.
//
// Side Effects:
//   - Creates/updates SA, Role, and RoleBinding in StoryRun namespace.
//   - Logs operations and emits Kubernetes events.
func (r *RBACManager) Reconcile(ctx context.Context, storyRun *runsv1alpha1.StoryRun) error {
	log := logging.NewReconcileLogger(ctx, "storyrun-rbac")
	story, err := r.getStoryForRun(ctx, storyRun)
	if err != nil {
		if apierrors.IsNotFound(err) {
			log.WithStoryRun(storyRun).Info("Parent Story not found; continuing RBAC reconciliation without story-scoped annotations")
		} else {
			log.WithStoryRun(storyRun).Error(err, "Failed to fetch parent Story for RBAC provisioning")
			return fmt.Errorf("failed to get Story %s for StoryRun %s: %w",
				storyRun.Spec.StoryRef.ToNamespacedName(storyRun), storyRun.Name, err)
		}
		story = nil
	}

	saName := runsidentity.ServiceAccountName(storyRun.Name)
	if err := r.reconcileServiceAccount(ctx, storyRun, story, saName, log); err != nil {
		return err
	}
	extraRules, err := r.collectStoryRBACRules(ctx, storyRun, story, log)
	if err != nil {
		return err
	}
	if err := r.reconcileRole(ctx, storyRun, saName, extraRules, log); err != nil {
		return err
	}
	if err := r.reconcileRoleBinding(ctx, storyRun, saName, log); err != nil {
		return err
	}
	return nil
}

// reconcileServiceAccount ensures the per-StoryRun ServiceAccount ("<storyRun>-engram-runner")
// exists, copies Story-provided storage annotations, and sets the StoryRun as owner so garbage
// collection removes the ServiceAccount with the StoryRun.
// Behavior:
//   - Calls controllerutil.CreateOrUpdate with a mutation callback that lazily allocates the
//     annotations map, copies Story.Spec.Policy.Storage.S3.Authentication.ServiceAccountAnnotations,
//     prunes annotations removed from the Story, and stamps the StoryRun owner reference.
//   - Emits Normal events + RBAC metrics whenever the ServiceAccount mutates so annotation churn
//     is observable without scraping debug logs, and logs the exact keys applied/pruned so IAM
//     policy rollouts remain auditable.
//
// Inputs:
//   - ctx/storyRun/story/saName/log forwarded from RBACManager.Reconcile.
//
// Returns:
//   - error wrapping CreateOrUpdate failures; nil on success.
//
// Side Effects:
//   - Creates or updates the namespaced ServiceAccount and its annotations.
//
// Notes:
//   - Contractually tied to config.Resolver.ResolveExecutionConfig, which defaults the same
//     `<storyRun>-engram-runner` name when no ServiceAccount is provided.
func (r *RBACManager) reconcileServiceAccount(ctx context.Context, storyRun *runsv1alpha1.StoryRun, story *bubuv1alpha1.Story, saName string, log *logging.ReconcileLogger) error {
	sa := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: saName, Namespace: storyRun.Namespace}}
	desiredAnnotations := storyStorageAnnotations(story)
	op, err := controllerutil.CreateOrUpdate(ctx, r.Client, sa, func() error {
		if sa.Annotations == nil {
			sa.Annotations = make(map[string]string)
		}
		if removed := pruneManagedAnnotations(sa, desiredAnnotations); len(removed) > 0 {
			log.Info("Pruned stale ServiceAccount annotations", "annotations", removed)
		}
		if len(desiredAnnotations) > 0 {
			log.Info("Applying S3 ServiceAccount annotations from StoragePolicy",
				"count", len(desiredAnnotations),
				"keys", sortedAnnotationKeys(desiredAnnotations),
			)
		}
		maps.Copy(sa.Annotations, desiredAnnotations)
		if len(desiredAnnotations) == 0 {
			delete(sa.Annotations, managedStorageAnnotationKey)
		} else {
			sa.Annotations[managedStorageAnnotationKey] = encodeManagedAnnotationKeys(desiredAnnotations)
		}
		return controllerutil.SetOwnerReference(storyRun, sa, r.Scheme)
	})
	if err != nil {
		return fmt.Errorf("failed to create or update ServiceAccount: %w", err)
	}
	metrics.RecordStoryRunRBACOperation("ServiceAccount", operationResultLabel(op))
	if op != controllerutil.OperationResultNone {
		log.Info("Reconciled ServiceAccount for Engram runner", "serviceAccount", saName, "operation", op)
		r.emitRBACEvent(storyRun, rbacEventReason("ServiceAccount", op), fmt.Sprintf("ServiceAccount %s %s", saName, op))
	}
	return nil
}

// reconcileRole converges the StoryRun-scoped Role (named after the Engram runner ServiceAccount)
// on the verbs needed to read/patch StepRuns and TransportBindings (internal/controller/runs/rbac.go:131-164).
// Behavior:
//   - Builds the fixed PolicyRule set (get/watch stepruns, patch/update statuses, read/update
//     transportbindings) and calls pkg/kubeutil.EnsureRole so owner references stay consistent.
//   - Records RBAC operation metrics keyed by controller-runtime OperationResult to highlight
//     unexpected Role churn.
//
// Inputs:
//   - ctx/storyRun/saName/log forwarded from RBACManager.Reconcile.
//
// Returns:
//   - error if EnsureRole fails; nil otherwise.
//
// Side Effects:
//   - Creates or updates the Role in the StoryRun namespace; logs/events when an actual change occurs.
//
// Notes:
//   - Shares helpers with Impulse RBAC, so edits must keep both controllers aligned.
func (r *RBACManager) reconcileRole(ctx context.Context, storyRun *runsv1alpha1.StoryRun, saName string, extraRules []rbacv1.PolicyRule, log *logging.ReconcileLogger) error {
	rules := []rbacv1.PolicyRule{
		{APIGroups: []string{"runs.bubustack.io"}, Resources: []string{"stepruns"}, Verbs: []string{"get", "watch"}},
		{APIGroups: []string{"runs.bubustack.io"}, Resources: []string{"stepruns/status"}, Verbs: []string{"patch", "update"}},
		{APIGroups: []string{"transport.bubustack.io"}, Resources: []string{"transportbindings"}, Verbs: []string{"get", "list", "watch"}},
		{APIGroups: []string{"transport.bubustack.io"}, Resources: []string{"transportbindings/status"}, Verbs: []string{"get", "patch", "update"}},
		{APIGroups: []string{""}, Resources: []string{"configmaps", "secrets"}, Verbs: []string{"get"}},
	}
	if len(extraRules) > 0 {
		rules = mergePolicyRules(rules, extraRules)
	}
	op, err := kubeutil.EnsureRole(ctx, r.Client, r.Scheme, storyRun, saName, storyRun.Namespace, rules, kubeutil.OwnerReference)
	if err != nil {
		return fmt.Errorf("failed to create or update Role: %w", err)
	}
	metrics.RecordStoryRunRBACOperation("Role", operationResultLabel(op))
	if op != controllerutil.OperationResultNone {
		log.Info("Reconciled Role for Engram runner", "role", saName, "operation", op)
		r.emitRBACEvent(storyRun, rbacEventReason("Role", op), fmt.Sprintf("Role %s %s", saName, op))
	}
	return nil
}

// reconcileRoleBinding binds the StoryRun’s "<storyRun>-engram-runner" ServiceAccount to the
// matching Role using pkg/kubeutil.EnsureRoleBinding (internal/controller/runs/rbac.go:166-225).
// Behavior:
//   - Constructs a single ServiceAccount subject in the StoryRun namespace, builds the RoleRef,
//     and delegates CreateOrUpdate + owner reference stamping to pkg/kubeutil.
//   - Prunes and logs stale subjects before reapplying owner references so future RBAC edits do
//     not leave unexpected ServiceAccounts bound to the Role.
//
// Inputs:
//   - ctx/storyRun/saName/log from RBACManager.Reconcile.
//
// Returns:
//   - error wrapping EnsureRoleBinding failures.
//
// Side Effects:
//   - Creates/updates the RoleBinding; logs/events when a create/update occurred.
//
// Notes:
//   - Must stay in lockstep with Impulse RBAC bindings and config.Resolver’s ServiceAccount defaults.
func (r *RBACManager) reconcileRoleBinding(ctx context.Context, storyRun *runsv1alpha1.StoryRun, saName string, log *logging.ReconcileLogger) error {
	subjects := []rbacv1.Subject{
		kubeutil.StoryRunEngramRunnerSubject(storyRun.Name, storyRun.Namespace),
	}
	roleRef := rbacv1.RoleRef{
		APIGroup: rbacv1.GroupName,
		Kind:     "Role",
		Name:     saName,
	}
	var pruned []string
	existing := &rbacv1.RoleBinding{}
	if err := r.Get(ctx, types.NamespacedName{Name: saName, Namespace: storyRun.Namespace}, existing); err == nil {
		pruned = diffSubjects(existing.Subjects, subjects)
	} else if !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to inspect existing RoleBinding subjects: %w", err)
	}
	op, err := kubeutil.EnsureRoleBinding(ctx, r.Client, r.Scheme, storyRun, saName, storyRun.Namespace, roleRef, subjects, kubeutil.OwnerReference)
	if err != nil {
		return fmt.Errorf("failed to create or update RoleBinding: %w", err)
	}
	metrics.RecordStoryRunRBACOperation("RoleBinding", operationResultLabel(op))
	if len(pruned) > 0 {
		log.Info("Pruned stale RoleBinding subjects", "roleBinding", saName, "removed", pruned)
	}
	if op != controllerutil.OperationResultNone {
		log.Info("Reconciled RoleBinding for Engram runner", "roleBinding", saName, "operation", op)
		r.emitRBACEvent(storyRun, rbacEventReason("RoleBinding", op), fmt.Sprintf("RoleBinding %s %s", saName, op))
	}
	return nil
}

func (r *RBACManager) collectStoryRBACRules(ctx context.Context, storyRun *runsv1alpha1.StoryRun, story *bubuv1alpha1.Story, log *logging.ReconcileLogger) ([]rbacv1.PolicyRule, error) {
	if story == nil {
		return nil, nil
	}

	engramCache := map[types.NamespacedName]*bubuv1alpha1.Engram{}
	templateCache := map[string]*catalogv1alpha1.EngramTemplate{}
	var collected []rbacv1.PolicyRule

	for _, step := range story.Spec.Steps {
		if step.Ref == nil {
			continue
		}
		key := step.Ref.ToNamespacedName(storyRun)
		engram := engramCache[key]
		if engram == nil {
			engram = &bubuv1alpha1.Engram{}
			if err := r.Get(ctx, key, engram); err != nil {
				if apierrors.IsNotFound(err) {
					log.WithStoryRun(storyRun).Info("Referenced Engram not found while collecting RBAC policies", "engram", key.String())
					continue
				}
				return nil, err
			}
			engramCache[key] = engram
		}

		var template *catalogv1alpha1.EngramTemplate
		if engram.Spec.TemplateRef.Name != "" {
			template = templateCache[engram.Spec.TemplateRef.Name]
			if template == nil {
				template = &catalogv1alpha1.EngramTemplate{}
				if err := r.Get(ctx, types.NamespacedName{Name: engram.Spec.TemplateRef.Name}, template); err != nil {
					if apierrors.IsNotFound(err) {
						log.WithStoryRun(storyRun).Info("Referenced EngramTemplate not found while collecting RBAC policies", "template", engram.Spec.TemplateRef.Name)
						continue
					}
					return nil, err
				}
				templateCache[engram.Spec.TemplateRef.Name] = template
			}
		}

		if engram.Spec.Overrides != nil && engram.Spec.Overrides.RBAC != nil {
			collected = append(collected, engram.Spec.Overrides.RBAC.Rules...)
			continue
		}
		if template != nil && template.Spec.Execution != nil && template.Spec.Execution.RBAC != nil {
			collected = append(collected, template.Spec.Execution.RBAC.Rules...)
		}
	}

	if len(collected) == 0 {
		return nil, nil
	}
	return mergePolicyRules(nil, collected), nil
}

// getStoryForRun fetches the parent Story for a StoryRun.
//
// Behavior:
//   - Uses storyRef to construct the namespaced key.
//   - Returns the Story or a wrapped error.
//
// Arguments:
//   - ctx context.Context: propagated to GET.
//   - srun *runsv1alpha1.StoryRun: provides the story reference.
//
// Returns:
//   - *bubuv1alpha1.Story: the parent Story.
//   - error: nil on success, or GET errors.
func (r *RBACManager) getStoryForRun(ctx context.Context, srun *runsv1alpha1.StoryRun) (*bubuv1alpha1.Story, error) {
	var story bubuv1alpha1.Story
	key := types.NamespacedName{
		Name:      srun.Spec.StoryRef.Name,
		Namespace: srun.Spec.StoryRef.ToNamespacedName(srun).Namespace,
	}
	if err := r.Get(ctx, key, &story); err != nil {
		return nil, err
	}
	return &story, nil
}

// storyStorageAnnotations extracts S3 ServiceAccount annotations from Story policy.
//
// Behavior:
//   - Returns nil if Story or S3 auth config is missing.
//   - Returns a cloned map of ServiceAccountAnnotations.
//
// Arguments:
//   - story *bubuv1alpha1.Story: the Story to extract annotations from.
//
// Returns:
//   - map[string]string: cloned annotations, or nil.
func storyStorageAnnotations(story *bubuv1alpha1.Story) map[string]string {
	if story == nil ||
		story.Spec.Policy == nil ||
		story.Spec.Policy.Storage == nil ||
		story.Spec.Policy.Storage.S3 == nil {
		return nil
	}
	src := story.Spec.Policy.Storage.S3.Authentication.ServiceAccountAnnotations
	if len(src) == 0 {
		return nil
	}
	clone := make(map[string]string, len(src))
	maps.Copy(clone, src)
	return clone
}

// pruneManagedAnnotations removes stale managed annotations from a ServiceAccount.
//
// Behavior:
//   - Parses previously managed keys from managedStorageAnnotationKey.
//   - Removes keys not in the desired set.
//   - Returns sorted list of removed keys.
//
// Arguments:
//   - sa *corev1.ServiceAccount: the ServiceAccount to prune.
//   - desired map[string]string: the target annotations.
//
// Returns:
//   - []string: sorted list of removed annotation keys.
//
// Side Effects:
//   - Mutates sa.Annotations by deleting stale keys.
func pruneManagedAnnotations(sa *corev1.ServiceAccount, desired map[string]string) []string {
	current := parseManagedAnnotationKeys(sa.Annotations[managedStorageAnnotationKey])
	if len(current) == 0 {
		return nil
	}
	removed := make([]string, 0, len(current))
	for key := range current {
		if _, keep := desired[key]; keep {
			continue
		}
		delete(sa.Annotations, key)
		removed = append(removed, key)
	}
	sort.Strings(removed)
	return removed
}

// parseManagedAnnotationKeys parses a comma-separated list of managed keys.
//
// Behavior:
//   - Splits value on commas and trims whitespace.
//   - Returns a set of non-empty keys.
//
// Arguments:
//   - value string: comma-separated annotation keys.
//
// Returns:
//   - map[string]struct{}: set of parsed keys, or nil if empty.
func parseManagedAnnotationKeys(value string) map[string]struct{} {
	if value == "" {
		return nil
	}
	result := make(map[string]struct{})
	for token := range strings.SplitSeq(value, ",") {
		if key := strings.TrimSpace(token); key != "" {
			result[key] = struct{}{}
		}
	}
	return result
}

// encodeManagedAnnotationKeys encodes annotation keys as a sorted comma-separated string.
//
// Behavior:
//   - Collects keys, sorts them, and joins with commas.
//
// Arguments:
//   - desired map[string]string: annotations to encode.
//
// Returns:
//   - string: sorted comma-separated keys.
func encodeManagedAnnotationKeys(desired map[string]string) string {
	keys := make([]string, 0, len(desired))
	for k := range desired {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return strings.Join(keys, ",")
}

// sortedAnnotationKeys returns sorted annotation keys for logging.
//
// Behavior:
//   - Returns nil for empty maps.
//   - Collects and sorts keys.
//
// Arguments:
//   - desired map[string]string: annotations to extract keys from.
//
// Returns:
//   - []string: sorted keys, or nil.
func sortedAnnotationKeys(desired map[string]string) []string {
	if len(desired) == 0 {
		return nil
	}
	keys := make([]string, 0, len(desired))
	for k := range desired {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// emitRBACEvent emits a Kubernetes event for RBAC operations.
//
// Behavior:
//   - Returns early if Recorder is nil.
//   - Emits a Normal event with the given reason and message.
//
// Arguments:
//   - obj client.Object: the object to attach the event to.
//   - reason string: event reason (e.g., "ServiceAccountCreated").
//   - message string: human-readable message.
//
// Returns:
//   - None.
//
// Side Effects:
//   - Emits a Kubernetes event.
func (r *RBACManager) emitRBACEvent(obj client.Object, reason, message string) {
	if r.Recorder == nil {
		return
	}
	r.Recorder.Event(obj, corev1.EventTypeNormal, reason, message)
}

// rbacEventReason constructs an event reason from kind and operation result.
//
// Behavior:
//   - Appends "Created", "Updated", or "UpdatedStatus" based on operation.
//   - Returns just the kind for OperationResultNone.
//
// Arguments:
//   - kind string: resource kind (e.g., "ServiceAccount").
//   - op controllerutil.OperationResult: the operation that occurred.
//
// Returns:
//   - string: the event reason.
func rbacEventReason(kind string, op controllerutil.OperationResult) string {
	switch op {
	case controllerutil.OperationResultCreated:
		return kind + "Created"
	case controllerutil.OperationResultUpdated:
		return kind + "Updated"
	case controllerutil.OperationResultUpdatedStatus:
		return kind + "UpdatedStatus"
	default:
		return kind
	}
}

// diffSubjects finds subjects in current but not in desired.
//
// Behavior:
//   - Builds a set from desired subjects.
//   - Returns sorted list of current subjects not in desired.
//
// Arguments:
//   - current, desired []rbacv1.Subject: subject lists to compare.
//
// Returns:
//   - []string: sorted keys of removed subjects.
func diffSubjects(current, desired []rbacv1.Subject) []string {
	if len(current) == 0 {
		return nil
	}
	desiredSet := make(map[string]struct{}, len(desired))
	for _, sub := range desired {
		desiredSet[subjectKey(sub)] = struct{}{}
	}
	removed := make([]string, 0, len(current))
	for _, sub := range current {
		key := subjectKey(sub)
		if _, keep := desiredSet[key]; keep {
			continue
		}
		removed = append(removed, key)
	}
	sort.Strings(removed)
	return removed
}

// subjectKey generates a unique key for a Subject.
//
// Behavior:
//   - Formats as "kind/namespace/name" (lowercase kind).
//   - Uses "-" for empty namespace (cluster-scoped).
//
// Arguments:
//   - sub rbacv1.Subject: the subject to key.
//
// Returns:
//   - string: the unique key.
func subjectKey(sub rbacv1.Subject) string {
	namespace := sub.Namespace
	if namespace == "" {
		namespace = "-"
	}
	return fmt.Sprintf("%s/%s/%s", strings.ToLower(sub.Kind), namespace, sub.Name)
}

func mergePolicyRules(base []rbacv1.PolicyRule, extras []rbacv1.PolicyRule) []rbacv1.PolicyRule {
	merged := make([]rbacv1.PolicyRule, 0, len(base)+len(extras))
	merged = append(merged, base...)
	merged = append(merged, extras...)

	unique := map[string]rbacv1.PolicyRule{}
	for _, rule := range merged {
		unique[policyRuleKey(rule)] = rule
	}
	out := make([]rbacv1.PolicyRule, 0, len(unique))
	for _, rule := range unique {
		out = append(out, rule)
	}
	sort.Slice(out, func(i, j int) bool {
		return policyRuleKey(out[i]) < policyRuleKey(out[j])
	})
	return out
}

func policyRuleKey(rule rbacv1.PolicyRule) string {
	apiGroups := append([]string(nil), rule.APIGroups...)
	resources := append([]string(nil), rule.Resources...)
	resourceNames := append([]string(nil), rule.ResourceNames...)
	verbs := append([]string(nil), rule.Verbs...)
	nonResource := append([]string(nil), rule.NonResourceURLs...)

	sort.Strings(apiGroups)
	sort.Strings(resources)
	sort.Strings(resourceNames)
	sort.Strings(verbs)
	sort.Strings(nonResource)

	return strings.Join([]string{
		strings.Join(apiGroups, ","),
		strings.Join(resources, ","),
		strings.Join(resourceNames, ","),
		strings.Join(verbs, ","),
		strings.Join(nonResource, ","),
	}, "|")
}

// operationResultLabel converts OperationResult to a metrics label string.
//
// Behavior:
//   - Returns the string representation.
//   - Uses "unchanged" for empty/None results.
//
// Arguments:
//   - op controllerutil.OperationResult: the operation result.
//
// Returns:
//   - string: label for metrics recording.
func operationResultLabel(op controllerutil.OperationResult) string {
	label := string(op)
	if label == "" {
		label = string(controllerutil.OperationResultNone)
	}
	return label
}
