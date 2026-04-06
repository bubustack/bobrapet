package runs

import (
	"bytes"
	"context"
	"io"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	catalogv1alpha1 "github.com/bubustack/bobrapet/api/catalog/v1alpha1"
	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	bubuv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/pkg/logging"
	"github.com/bubustack/bobrapet/pkg/refs"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/yaml"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	identity "github.com/bubustack/core/runtime/identity"
)

func TestRBACManagerReconcileCopiesStorageAnnotations(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := runsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add StepRun scheme: %v", err)
	}
	if err := bubuv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add Story scheme: %v", err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add core scheme: %v", err)
	}
	if err := rbacv1.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add rbac scheme: %v", err)
	}

	annotations := map[string]string{
		"eks.amazonaws.com/role-arn": "arn:aws:iam::123:role/runner",
	}
	story := &bubuv1alpha1.Story{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "demo-story",
			Namespace: "default",
		},
		Spec: bubuv1alpha1.StorySpec{
			Policy: &bubuv1alpha1.StoryPolicy{
				Storage: &bubuv1alpha1.StoragePolicy{
					S3: &bubuv1alpha1.S3StorageProvider{
						Authentication: bubuv1alpha1.S3Authentication{
							ServiceAccountAnnotations: annotations,
						},
					},
				},
			},
		},
	}

	storyRun := &runsv1alpha1.StoryRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "demo-storyrun",
			Namespace: "default",
		},
		Spec: runsv1alpha1.StoryRunSpec{
			StoryRef: refs.StoryReference{
				ObjectReference: refs.ObjectReference{
					Name: story.Name,
				},
			},
		},
	}

	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(story, storyRun).
		Build()

	manager := &RBACManager{
		Client: client,
		Scheme: scheme,
	}

	if err := manager.Reconcile(context.Background(), storyRun); err != nil {
		t.Fatalf("rbac reconcile failed: %v", err)
	}

	saName := identity.StoryRunEngramRunnerServiceAccount(storyRun.Name)
	var sa corev1.ServiceAccount
	if err := client.Get(context.Background(), types.NamespacedName{Name: saName, Namespace: storyRun.Namespace}, &sa); err != nil {
		t.Fatalf("failed to fetch reconciled service account: %v", err)
	}

	for key, val := range annotations {
		if sa.Annotations[key] != val {
			t.Fatalf("expected annotation %s=%s, got %s", key, val, sa.Annotations[key])
		}
	}
	if _, ok := sa.Annotations[managedStorageAnnotationKey]; !ok {
		t.Fatalf("expected managed annotation marker to be set")
	}

	expectedKeys := sets.NewString()
	for key := range annotations {
		expectedKeys.Insert(key)
	}
	if got := sets.NewString(strings.Split(sa.Annotations[managedStorageAnnotationKey], ",")...); !got.Equal(expectedKeys) {
		t.Fatalf("expected managed keys %v, got %v", expectedKeys.List(), got.List())
	}
}

func TestRBACManagerReconcilePatchesExistingServiceAccountAnnotations(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	if err := runsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add StepRun scheme: %v", err)
	}
	if err := bubuv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add Story scheme: %v", err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add core scheme: %v", err)
	}
	if err := rbacv1.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add rbac scheme: %v", err)
	}

	annotations := map[string]string{
		"eks.amazonaws.com/role-arn": "arn:aws:iam::123:role/runner",
	}
	story := &bubuv1alpha1.Story{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "demo-story",
			Namespace: "default",
		},
		Spec: bubuv1alpha1.StorySpec{
			Policy: &bubuv1alpha1.StoryPolicy{
				Storage: &bubuv1alpha1.StoragePolicy{
					S3: &bubuv1alpha1.S3StorageProvider{
						Authentication: bubuv1alpha1.S3Authentication{
							ServiceAccountAnnotations: annotations,
						},
					},
				},
			},
		},
	}
	storyRun := &runsv1alpha1.StoryRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "demo-storyrun",
			Namespace: "default",
			UID:       types.UID("storyrun-uid"),
		},
		Spec: runsv1alpha1.StoryRunSpec{
			StoryRef: refs.StoryReference{
				ObjectReference: refs.ObjectReference{Name: story.Name},
			},
		},
	}

	saName := identity.StoryRunEngramRunnerServiceAccount(storyRun.Name)
	existing := &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:      saName,
			Namespace: storyRun.Namespace,
			Annotations: map[string]string{
				managedStorageAnnotationKey: "stale.example/key",
				"stale.example/key":         "stale",
				"preserve.example/key":      "keep",
			},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: runsv1alpha1.GroupVersion.String(),
				Kind:       "StoryRun",
				Name:       storyRun.Name,
				UID:        storyRun.UID,
			}},
		},
	}

	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(story, storyRun, existing).
		Build()

	manager := &RBACManager{
		Client: client,
		Scheme: scheme,
	}

	if err := manager.Reconcile(context.Background(), storyRun); err != nil {
		t.Fatalf("rbac reconcile failed: %v", err)
	}

	var sa corev1.ServiceAccount
	if err := client.Get(context.Background(), types.NamespacedName{Name: saName, Namespace: storyRun.Namespace}, &sa); err != nil {
		t.Fatalf("failed to fetch reconciled service account: %v", err)
	}

	if _, ok := sa.Annotations["stale.example/key"]; ok {
		t.Fatalf("expected stale managed annotation to be pruned")
	}
	if sa.Annotations["preserve.example/key"] != "keep" {
		t.Fatalf("expected unmanaged annotation to be preserved, got %q", sa.Annotations["preserve.example/key"])
	}
	for key, val := range annotations {
		if sa.Annotations[key] != val {
			t.Fatalf("expected annotation %s=%s, got %s", key, val, sa.Annotations[key])
		}
	}
}

func TestRBACManagerReconcileRejectsUnownedServiceAccountCollision(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	if err := runsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add StoryRun scheme: %v", err)
	}
	if err := bubuv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add Story scheme: %v", err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add core scheme: %v", err)
	}
	if err := rbacv1.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add rbac scheme: %v", err)
	}

	story := &bubuv1alpha1.Story{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "demo-story",
			Namespace: "default",
		},
	}
	storyRun := &runsv1alpha1.StoryRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "demo-storyrun",
			Namespace: "default",
			UID:       types.UID("storyrun-uid"),
		},
		Spec: runsv1alpha1.StoryRunSpec{
			StoryRef: refs.StoryReference{
				ObjectReference: refs.ObjectReference{Name: story.Name},
			},
		},
	}

	saName := identity.StoryRunEngramRunnerServiceAccount(storyRun.Name)
	existing := &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:      saName,
			Namespace: storyRun.Namespace,
			Annotations: map[string]string{
				"preserve.example/key": "keep",
			},
		},
	}

	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(story, storyRun, existing).
		Build()

	manager := &RBACManager{
		Client: client,
		Scheme: scheme,
	}

	err := manager.Reconcile(context.Background(), storyRun)
	if err == nil {
		t.Fatalf("expected unmanaged serviceaccount collision to be rejected")
	}
	if !strings.Contains(err.Error(), "refusing to adopt existing ServiceAccount") {
		t.Fatalf("expected collision error, got %v", err)
	}

	var sa corev1.ServiceAccount
	if getErr := client.Get(context.Background(), types.NamespacedName{Name: saName, Namespace: storyRun.Namespace}, &sa); getErr != nil {
		t.Fatalf("failed to fetch existing service account: %v", getErr)
	}
	if sa.Annotations["preserve.example/key"] != "keep" {
		t.Fatalf("expected existing serviceaccount to remain unchanged, got annotations %v", sa.Annotations)
	}
}

func TestSanitizeStoryRBACRules(t *testing.T) {
	t.Parallel()

	allowedSecret := rbacv1.PolicyRule{
		APIGroups:     []string{""},
		Resources:     []string{"secrets"},
		Verbs:         []string{"get"},
		ResourceNames: []string{"runner-secret"},
	}
	allowedConfigMap := rbacv1.PolicyRule{
		APIGroups:     []string{""},
		Resources:     []string{"configmaps"},
		Verbs:         []string{"get"},
		ResourceNames: []string{"runner-config"},
	}
	unsafeRules := []rbacv1.PolicyRule{
		{
			APIGroups:     []string{""},
			Resources:     []string{"secrets"},
			Verbs:         []string{"get"},
			ResourceNames: []string{},
		},
		{
			APIGroups:     []string{""},
			Resources:     []string{"secrets"},
			Verbs:         []string{"list"},
			ResourceNames: []string{"runner-secret"},
		},
		{
			APIGroups:     []string{"rbac.authorization.k8s.io"},
			Resources:     []string{"roles"},
			Verbs:         []string{"create"},
			ResourceNames: []string{"x"},
		},
		{
			APIGroups:       []string{""},
			Resources:       []string{"secrets"},
			Verbs:           []string{"get"},
			ResourceNames:   []string{"runner-secret"},
			NonResourceURLs: []string{"/metrics"},
		},
	}

	filtered := sanitizeStoryRBACRules(append([]rbacv1.PolicyRule{allowedSecret, allowedConfigMap}, unsafeRules...))
	if len(filtered) != 2 {
		t.Fatalf("expected 2 safe rules, got %d (%v)", len(filtered), filtered)
	}
	gotKeys := []string{policyRuleKey(filtered[0]), policyRuleKey(filtered[1])}
	wantKeys := []string{policyRuleKey(allowedSecret), policyRuleKey(allowedConfigMap)}
	slices.Sort(gotKeys)
	slices.Sort(wantKeys)
	if !slices.Equal(gotKeys, wantKeys) {
		t.Fatalf("unexpected sanitized rules: got=%v want=%v", gotKeys, wantKeys)
	}
}

func TestCollectStoryRBACRulesFiltersUnsafeRulesFromEngramOverrides(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	if err := runsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add runs scheme: %v", err)
	}
	if err := bubuv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add core scheme: %v", err)
	}
	if err := catalogv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add catalog scheme: %v", err)
	}
	if err := rbacv1.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add rbac scheme: %v", err)
	}

	story := &bubuv1alpha1.Story{
		ObjectMeta: metav1.ObjectMeta{Name: "demo-story", Namespace: "default"},
		Spec: bubuv1alpha1.StorySpec{
			Steps: []bubuv1alpha1.Step{
				{
					Name: "step-a",
					Ref: &refs.EngramReference{
						ObjectReference: refs.ObjectReference{Name: "engram-a"},
					},
				},
			},
		},
	}
	storyRun := &runsv1alpha1.StoryRun{
		ObjectMeta: metav1.ObjectMeta{Name: "demo-storyrun", Namespace: "default"},
		Spec: runsv1alpha1.StoryRunSpec{
			StoryRef: refs.StoryReference{ObjectReference: refs.ObjectReference{Name: story.Name}},
		},
	}
	engram := &bubuv1alpha1.Engram{
		ObjectMeta: metav1.ObjectMeta{Name: "engram-a", Namespace: "default"},
		Spec: bubuv1alpha1.EngramSpec{
			Overrides: &bubuv1alpha1.ExecutionOverrides{
				RBAC: &bubuv1alpha1.RBACRuleOverrides{
					Rules: []rbacv1.PolicyRule{
						{
							APIGroups:     []string{""},
							Resources:     []string{"secrets"},
							Verbs:         []string{"get"},
							ResourceNames: []string{"runner-secret"},
						},
						{
							APIGroups:     []string{""},
							Resources:     []string{"secrets"},
							Verbs:         []string{"list"},
							ResourceNames: []string{"runner-secret"},
						},
						{
							APIGroups:     []string{"rbac.authorization.k8s.io"},
							Resources:     []string{"roles"},
							Verbs:         []string{"create"},
							ResourceNames: []string{"x"},
						},
					},
				},
			},
		},
	}

	client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(story, storyRun, engram).Build()
	manager := &RBACManager{Client: client, Scheme: scheme}

	rules, err := manager.collectStoryRBACRules(context.Background(), storyRun, story, logging.NewReconcileLogger(context.Background(), "test"))
	if err != nil {
		t.Fatalf("collectStoryRBACRules failed: %v", err)
	}
	if len(rules) != 1 {
		t.Fatalf("expected exactly 1 safe dynamic rule, got %d (%v)", len(rules), rules)
	}
	got := rules[0]
	if !maps.Equal(ruleSliceToSet(got.APIGroups), ruleSliceToSet([]string{""})) {
		t.Fatalf("unexpected api groups: %v", got.APIGroups)
	}
	if !maps.Equal(ruleSliceToSet(got.Resources), ruleSliceToSet([]string{"secrets"})) {
		t.Fatalf("unexpected resources: %v", got.Resources)
	}
	if !maps.Equal(ruleSliceToSet(got.Verbs), ruleSliceToSet([]string{"get"})) {
		t.Fatalf("unexpected verbs: %v", got.Verbs)
	}
	if !maps.Equal(ruleSliceToSet(got.ResourceNames), ruleSliceToSet([]string{"runner-secret"})) {
		t.Fatalf("unexpected resource names: %v", got.ResourceNames)
	}
}

func TestGeneratedRBACManagerRoleLimitsDynamicRoleGrantVerbs(t *testing.T) {
	t.Parallel()

	data, err := os.ReadFile(filepath.Join("..", "..", "..", "config", "rbac", "role.yaml"))
	if err != nil {
		t.Fatalf("failed to read generated RBAC manifest: %v", err)
	}

	decoder := yaml.NewYAMLOrJSONDecoder(bytes.NewReader(data), 4096)
	found := false
	allowed := map[string]struct{}{
		"bind":     {},
		"create":   {},
		"escalate": {},
		"get":      {},
		"list":     {},
		"patch":    {},
		"watch":    {},
	}
	for {
		var cr rbacv1.ClusterRole
		if err := decoder.Decode(&cr); err != nil {
			if err == io.EOF {
				break
			}
			t.Fatalf("failed to decode generated RBAC manifest: %v", err)
		}
		if cr.Name != "manager-role" { //nolint:goconst
			continue
		}
		found = true
		for _, rule := range cr.Rules {
			if !containsString(rule.APIGroups, "rbac.authorization.k8s.io") || !containsString(rule.Resources, "roles") {
				continue
			}
			for _, verb := range rule.Verbs {
				if _, ok := allowed[verb]; !ok {
					t.Fatalf("generated manager-role grants unexpected role verb %q in rule %#v", verb, rule)
				}
			}
		}
		break
	}
	if !found {
		t.Fatal("manager-role not found in generated RBAC manifest")
	}
}

func TestGeneratedRBACManagerRoleAllowsCacheBackedRoleBindingReads(t *testing.T) {
	t.Parallel()

	data, err := os.ReadFile(filepath.Join("..", "..", "..", "config", "rbac", "role.yaml"))
	if err != nil {
		t.Fatalf("failed to read generated RBAC manifest: %v", err)
	}

	decoder := yaml.NewYAMLOrJSONDecoder(bytes.NewReader(data), 4096)
	found := false
	allowed := map[string]struct{}{
		"create": {},
		"get":    {},
		"list":   {},
		"patch":  {},
		"watch":  {},
	}
	required := map[string]struct{}{
		"get":   {},
		"list":  {},
		"watch": {},
	}
	for {
		var cr rbacv1.ClusterRole
		if err := decoder.Decode(&cr); err != nil {
			if err == io.EOF {
				break
			}
			t.Fatalf("failed to decode generated RBAC manifest: %v", err)
		}
		if cr.Name != "manager-role" {
			continue
		}
		found = true
		granted := map[string]struct{}{}
		for _, rule := range cr.Rules {
			if !containsString(rule.APIGroups, "rbac.authorization.k8s.io") || !containsString(rule.Resources, "rolebindings") {
				continue
			}
			for _, verb := range rule.Verbs {
				if _, ok := allowed[verb]; !ok {
					t.Fatalf("generated manager-role grants unexpected rolebinding verb %q in rule %#v", verb, rule)
				}
				granted[verb] = struct{}{}
			}
		}
		for verb := range required {
			if _, ok := granted[verb]; !ok {
				t.Fatalf("generated manager-role must grant rolebindings %s for cache-backed reconciliation, got verbs %v", verb, slices.Sorted(maps.Keys(granted)))
			}
		}
		break
	}
	if !found {
		t.Fatal("manager-role not found in generated RBAC manifest")
	}
}

//nolint:gocyclo // test readability benefits from a single linear flow
func TestGeneratedRBACManagerRoleAllowsCacheBackedCoreObjectReads(t *testing.T) {
	t.Parallel()

	data, err := os.ReadFile(filepath.Join("..", "..", "..", "config", "rbac", "role.yaml"))
	if err != nil {
		t.Fatalf("failed to read generated RBAC manifest: %v", err)
	}

	decoder := yaml.NewYAMLOrJSONDecoder(bytes.NewReader(data), 4096)
	found := false
	required := map[string][]string{
		"secrets":         {"get", "list", "watch", "create", "patch"},
		"serviceaccounts": {"get", "list", "watch", "create", "patch"},
	}
	actual := map[string]map[string]struct{}{}
	for {
		var cr rbacv1.ClusterRole
		if err := decoder.Decode(&cr); err != nil {
			if err == io.EOF {
				break
			}
			t.Fatalf("failed to decode generated RBAC manifest: %v", err)
		}
		if cr.Name != "manager-role" {
			continue
		}
		found = true
		for _, rule := range cr.Rules {
			if !containsString(rule.APIGroups, "") {
				continue
			}
			for resource := range required {
				if !containsString(rule.Resources, resource) {
					continue
				}
				if actual[resource] == nil {
					actual[resource] = map[string]struct{}{}
				}
				for _, verb := range rule.Verbs {
					actual[resource][verb] = struct{}{}
				}
			}
		}
		break
	}
	if !found {
		t.Fatal("manager-role not found in generated RBAC manifest")
	}
	for resource, verbs := range required {
		got := actual[resource]
		if got == nil {
			t.Fatalf("generated manager-role does not grant %s access", resource)
		}
		for _, verb := range verbs {
			if _, ok := got[verb]; !ok {
				t.Fatalf("generated manager-role must grant %s %s for cache-backed reconciliation, got verbs %v", resource, verb, slices.Sorted(maps.Keys(got)))
			}
		}
	}
}

func TestGeneratedRBACManagerRoleDoesNotUpdateManagedSecretsOrServiceAccounts(t *testing.T) {
	t.Parallel()

	data, err := os.ReadFile(filepath.Join("..", "..", "..", "config", "rbac", "role.yaml"))
	if err != nil {
		t.Fatalf("failed to read generated RBAC manifest: %v", err)
	}

	decoder := yaml.NewYAMLOrJSONDecoder(bytes.NewReader(data), 4096)
	found := false
	allowed := map[string]struct{}{
		"create": {},
		"get":    {},
		"list":   {},
		"patch":  {},
		"watch":  {},
	}
	for {
		var cr rbacv1.ClusterRole
		if err := decoder.Decode(&cr); err != nil {
			if err == io.EOF {
				break
			}
			t.Fatalf("failed to decode generated RBAC manifest: %v", err)
		}
		if cr.Name != "manager-role" {
			continue
		}
		found = true
		for _, rule := range cr.Rules {
			if !containsString(rule.APIGroups, "") {
				continue
			}
			if !containsString(rule.Resources, "secrets") && !containsString(rule.Resources, "serviceaccounts") {
				continue
			}
			for _, verb := range rule.Verbs {
				if _, ok := allowed[verb]; !ok {
					t.Fatalf("generated manager-role grants unexpected core secret/serviceaccount verb %q in rule %#v", verb, rule)
				}
			}
		}
		break
	}
	if !found {
		t.Fatal("manager-role not found in generated RBAC manifest")
	}
}

func TestCollectStoryRBACRulesMergesTemplateAndOverrides(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	if err := runsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add runs scheme: %v", err)
	}
	if err := bubuv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add core scheme: %v", err)
	}
	if err := catalogv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add catalog scheme: %v", err)
	}

	templateRule := rbacv1.PolicyRule{
		APIGroups:     []string{""},
		Resources:     []string{"configmaps"},
		Verbs:         []string{"get"},
		ResourceNames: []string{"shared-prompts"},
	}
	overrideRule := rbacv1.PolicyRule{
		APIGroups:     []string{""},
		Resources:     []string{"secrets"},
		Verbs:         []string{"get"},
		ResourceNames: []string{"runtime-creds"},
	}
	template := &catalogv1alpha1.EngramTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: "template-a"},
		Spec: catalogv1alpha1.EngramTemplateSpec{
			TemplateSpec: catalogv1alpha1.TemplateSpec{
				Execution: &catalogv1alpha1.TemplateExecutionPolicy{
					RBAC: &catalogv1alpha1.TemplateRBACPolicy{
						Rules: []rbacv1.PolicyRule{templateRule},
					},
				},
			},
		},
	}
	story := &bubuv1alpha1.Story{
		ObjectMeta: metav1.ObjectMeta{Name: "demo-story", Namespace: "default"},
		Spec: bubuv1alpha1.StorySpec{
			Steps: []bubuv1alpha1.Step{{Ref: &refs.EngramReference{ObjectReference: refs.ObjectReference{Name: "engram-a"}}}},
		},
	}
	storyRun := &runsv1alpha1.StoryRun{
		ObjectMeta: metav1.ObjectMeta{Name: "demo-storyrun", Namespace: "default"},
		Spec: runsv1alpha1.StoryRunSpec{
			StoryRef: refs.StoryReference{ObjectReference: refs.ObjectReference{Name: story.Name}},
		},
	}
	engram := &bubuv1alpha1.Engram{
		ObjectMeta: metav1.ObjectMeta{Name: "engram-a", Namespace: "default"},
		Spec: bubuv1alpha1.EngramSpec{
			TemplateRef: refs.EngramTemplateReference{Name: template.Name},
			Overrides: &bubuv1alpha1.ExecutionOverrides{
				RBAC: &bubuv1alpha1.RBACRuleOverrides{
					Rules: []rbacv1.PolicyRule{overrideRule},
				},
			},
		},
	}

	client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(template, story, storyRun, engram).Build()
	manager := &RBACManager{Client: client, Scheme: scheme}

	rules, err := manager.collectStoryRBACRules(context.Background(), storyRun, story, logging.NewReconcileLogger(context.Background(), "test"))
	if err != nil {
		t.Fatalf("collectStoryRBACRules failed: %v", err)
	}
	if len(rules) != 2 {
		t.Fatalf("expected template and override rules to merge, got %d (%v)", len(rules), rules)
	}
	if !containsPolicyRule(rules, templateRule) {
		t.Fatalf("expected merged rules to include template rule, got %v", rules)
	}
	if !containsPolicyRule(rules, overrideRule) {
		t.Fatalf("expected merged rules to include override rule, got %v", rules)
	}
}

func TestCollectStoryRBACRulesPreservesTrustedTemplatePodRules(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	if err := runsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add runs scheme: %v", err)
	}
	if err := bubuv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add core scheme: %v", err)
	}
	if err := catalogv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add catalog scheme: %v", err)
	}

	podsRule := rbacv1.PolicyRule{
		APIGroups: []string{""},
		Resources: []string{"pods"},
		Verbs:     []string{"create", "delete", "get", "list", "watch"},
	}
	attachRule := rbacv1.PolicyRule{
		APIGroups: []string{""},
		Resources: []string{"pods/attach"},
		Verbs:     []string{"create"},
	}
	template := &catalogv1alpha1.EngramTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: "template-a"},
		Spec: catalogv1alpha1.EngramTemplateSpec{
			TemplateSpec: catalogv1alpha1.TemplateSpec{
				Execution: &catalogv1alpha1.TemplateExecutionPolicy{
					RBAC: &catalogv1alpha1.TemplateRBACPolicy{
						Rules: []rbacv1.PolicyRule{podsRule, attachRule},
					},
				},
			},
		},
	}
	story := &bubuv1alpha1.Story{
		ObjectMeta: metav1.ObjectMeta{Name: "demo-story", Namespace: "default"},
		Spec: bubuv1alpha1.StorySpec{
			Steps: []bubuv1alpha1.Step{{Ref: &refs.EngramReference{ObjectReference: refs.ObjectReference{Name: "engram-a"}}}},
		},
	}
	storyRun := &runsv1alpha1.StoryRun{
		ObjectMeta: metav1.ObjectMeta{Name: "demo-storyrun", Namespace: "default"},
		Spec: runsv1alpha1.StoryRunSpec{
			StoryRef: refs.StoryReference{ObjectReference: refs.ObjectReference{Name: story.Name}},
		},
	}
	engram := &bubuv1alpha1.Engram{
		ObjectMeta: metav1.ObjectMeta{Name: "engram-a", Namespace: "default"},
		Spec: bubuv1alpha1.EngramSpec{
			TemplateRef: refs.EngramTemplateReference{Name: template.Name},
		},
	}

	client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(template, story, storyRun, engram).Build()
	manager := &RBACManager{Client: client, Scheme: scheme}

	rules, err := manager.collectStoryRBACRules(context.Background(), storyRun, story, logging.NewReconcileLogger(context.Background(), "test"))
	if err != nil {
		t.Fatalf("collectStoryRBACRules failed: %v", err)
	}
	if len(rules) != 2 {
		t.Fatalf("expected trusted template pod rules to survive, got %d (%v)", len(rules), rules)
	}
	if !containsPolicyRule(rules, podsRule) {
		t.Fatalf("expected merged rules to include pods rule, got %v", rules)
	}
	if !containsPolicyRule(rules, attachRule) {
		t.Fatalf("expected merged rules to include pods/attach rule, got %v", rules)
	}
}

func TestCollectStoryRBACRulesIncludesCompensationsAndFinally(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	if err := runsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add runs scheme: %v", err)
	}
	if err := bubuv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add core scheme: %v", err)
	}
	if err := catalogv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add catalog scheme: %v", err)
	}

	templateRule := rbacv1.PolicyRule{
		APIGroups: []string{"runs.bubustack.io"},
		Resources: []string{"storyruns"},
		Verbs:     []string{"create", "get", "list", "watch"},
	}
	overrideRule := rbacv1.PolicyRule{
		APIGroups:     []string{""},
		Resources:     []string{"secrets"},
		Verbs:         []string{"get"},
		ResourceNames: []string{"notify-creds"},
	}
	template := &catalogv1alpha1.EngramTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: "template-a"},
		Spec: catalogv1alpha1.EngramTemplateSpec{
			TemplateSpec: catalogv1alpha1.TemplateSpec{
				Execution: &catalogv1alpha1.TemplateExecutionPolicy{
					RBAC: &catalogv1alpha1.TemplateRBACPolicy{
						Rules: []rbacv1.PolicyRule{templateRule},
					},
				},
			},
		},
	}
	story := &bubuv1alpha1.Story{
		ObjectMeta: metav1.ObjectMeta{Name: "demo-story", Namespace: "default"},
		Spec: bubuv1alpha1.StorySpec{
			Compensations: []bubuv1alpha1.Step{{Ref: &refs.EngramReference{ObjectReference: refs.ObjectReference{Name: "engram-comp"}}}},
			Finally:       []bubuv1alpha1.Step{{Ref: &refs.EngramReference{ObjectReference: refs.ObjectReference{Name: "engram-final"}}}},
		},
	}
	storyRun := &runsv1alpha1.StoryRun{
		ObjectMeta: metav1.ObjectMeta{Name: "demo-storyrun", Namespace: "default"},
		Spec: runsv1alpha1.StoryRunSpec{
			StoryRef: refs.StoryReference{ObjectReference: refs.ObjectReference{Name: story.Name}},
		},
	}
	comp := &bubuv1alpha1.Engram{
		ObjectMeta: metav1.ObjectMeta{Name: "engram-comp", Namespace: "default"},
		Spec: bubuv1alpha1.EngramSpec{
			TemplateRef: refs.EngramTemplateReference{Name: template.Name},
		},
	}
	final := &bubuv1alpha1.Engram{
		ObjectMeta: metav1.ObjectMeta{Name: "engram-final", Namespace: "default"},
		Spec: bubuv1alpha1.EngramSpec{
			Overrides: &bubuv1alpha1.ExecutionOverrides{
				RBAC: &bubuv1alpha1.RBACRuleOverrides{
					Rules: []rbacv1.PolicyRule{overrideRule},
				},
			},
		},
	}

	client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(template, story, storyRun, comp, final).Build()
	manager := &RBACManager{Client: client, Scheme: scheme}

	rules, err := manager.collectStoryRBACRules(context.Background(), storyRun, story, logging.NewReconcileLogger(context.Background(), "test"))
	if err != nil {
		t.Fatalf("collectStoryRBACRules failed: %v", err)
	}
	if len(rules) != 2 {
		t.Fatalf("expected compensation and finally rules to be collected, got %d (%v)", len(rules), rules)
	}
	if !containsPolicyRule(rules, templateRule) {
		t.Fatalf("expected collected rules to include compensation template rule, got %v", rules)
	}
	if !containsPolicyRule(rules, overrideRule) {
		t.Fatalf("expected collected rules to include finally override rule, got %v", rules)
	}
}

func TestCollectStoryRBACRulesDropsWildcardTemplateRules(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	if err := runsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add runs scheme: %v", err)
	}
	if err := bubuv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add core scheme: %v", err)
	}
	if err := catalogv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add catalog scheme: %v", err)
	}

	template := &catalogv1alpha1.EngramTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: "template-a"},
		Spec: catalogv1alpha1.EngramTemplateSpec{
			TemplateSpec: catalogv1alpha1.TemplateSpec{
				Execution: &catalogv1alpha1.TemplateExecutionPolicy{
					RBAC: &catalogv1alpha1.TemplateRBACPolicy{
						Rules: []rbacv1.PolicyRule{{
							APIGroups: []string{""},
							Resources: []string{"*"},
							Verbs:     []string{"get"},
						}},
					},
				},
			},
		},
	}
	story := &bubuv1alpha1.Story{
		ObjectMeta: metav1.ObjectMeta{Name: "demo-story", Namespace: "default"},
		Spec: bubuv1alpha1.StorySpec{
			Steps: []bubuv1alpha1.Step{{Ref: &refs.EngramReference{ObjectReference: refs.ObjectReference{Name: "engram-a"}}}},
		},
	}
	storyRun := &runsv1alpha1.StoryRun{
		ObjectMeta: metav1.ObjectMeta{Name: "demo-storyrun", Namespace: "default"},
		Spec: runsv1alpha1.StoryRunSpec{
			StoryRef: refs.StoryReference{ObjectReference: refs.ObjectReference{Name: story.Name}},
		},
	}
	engram := &bubuv1alpha1.Engram{
		ObjectMeta: metav1.ObjectMeta{Name: "engram-a", Namespace: "default"},
		Spec: bubuv1alpha1.EngramSpec{
			TemplateRef: refs.EngramTemplateReference{Name: template.Name},
		},
	}

	client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(template, story, storyRun, engram).Build()
	manager := &RBACManager{Client: client, Scheme: scheme}

	rules, err := manager.collectStoryRBACRules(context.Background(), storyRun, story, logging.NewReconcileLogger(context.Background(), "test"))
	if err != nil {
		t.Fatalf("collectStoryRBACRules failed: %v", err)
	}
	if len(rules) != 0 {
		t.Fatalf("expected wildcard template rules to be dropped, got %v", rules)
	}
}

func TestReconcileRoleDoesNotGrantBlanketSecretOrConfigMapRead(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	if err := runsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add runs scheme: %v", err)
	}
	if err := bubuv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add core scheme: %v", err)
	}
	if err := rbacv1.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add rbac scheme: %v", err)
	}

	storyRun := &runsv1alpha1.StoryRun{
		ObjectMeta: metav1.ObjectMeta{Name: "demo-storyrun", Namespace: "default"},
	}
	client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(storyRun).Build()
	manager := &RBACManager{Client: client, Scheme: scheme}

	if err := manager.reconcileRole(context.Background(), storyRun, "demo-sa", nil, logging.NewReconcileLogger(context.Background(), "test")); err != nil {
		t.Fatalf("reconcileRole failed: %v", err)
	}

	role := &rbacv1.Role{}
	if err := client.Get(context.Background(), types.NamespacedName{Name: "demo-sa", Namespace: "default"}, role); err != nil {
		t.Fatalf("failed to fetch reconciled role: %v", err)
	}

	for _, rule := range role.Rules {
		if !ruleContainsResource(rule, "secrets") && !ruleContainsResource(rule, "configmaps") {
			continue
		}
		t.Fatalf("unexpected default secret/configmap permission in role: %#v", rule)
	}
}

func TestReconcileRoleGrantsEffectClaimAccess(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	if err := runsv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add runs scheme: %v", err)
	}
	if err := bubuv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add core scheme: %v", err)
	}
	if err := rbacv1.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add rbac scheme: %v", err)
	}

	storyRun := &runsv1alpha1.StoryRun{
		ObjectMeta: metav1.ObjectMeta{Name: "demo-storyrun", Namespace: "default"},
	}
	client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(storyRun).Build()
	manager := &RBACManager{Client: client, Scheme: scheme}

	if err := manager.reconcileRole(context.Background(), storyRun, "demo-sa", nil, logging.NewReconcileLogger(context.Background(), "test")); err != nil {
		t.Fatalf("reconcileRole failed: %v", err)
	}

	role := &rbacv1.Role{}
	if err := client.Get(context.Background(), types.NamespacedName{Name: "demo-sa", Namespace: "default"}, role); err != nil {
		t.Fatalf("failed to fetch reconciled role: %v", err)
	}

	var claimRule *rbacv1.PolicyRule
	for i := range role.Rules {
		rule := &role.Rules[i]
		if containsString(rule.APIGroups, "runs.bubustack.io") && ruleContainsResource(*rule, "effectclaims") {
			claimRule = rule
			break
		}
	}
	if claimRule == nil {
		t.Fatal("expected runner role to grant access to runs.bubustack.io effectclaims")
	}

	expectedVerbs := map[string]struct{}{
		"get":    {},
		"create": {},
		"update": {},
	}
	if got := ruleSliceToSet(claimRule.Verbs); len(got) != len(expectedVerbs) {
		t.Fatalf("unexpected effectclaim verbs: %#v", claimRule.Verbs)
	}
	for verb := range expectedVerbs {
		if _, ok := ruleSliceToSet(claimRule.Verbs)[verb]; !ok {
			t.Fatalf("expected effectclaim verb %q in %#v", verb, claimRule.Verbs)
		}
	}
}

func ruleContainsResource(rule rbacv1.PolicyRule, resource string) bool {
	return slices.Contains(rule.Resources, resource)
}

func ruleSliceToSet(values []string) map[string]struct{} {
	set := make(map[string]struct{}, len(values))
	for _, value := range values {
		set[value] = struct{}{}
	}
	return set
}

func containsString(slice []string, value string) bool {
	return slices.Contains(slice, value)
}

func containsPolicyRule(rules []rbacv1.PolicyRule, expected rbacv1.PolicyRule) bool {
	expectedGroups := ruleSliceToSet(expected.APIGroups)
	expectedResources := ruleSliceToSet(expected.Resources)
	expectedVerbs := ruleSliceToSet(expected.Verbs)
	expectedNames := ruleSliceToSet(expected.ResourceNames)

	for _, rule := range rules {
		if !maps.Equal(ruleSliceToSet(rule.APIGroups), expectedGroups) {
			continue
		}
		if !maps.Equal(ruleSliceToSet(rule.Resources), expectedResources) {
			continue
		}
		if !maps.Equal(ruleSliceToSet(rule.Verbs), expectedVerbs) {
			continue
		}
		if !maps.Equal(ruleSliceToSet(rule.ResourceNames), expectedNames) {
			continue
		}
		return true
	}
	return false
}
