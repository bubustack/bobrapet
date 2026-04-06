/*
Copyright 2026 BubuStack.

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
	"context"
	"slices"
	"testing"

	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	catalogv1alpha1 "github.com/bubustack/bobrapet/api/catalog/v1alpha1"
	"github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/internal/config"
)

func TestResolveImpulseRBACRulesMergesTemplateAndInstanceRules(t *testing.T) {
	t.Parallel()

	templateRule := rbacv1.PolicyRule{
		APIGroups:     []string{""},
		Resources:     []string{"configmaps"},
		Verbs:         []string{"get"},
		ResourceNames: []string{"impulse-template-config"},
	}
	instanceRule := rbacv1.PolicyRule{
		APIGroups:     []string{""},
		Resources:     []string{"secrets"},
		Verbs:         []string{"get"},
		ResourceNames: []string{"storage-creds"},
	}
	template := &catalogv1alpha1.ImpulseTemplate{
		Spec: catalogv1alpha1.ImpulseTemplateSpec{
			TemplateSpec: catalogv1alpha1.TemplateSpec{
				Execution: &catalogv1alpha1.TemplateExecutionPolicy{
					RBAC: &catalogv1alpha1.TemplateRBACPolicy{
						Rules: []rbacv1.PolicyRule{templateRule},
					},
				},
			},
		},
	}
	impulse := &v1alpha1.Impulse{
		Spec: v1alpha1.ImpulseSpec{
			Execution: &v1alpha1.ExecutionOverrides{
				RBAC: &v1alpha1.RBACRuleOverrides{
					Rules: []rbacv1.PolicyRule{instanceRule},
				},
			},
		},
	}

	got := resolveImpulseRBACRules(template, impulse)
	if len(got) != 2 {
		t.Fatalf("expected template and instance rules to merge, got %d: %#v", len(got), got)
	}
	if !roleContainsPolicyRule(got, templateRule) {
		t.Fatalf("expected merged rules to include template rule, got %#v", got)
	}
	if !roleContainsPolicyRule(got, instanceRule) {
		t.Fatalf("expected merged rules to include instance rule, got %#v", got)
	}
}

func TestResolveImpulseRBACRulesUsesTemplateFallback(t *testing.T) {
	t.Parallel()

	safe := rbacv1.PolicyRule{
		APIGroups:     []string{""},
		Resources:     []string{"configmaps"},
		Verbs:         []string{"get"},
		ResourceNames: []string{"impulse-template-config"},
	}
	template := &catalogv1alpha1.ImpulseTemplate{
		Spec: catalogv1alpha1.ImpulseTemplateSpec{
			TemplateSpec: catalogv1alpha1.TemplateSpec{
				Execution: &catalogv1alpha1.TemplateExecutionPolicy{
					RBAC: &catalogv1alpha1.TemplateRBACPolicy{
						Rules: []rbacv1.PolicyRule{
							safe,
							{
								APIGroups: []string{""},
								Resources: []string{"secrets"},
								Verbs:     []string{"list"},
							},
						},
					},
				},
			},
		},
	}

	got := resolveImpulseRBACRules(template, nil)
	if len(got) != 2 {
		t.Fatalf("expected template rules when no instance override exists, got %d: %#v", len(got), got)
	}
	if !roleContainsPolicyRule(got, safe) {
		t.Fatalf("expected template fallback to preserve declared rule, got %#v", got)
	}
	if !roleContainsResource(got, "secrets") {
		t.Fatalf("expected template fallback to keep all declared rules, got %#v", got)
	}
}

func TestEnsureImpulseRBACDefaultRoleMatchesTriggerBaseline(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add impulse scheme: %v", err)
	}
	if err := rbacv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add rbac scheme: %v", err)
	}

	impulse := &v1alpha1.Impulse{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "impulse",
			Namespace: "default",
		},
	}

	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(impulse).Build()
	r := &ImpulseReconciler{
		ControllerDependencies: config.ControllerDependencies{
			Client: cl,
			Scheme: scheme,
		},
	}

	if err := r.ensureImpulseRBAC(context.Background(), impulse, "impulse-runner", nil); err != nil {
		t.Fatalf("ensureImpulseRBAC failed: %v", err)
	}

	var role rbacv1.Role
	key := types.NamespacedName{Name: "impulse-runner", Namespace: "default"}
	if err := cl.Get(context.Background(), key, &role); err != nil {
		t.Fatalf("get role: %v", err)
	}

	assertRoleRuleVerbs(t, role.Rules, "storytriggers", "create", "get")
	assertRoleRuleVerbs(t, role.Rules, "storyruns", "get")
	assertRoleRuleVerbs(t, role.Rules, "storyruns/status", "patch")
	assertRoleRuleVerbs(t, role.Rules, "impulses", "get")
	assertRoleRuleVerbs(t, role.Rules, "impulses/status", "patch")

	if storyRunRule := findRoleRule(role.Rules, "storyruns"); storyRunRule == nil {
		t.Fatalf("default impulse role is missing storyruns readback access: %#v", role.Rules)
	} else if len(storyRunRule.Verbs) != 1 || !slices.Contains(storyRunRule.Verbs, "get") {
		t.Fatalf("default impulse storyruns access must stay minimal, got %#v", storyRunRule)
	}
	if storyTriggerRule := findRoleRule(role.Rules, "storytriggers"); storyTriggerRule == nil {
		t.Fatalf("default impulse role is missing storytrigger submission access: %#v", role.Rules)
	} else if slices.Contains(storyTriggerRule.Verbs, "list") || slices.Contains(storyTriggerRule.Verbs, "watch") {
		t.Fatalf("default impulse storytrigger access should not include broad list/watch verbs: %#v", storyTriggerRule)
	}
}

func TestEnsureImpulseRBACPreservesAdditiveRules(t *testing.T) {
	t.Parallel()

	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add impulse scheme: %v", err)
	}
	if err := rbacv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add rbac scheme: %v", err)
	}

	impulse := &v1alpha1.Impulse{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "impulse",
			Namespace: "default",
		},
	}

	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(impulse).Build()
	r := &ImpulseReconciler{
		ControllerDependencies: config.ControllerDependencies{
			Client: cl,
			Scheme: scheme,
		},
	}

	extraRule := rbacv1.PolicyRule{
		APIGroups:     []string{""},
		Resources:     []string{"configmaps"},
		Verbs:         []string{"get"},
		ResourceNames: []string{"impulse-config"},
	}
	extra := []rbacv1.PolicyRule{extraRule}
	if err := r.ensureImpulseRBAC(context.Background(), impulse, "impulse-runner", extra); err != nil {
		t.Fatalf("ensureImpulseRBAC failed: %v", err)
	}

	var role rbacv1.Role
	key := types.NamespacedName{Name: "impulse-runner", Namespace: "default"}
	if err := cl.Get(context.Background(), key, &role); err != nil {
		t.Fatalf("get role: %v", err)
	}

	if !roleContainsPolicyRule(role.Rules, extraRule) {
		t.Fatalf("expected declared additive rule to be preserved, got %#v", role.Rules)
	}
}

func roleContainsResource(rules []rbacv1.PolicyRule, resource string) bool {
	for _, rule := range rules {
		if slices.Contains(rule.Resources, resource) {
			return true
		}
	}
	return false
}

func roleContainsPolicyRule(rules []rbacv1.PolicyRule, expected rbacv1.PolicyRule) bool {
	expectedKey := policyRuleKey(expected)
	for _, rule := range rules {
		if policyRuleKey(rule) == expectedKey {
			return true
		}
	}
	return false
}

func findRoleRule(rules []rbacv1.PolicyRule, resource string) *rbacv1.PolicyRule {
	for i := range rules {
		if slices.Contains(rules[i].Resources, resource) {
			return &rules[i]
		}
	}
	return nil
}

func assertRoleRuleVerbs(t *testing.T, rules []rbacv1.PolicyRule, resource string, verbs ...string) {
	t.Helper()

	rule := findRoleRule(rules, resource)
	if rule == nil {
		t.Fatalf("expected role rule for resource %q, got %#v", resource, rules)
	}
	if len(rule.Verbs) != len(verbs) {
		t.Fatalf("unexpected verbs for %s: got %v want %v", resource, rule.Verbs, verbs)
	}
	for _, verb := range verbs {
		if !slices.Contains(rule.Verbs, verb) {
			t.Fatalf("expected verb %q in %s rule, got %v", verb, resource, rule.Verbs)
		}
	}
}
