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

package kubeutil

import (
	"context"
	"fmt"

	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	runsidentity "github.com/bubustack/bobrapet/pkg/runs/identity"
)

// ReferenceStyle selects how RBAC resources should attach owner references.
type ReferenceStyle int

const (
	// ControllerReference sets a controller reference via controllerutil.SetControllerReference.
	ControllerReference ReferenceStyle = iota
	// OwnerReference sets a non-controller owner reference via controllerutil.SetOwnerReference.
	OwnerReference
)

func setOwnerReference(owner client.Object, obj metav1.Object, scheme *runtime.Scheme, style ReferenceStyle) error {
	if owner == nil {
		return fmt.Errorf("owner is nil")
	}
	if obj == nil {
		return fmt.Errorf("object is nil")
	}
	if scheme == nil {
		return fmt.Errorf("scheme is nil")
	}
	switch style {
	case ControllerReference:
		return controllerutil.SetControllerReference(owner, obj, scheme)
	case OwnerReference:
		return controllerutil.SetOwnerReference(owner, obj, scheme)
	default:
		return fmt.Errorf("unknown reference style %d", style)
	}
}

// EnsureRole CreateOrUpdates a namespaced Role with the supplied rules and owner reference.
//
// Arguments:
//   - ctx context.Context: context for the operation.
//   - c client.Client: Kubernetes client.
//   - scheme *runtime.Scheme: scheme for setting owner references.
//   - owner client.Object: the owner object for the Role.
//   - name string: Role name.
//   - namespace string: Role namespace.
//   - rules []rbacv1.PolicyRule: Role rules.
//   - style ReferenceStyle: how to set the owner reference.
//
// Returns:
//   - controllerutil.OperationResult: result of the operation.
//   - error: any error encountered.
func EnsureRole(
	ctx context.Context,
	c client.Client,
	scheme *runtime.Scheme,
	owner client.Object,
	name string,
	namespace string,
	rules []rbacv1.PolicyRule,
	style ReferenceStyle,
) (controllerutil.OperationResult, error) {
	role := &rbacv1.Role{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace}}
	return controllerutil.CreateOrPatch(ctx, c, role, func() error {
		role.Rules = rules
		return setOwnerReference(owner, role, scheme, style)
	})
}

// EnsureRoleBinding CreateOrUpdates a namespaced RoleBinding with the supplied RoleRef,
// subjects, and owner reference.
//
// Arguments:
//   - ctx context.Context: context for the operation.
//   - c client.Client: Kubernetes client.
//   - scheme *runtime.Scheme: scheme for setting owner references.
//   - owner client.Object: the owner object for the RoleBinding.
//   - name string: RoleBinding name.
//   - namespace string: RoleBinding namespace.
//   - roleRef rbacv1.RoleRef: the role reference.
//   - subjects []rbacv1.Subject: the subjects.
//   - style ReferenceStyle: how to set the owner reference.
//
// Returns:
//   - controllerutil.OperationResult: result of the operation.
//   - error: any error encountered.
func EnsureRoleBinding(
	ctx context.Context,
	c client.Client,
	scheme *runtime.Scheme,
	owner client.Object,
	name string,
	namespace string,
	roleRef rbacv1.RoleRef,
	subjects []rbacv1.Subject,
	style ReferenceStyle,
) (controllerutil.OperationResult, error) {
	rb := &rbacv1.RoleBinding{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace}}
	return controllerutil.CreateOrPatch(ctx, c, rb, func() error {
		rb.RoleRef = roleRef
		rb.Subjects = subjects
		return setOwnerReference(owner, rb, scheme, style)
	})
}

// StoryRunEngramRunnerSubject returns the canonical ServiceAccount subject for a StoryRun.
//
// Arguments:
//   - storyRun string: the StoryRun name.
//   - namespace string: the namespace.
//
// Returns:
//   - rbacv1.Subject: the ServiceAccount subject.
func StoryRunEngramRunnerSubject(storyRun, namespace string) rbacv1.Subject {
	return runsidentity.NewEngramRunner(storyRun).Subject(namespace)
}
