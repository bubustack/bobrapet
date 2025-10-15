package runs

import (
	"context"
	"fmt"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	bubuv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/pkg/logging"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

// RBACManager handles the reconciliation of RBAC resources for a StoryRun.
type RBACManager struct {
	client.Client
	Scheme *runtime.Scheme
}

// NewRBACManager creates a new RBACManager.
func NewRBACManager(k8sClient client.Client, scheme *runtime.Scheme) *RBACManager {
	return &RBACManager{Client: k8sClient, Scheme: scheme}
}

// Reconcile ensures the necessary ServiceAccount, Role, and RoleBinding exist for the StoryRun.
func (r *RBACManager) Reconcile(ctx context.Context, storyRun *runsv1alpha1.StoryRun) error {
	log := logging.NewReconcileLogger(ctx, "storyrun-rbac")

	story, err := r.getStoryForRun(ctx, storyRun)
	if err != nil {
		// If the story isn't found, we can't determine the storage policy, but we can still proceed
		// with the basic RBAC setup. The error will be handled in the main reconcile loop.
		log.Error(err, "Could not get parent story for RBAC setup, proceeding without storage policy")
	}

	saName := fmt.Sprintf("%s-engram-runner", storyRun.Name)
	sa := &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:      saName,
			Namespace: storyRun.Namespace,
		},
	}

	_, err = controllerutil.CreateOrUpdate(ctx, r.Client, sa, func() error {
		if sa.Annotations == nil {
			sa.Annotations = make(map[string]string)
		}

		if story != nil && story.Spec.Policy != nil && story.Spec.Policy.Storage != nil && story.Spec.Policy.Storage.S3 != nil &&
			story.Spec.Policy.Storage.S3.Authentication.ServiceAccountAnnotations != nil {
			log.Info("Applying S3 ServiceAccount annotations from StoragePolicy")
			for k, v := range story.Spec.Policy.Storage.S3.Authentication.ServiceAccountAnnotations {
				sa.Annotations[k] = v
			}
		}

		return controllerutil.SetOwnerReference(storyRun, sa, r.Scheme)
	})

	if err != nil {
		return fmt.Errorf("failed to create or update ServiceAccount: %w", err)
	}

	role := &rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{
			Name:      saName,
			Namespace: storyRun.Namespace,
		},
		Rules: []rbacv1.PolicyRule{
			{
				APIGroups: []string{"runs.bubustack.io"},
				Resources: []string{"stepruns"},
				Verbs:     []string{"get", "watch"},
			},
			{
				APIGroups: []string{"runs.bubustack.io"},
				Resources: []string{"stepruns/status"},
				Verbs:     []string{"patch", "update"},
			},
		},
	}
	if err := controllerutil.SetOwnerReference(storyRun, role, r.Scheme); err != nil {
		return fmt.Errorf("failed to set owner reference on Role: %w", err)
	}
	if err := r.Create(ctx, role); err != nil && !errors.IsAlreadyExists(err) {
		return fmt.Errorf("failed to create Role: %w", err)
	} else if err == nil {
		log.Info("Created Role for Engram runner", "role", role.Name)
	}

	rb := &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:      saName,
			Namespace: storyRun.Namespace,
		},
		Subjects: []rbacv1.Subject{
			{
				Kind:      "ServiceAccount",
				Name:      saName,
				Namespace: storyRun.Namespace,
			},
		},
		RoleRef: rbacv1.RoleRef{
			APIGroup: "rbac.authorization.k8s.io",
			Kind:     "Role",
			Name:     saName,
		},
	}
	if err := controllerutil.SetOwnerReference(storyRun, rb, r.Scheme); err != nil {
		return fmt.Errorf("failed to set owner reference on RoleBinding: %w", err)
	}
	if err := r.Create(ctx, rb); err != nil && !errors.IsAlreadyExists(err) {
		return fmt.Errorf("failed to create RoleBinding: %w", err)
	} else if err == nil {
		log.Info("Created RoleBinding for Engram runner", "roleBinding", rb.Name)
	}

	return nil
}

// getStoryForRun is a helper to fetch the parent Story. It's duplicated here
// to keep the RBACManager self-contained.
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
