package refs

import (
	"context"
	"fmt"
	"strings"

	policyv1alpha1 "github.com/bubustack/bobrapet/api/policy/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// GrantCheck describes a cross-namespace reference request.
type GrantCheck struct {
	FromGroup     string
	FromKind      string
	FromNamespace string

	ToGroup     string
	ToKind      string
	ToNamespace string
	ToName      string
}

// ReferenceGranted returns true when a ReferenceGrant in the target namespace allows
// the specified cross-namespace reference.
func ReferenceGranted(ctx context.Context, reader client.Reader, check GrantCheck) (bool, error) {
	if reader == nil {
		return false, fmt.Errorf("reference grant check requires a reader")
	}
	if strings.TrimSpace(check.ToNamespace) == "" {
		return false, fmt.Errorf("reference grant check requires a target namespace")
	}

	var grants policyv1alpha1.ReferenceGrantList
	if err := reader.List(ctx, &grants, client.InNamespace(check.ToNamespace)); err != nil {
		return false, err
	}
	for i := range grants.Items {
		if grantAllows(&grants.Items[i], check) {
			return true, nil
		}
	}
	return false, nil
}

func grantAllows(grant *policyv1alpha1.ReferenceGrant, check GrantCheck) bool {
	if grant == nil || grant.Spec.From == nil || grant.Spec.To == nil {
		return false
	}
	if strings.TrimSpace(grant.Namespace) == "" || grant.Namespace != check.ToNamespace {
		return false
	}

	fromGroup := strings.TrimSpace(check.FromGroup)
	fromKind := strings.TrimSpace(check.FromKind)
	fromNamespace := strings.TrimSpace(check.FromNamespace)
	toGroup := strings.TrimSpace(check.ToGroup)
	toKind := strings.TrimSpace(check.ToKind)
	toName := strings.TrimSpace(check.ToName)

	fromAllowed := false
	for _, entry := range grant.Spec.From {
		if strings.TrimSpace(entry.Group) == fromGroup &&
			strings.TrimSpace(entry.Kind) == fromKind &&
			strings.TrimSpace(entry.Namespace) == fromNamespace {
			fromAllowed = true
			break
		}
	}
	if !fromAllowed {
		return false
	}

	for _, entry := range grant.Spec.To {
		if strings.TrimSpace(entry.Group) != toGroup || strings.TrimSpace(entry.Kind) != toKind {
			continue
		}
		if entry.Name != nil && strings.TrimSpace(*entry.Name) != "" && strings.TrimSpace(*entry.Name) != toName {
			continue
		}
		return true
	}
	return false
}
