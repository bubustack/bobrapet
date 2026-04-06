package identity

import (
	"maps"

	rbacv1 "k8s.io/api/rbac/v1"

	coreidentity "github.com/bubustack/core/runtime/identity"
)

// EngramRunner captures the shared metadata for the `<storyRun>-engram-runner` contract.
type EngramRunner struct {
	storyRun string
}

// NewEngramRunner constructs a new EngramRunner helper.
func NewEngramRunner(storyRun string) EngramRunner {
	return EngramRunner{storyRun: storyRun}
}

// StoryRun returns the owning StoryRun name.
func (e EngramRunner) StoryRun() string {
	return e.storyRun
}

// ServiceAccountName exposes the canonical `<storyRun>-engram-runner` ServiceAccount.
func (e EngramRunner) ServiceAccountName() string {
	return ServiceAccountName(e.storyRun)
}

// SelectorLabels returns a copy of the deterministic selector labels controllers share.
func (e EngramRunner) SelectorLabels() map[string]string {
	return SelectorLabels(e.storyRun)
}

// Subject builds the RBAC Subject pointing at the runner ServiceAccount.
func (e EngramRunner) Subject(namespace string) rbacv1.Subject {
	return ServiceAccountSubject(e.ServiceAccountName(), namespace)
}

// ServiceAccountName exposes the canonical `<storyRun>-engram-runner` name.
func ServiceAccountName(storyRun string) string {
	return coreidentity.StoryRunEngramRunnerServiceAccount(storyRun)
}

// SelectorLabels returns a copy of the deterministic StoryRun selector labels.
func SelectorLabels(storyRun string) map[string]string {
	return cloneStringMap(coreidentity.StoryRunSelectorLabels(storyRun))
}

// ServiceAccountSubject returns an RBAC subject for the ServiceAccount in namespace.
func ServiceAccountSubject(name, namespace string) rbacv1.Subject {
	return rbacv1.Subject{
		Kind:      rbacv1.ServiceAccountKind,
		Name:      name,
		Namespace: namespace,
	}
}

func cloneStringMap(src map[string]string) map[string]string {
	if len(src) == 0 {
		return map[string]string{}
	}
	out := make(map[string]string, len(src))
	maps.Copy(out, src)
	return out
}
