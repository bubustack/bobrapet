package setup

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	catalogv1alpha1 "github.com/bubustack/bobrapet/api/catalog/v1alpha1"
	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	bubushv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/pkg/enums"
	"github.com/bubustack/bobrapet/pkg/refs"
)

var setupLog = log.Log.WithName("setup")

// SetupIndexers sets up the field indexers for all the controllers.
// The provided context must be tied to the manager lifecycle to ensure
// proper cancellation on shutdown.
func SetupIndexers(ctx context.Context, mgr manager.Manager) {
	setupLog.Info("setting up field indexes")

	mustIndexField(ctx, mgr, &bubushv1alpha1.Engram{}, "spec.templateRef.name", extractEngramTemplateName, "failed to index Engram spec.templateRef.name")

	mustIndexField(ctx, mgr, &runsv1alpha1.StepRun{}, "spec.engramRef.key", extractStepRunEngramRef, "failed to index StepRun spec.engramRef.key")

	mustIndexField(ctx, mgr, &runsv1alpha1.StepRun{}, "spec.storyRunRef.name", extractStepRunStoryRunRef, "failed to index StepRun spec.storyRunRef.name")

	mustIndexField(ctx, mgr, &runsv1alpha1.StoryRun{}, "spec.impulseRef.name", extractStoryRunImpulseRef, "failed to index StoryRun spec.impulseRef.name")

	mustIndexField(ctx, mgr, &bubushv1alpha1.Impulse{}, "spec.templateRef.name", extractImpulseTemplateName, "failed to index Impulse spec.templateRef.name")

	mustIndexField(ctx, mgr, &runsv1alpha1.StoryRun{}, "spec.storyRef.name", extractStoryRunStoryRefName, "failed to index StoryRun spec.storyRef.name")

	mustIndexField(ctx, mgr, &bubushv1alpha1.Impulse{}, "spec.storyRef.name", extractImpulseStoryRefName, "failed to index Impulse spec.storyRef.name")

	mustIndexField(ctx, mgr, &bubushv1alpha1.Story{}, "spec.steps.ref.key", extractStoryStepEngramRefs, "failed to index Story spec.steps.ref.key")
	mustIndexField(ctx, mgr, &bubushv1alpha1.Story{}, "spec.steps.storyRef.key", extractStoryExecuteStoryRefs, "failed to index Story spec.steps.storyRef.key")

	mustIndexField(ctx, mgr, &catalogv1alpha1.EngramTemplate{}, "spec.description", extractEngramTemplateDescription, "failed to index EngramTemplate spec.description")

	mustIndexField(ctx, mgr, &catalogv1alpha1.EngramTemplate{}, "spec.version", extractEngramTemplateVersion, "failed to index EngramTemplate spec.version")
}

// mustIndexField registers an index and terminates the process if registration fails.
func mustIndexField(
	ctx context.Context,
	mgr manager.Manager,
	obj client.Object,
	field string,
	extractor func(client.Object) []string,
	errMsg string,
) {
	if err := mgr.GetFieldIndexer().IndexField(ctx, obj, field, extractor); err != nil {
		setupLog.Error(err, errMsg)
		os.Exit(1)
	}
}

func extractEngramTemplateName(rawObj client.Object) []string {
	engram := rawObj.(*bubushv1alpha1.Engram)
	if engram.Spec.TemplateRef.Name == "" {
		return nil
	}
	return []string{engram.Spec.TemplateRef.Name}
}

func extractStepRunEngramRef(rawObj client.Object) []string {
	stepRun := rawObj.(*runsv1alpha1.StepRun)
	if stepRun.Spec.EngramRef == nil || stepRun.Spec.EngramRef.Name == "" {
		return nil
	}
	namespace := refs.ResolveNamespace(stepRun, &stepRun.Spec.EngramRef.ObjectReference)
	return []string{namespacedKey(namespace, stepRun.Spec.EngramRef.Name)}
}

func extractStepRunStoryRunRef(rawObj client.Object) []string {
	stepRun := rawObj.(*runsv1alpha1.StepRun)
	if stepRun.Spec.StoryRunRef.Name == "" {
		return nil
	}
	return []string{stepRun.Spec.StoryRunRef.Name}
}

func extractStoryRunImpulseRef(rawObj client.Object) []string {
	storyRun := rawObj.(*runsv1alpha1.StoryRun)
	if storyRun.Spec.ImpulseRef == nil || storyRun.Spec.ImpulseRef.Name == "" {
		return nil
	}
	return []string{storyRun.Spec.ImpulseRef.Name}
}

func extractImpulseTemplateName(rawObj client.Object) []string {
	impulse := rawObj.(*bubushv1alpha1.Impulse)
	if impulse.Spec.TemplateRef.Name == "" {
		return nil
	}
	return []string{impulse.Spec.TemplateRef.Name}
}

func extractStoryRunStoryRefName(rawObj client.Object) []string {
	storyRun := rawObj.(*runsv1alpha1.StoryRun)
	if storyRun.Spec.StoryRef.Name == "" {
		return nil
	}
	return []string{storyRun.Spec.StoryRef.Name}
}

func extractImpulseStoryRefName(rawObj client.Object) []string {
	impulse := rawObj.(*bubushv1alpha1.Impulse)
	if impulse.Spec.StoryRef.Name == "" {
		return nil
	}
	return []string{impulse.Spec.StoryRef.Name}
}

func extractStoryStepEngramRefs(rawObj client.Object) []string {
	story := rawObj.(*bubushv1alpha1.Story)
	if len(story.Spec.Steps) == 0 {
		return nil
	}
	nameSet := make(map[string]struct{})
	for i := range story.Spec.Steps {
		step := &story.Spec.Steps[i]
		if step.Ref != nil && step.Ref.Name != "" {
			ns := refs.ResolveNamespace(story, &step.Ref.ObjectReference)
			nameSet[namespacedKey(ns, step.Ref.Name)] = struct{}{}
		}
	}
	if len(nameSet) == 0 {
		return nil
	}
	out := make([]string, 0, len(nameSet))
	for n := range nameSet {
		out = append(out, n)
	}
	return out
}

func extractStoryExecuteStoryRefs(rawObj client.Object) []string {
	story := rawObj.(*bubushv1alpha1.Story)
	if len(story.Spec.Steps) == 0 {
		return nil
	}
	nameSet := make(map[string]struct{})
	for i := range story.Spec.Steps {
		step := &story.Spec.Steps[i]
		if step.Type != enums.StepTypeExecuteStory || step.With == nil || len(step.With.Raw) == 0 {
			continue
		}
		var withBlock struct {
			StoryRef struct {
				Name      string `json:"name"`
				Namespace string `json:"namespace,omitempty"`
			} `json:"storyRef"`
		}
		if err := json.Unmarshal(step.With.Raw, &withBlock); err != nil {
			continue
		}
		if withBlock.StoryRef.Name == "" {
			continue
		}
		targetNamespace := story.Namespace
		if withBlock.StoryRef.Namespace != "" {
			targetNamespace = withBlock.StoryRef.Namespace
		}
		nameSet[namespacedKey(targetNamespace, withBlock.StoryRef.Name)] = struct{}{}
	}
	if len(nameSet) == 0 {
		return nil
	}
	out := make([]string, 0, len(nameSet))
	for n := range nameSet {
		out = append(out, n)
	}
	return out
}

func extractEngramTemplateDescription(obj client.Object) []string {
	template := obj.(*catalogv1alpha1.EngramTemplate)
	if template.Spec.Description != "" {
		return []string{template.Spec.Description}
	}
	return []string{"no-description"}
}

func extractEngramTemplateVersion(obj client.Object) []string {
	return []string{obj.(*catalogv1alpha1.EngramTemplate).Spec.Version}
}

func namespacedKey(namespace, name string) string {
	return fmt.Sprintf("%s/%s", namespace, name)
}
