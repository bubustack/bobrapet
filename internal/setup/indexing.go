package setup

import (
	"context"
	"os"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	catalogv1alpha1 "github.com/bubustack/bobrapet/api/catalog/v1alpha1"
	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	bubushv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
)

var setupLog = log.Log.WithName("setup")

// SetupIndexers sets up the field indexers for all the controllers.
// The provided context must be tied to the manager lifecycle to ensure
// proper cancellation on shutdown.
func SetupIndexers(ctx context.Context, mgr manager.Manager) {
	setupLog.Info("setting up field indexes")

	// Index Engrams by the EngramTemplate they reference.
	if err := mgr.GetFieldIndexer().IndexField(ctx, &bubushv1alpha1.Engram{}, "spec.templateRef.name", func(rawObj client.Object) []string {
		engram := rawObj.(*bubushv1alpha1.Engram)
		if engram.Spec.TemplateRef.Name == "" {
			return nil
		}
		return []string{engram.Spec.TemplateRef.Name}
	}); err != nil {
		setupLog.Error(err, "failed to index Engram spec.templateRef.name")
		os.Exit(1)
	}

	// Index StepRuns by the Engram they reference.
	if err := mgr.GetFieldIndexer().IndexField(ctx, &runsv1alpha1.StepRun{}, "spec.engramRef", func(rawObj client.Object) []string {
		stepRun := rawObj.(*runsv1alpha1.StepRun)
		if stepRun.Spec.EngramRef == nil || stepRun.Spec.EngramRef.Name == "" {
			return nil
		}
		return []string{stepRun.Spec.EngramRef.Name}
	}); err != nil {
		setupLog.Error(err, "failed to index StepRun spec.engramRef.name")
		os.Exit(1)
	}

	// Index StepRuns by the StoryRun they belong to.
	if err := mgr.GetFieldIndexer().IndexField(ctx, &runsv1alpha1.StepRun{}, "spec.storyRunRef.name", func(rawObj client.Object) []string {
		stepRun := rawObj.(*runsv1alpha1.StepRun)
		if stepRun.Spec.StoryRunRef.Name == "" {
			return nil
		}
		return []string{stepRun.Spec.StoryRunRef.Name}
	}); err != nil {
		setupLog.Error(err, "failed to index StepRun spec.storyRunRef.name")
		os.Exit(1)
	}

	// Index StoryRuns by the Impulse that triggered them.
	if err := mgr.GetFieldIndexer().IndexField(ctx, &runsv1alpha1.StoryRun{}, "spec.impulseRef.name", func(rawObj client.Object) []string {
		storyRun := rawObj.(*runsv1alpha1.StoryRun)
		if storyRun.Spec.ImpulseRef == nil || storyRun.Spec.ImpulseRef.Name == "" {
			return nil
		}
		return []string{storyRun.Spec.ImpulseRef.Name}
	}); err != nil {
		setupLog.Error(err, "failed to index StoryRun spec.impulseRef.name")
		os.Exit(1)
	}

	// Index Impulses by the ImpulseTemplate they reference.
	if err := mgr.GetFieldIndexer().IndexField(ctx, &bubushv1alpha1.Impulse{}, "spec.templateRef.name", func(rawObj client.Object) []string {
		impulse := rawObj.(*bubushv1alpha1.Impulse)
		if impulse.Spec.TemplateRef.Name == "" {
			return nil
		}
		return []string{impulse.Spec.TemplateRef.Name}
	}); err != nil {
		setupLog.Error(err, "failed to index Impulse spec.templateRef.name")
		os.Exit(1)
	}

	// Index StoryRuns by the Story they reference.
	if err := mgr.GetFieldIndexer().IndexField(ctx, &runsv1alpha1.StoryRun{}, "spec.storyRef.name", func(rawObj client.Object) []string {
		storyRun := rawObj.(*runsv1alpha1.StoryRun)
		if storyRun.Spec.StoryRef.Name == "" {
			return nil
		}
		return []string{storyRun.Spec.StoryRef.Name}
	}); err != nil {
		setupLog.Error(err, "failed to index StoryRun spec.storyRef.name")
		os.Exit(1)
	}

	// Index Impulses by the Story they trigger.
	if err := mgr.GetFieldIndexer().IndexField(ctx, &bubushv1alpha1.Impulse{}, "spec.storyRef.name", func(rawObj client.Object) []string {
		impulse := rawObj.(*bubushv1alpha1.Impulse)
		if impulse.Spec.StoryRef.Name == "" {
			return nil
		}
		return []string{impulse.Spec.StoryRef.Name}
	}); err != nil {
		setupLog.Error(err, "failed to index Impulse spec.storyRef.name")
		os.Exit(1)
	}

	// Index Stories by Engram names referenced in steps (spec.steps[].ref.name)
	if err := mgr.GetFieldIndexer().IndexField(ctx, &bubushv1alpha1.Story{}, "spec.steps.ref.name", func(rawObj client.Object) []string {
		story := rawObj.(*bubushv1alpha1.Story)
		if len(story.Spec.Steps) == 0 {
			return nil
		}
		nameSet := make(map[string]struct{})
		for i := range story.Spec.Steps {
			step := &story.Spec.Steps[i]
			if step.Ref != nil && step.Ref.Name != "" {
				nameSet[step.Ref.Name] = struct{}{}
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
	}); err != nil {
		setupLog.Error(err, "failed to index Story spec.steps.ref.name")
		os.Exit(1)
	}

	// Index EngramTemplates by description.
	if err := mgr.GetFieldIndexer().IndexField(ctx, &catalogv1alpha1.EngramTemplate{}, "spec.description",
		func(obj client.Object) []string {
			template := obj.(*catalogv1alpha1.EngramTemplate)
			if template.Spec.Description != "" {
				return []string{template.Spec.Description}
			}
			return []string{"no-description"}
		}); err != nil {
		setupLog.Error(err, "failed to index EngramTemplate spec.description")
		os.Exit(1)
	}

	// Index EngramTemplates by version.
	if err := mgr.GetFieldIndexer().IndexField(ctx, &catalogv1alpha1.EngramTemplate{}, "spec.version",
		func(obj client.Object) []string {
			return []string{obj.(*catalogv1alpha1.EngramTemplate).Spec.Version}
		}); err != nil {
		setupLog.Error(err, "failed to index EngramTemplate spec.version")
		os.Exit(1)
	}
}
