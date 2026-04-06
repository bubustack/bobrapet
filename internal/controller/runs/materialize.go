package runs

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/bubustack/bobrapet/api/runs/v1alpha1"
	bubuv1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/internal/config"
	"github.com/bubustack/bobrapet/pkg/enums"
	"github.com/bubustack/bobrapet/pkg/kubeutil"
	"github.com/bubustack/bobrapet/pkg/refs"
	runsidentity "github.com/bubustack/bobrapet/pkg/runs/identity"
	"github.com/bubustack/core/contracts"
	"github.com/bubustack/core/templating"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

const (
	materializeModeCondition = "condition"
	materializeModeObject    = "object"

	materializePurposeStepInput   = "step-input"
	materializePurposeStepConfig  = "step-config"
	materializePurposeIf          = "if"
	materializePurposeWait        = "wait"
	materializePurposeStoryOutput = "story-output"
	materializePurposeExecute     = "execute-story"
	materializePurposeParallel    = "parallel"
)

type materializeRequest struct {
	Mode     string         `json:"mode"`
	Template any            `json:"template"`
	Vars     map[string]any `json:"vars"`
}

func isMaterializeStepRun(step *v1alpha1.StepRun) bool {
	if step == nil {
		return false
	}
	if step.Labels == nil {
		return false
	}
	return strings.EqualFold(strings.TrimSpace(step.Labels[contracts.MaterializeLabelKey]), "true")
}

func materializeModeOverride(step *v1alpha1.StepRun) string {
	if step == nil || step.Annotations == nil {
		return ""
	}
	override := strings.TrimSpace(step.Annotations[contracts.MaterializeModeAnnotation])
	switch override {
	case string(enums.WorkloadModeJob), string(enums.WorkloadModeDeployment), string(enums.WorkloadModeStatefulSet):
		return override
	default:
		return ""
	}
}

func materializeStepID(purpose, stepName, childName string) string {
	parts := []string{"materialize", purpose}
	if stepName != "" {
		parts = append(parts, stepName)
	}
	if childName != "" {
		parts = append(parts, childName)
	}
	return kubeutil.ComposeName(parts...)
}

func materializeStepRunName(storyRunName, stepID string) string {
	return kubeutil.ComposeName(storyRunName, stepID)
}

func resolveMaterializeEngramName(resolver *config.Resolver) string {
	if resolver != nil {
		if cfg := resolver.GetOperatorConfig(); cfg != nil {
			if name := strings.TrimSpace(cfg.Controller.TemplateMaterializeEngram); name != "" {
				return name
			}
		}
	}
	return config.DefaultControllerConfig().TemplateMaterializeEngram
}

func ensureMaterializeEngram(ctx context.Context, c client.Client, namespace, engramName string) error {
	if strings.TrimSpace(engramName) == "" {
		return fmt.Errorf("materialize engram is not configured")
	}
	if strings.TrimSpace(namespace) == "" {
		return fmt.Errorf("materialize engram namespace is not set")
	}
	key := types.NamespacedName{Name: engramName, Namespace: namespace}
	var existing bubuv1alpha1.Engram
	if err := c.Get(ctx, key, &existing); err == nil {
		return nil
	} else if !apierrors.IsNotFound(err) {
		return err
	}

	engram := &bubuv1alpha1.Engram{
		ObjectMeta: metav1.ObjectMeta{
			Name:      engramName,
			Namespace: namespace,
			Labels: map[string]string{
				contracts.MaterializeLabelKey: "true",
			},
		},
		Spec: bubuv1alpha1.EngramSpec{
			TemplateRef: refs.EngramTemplateReference{Name: engramName},
		},
	}

	return c.Create(ctx, engram)
}

func buildMaterializeInput(mode string, template any, vars map[string]any) (*runtime.RawExtension, error) {
	if vars == nil {
		vars = map[string]any{}
	}
	req := materializeRequest{
		Mode:     mode,
		Template: template,
		Vars:     vars,
	}
	raw, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal materialize input: %w", err)
	}
	return &runtime.RawExtension{Raw: raw}, nil
}

//nolint:gocyclo // complex by design
func ensureMaterializeStepRun(
	ctx context.Context,
	c client.Client,
	scheme *runtime.Scheme,
	resolver *config.Resolver,
	storyRun *v1alpha1.StoryRun,
	purpose string,
	stepName string,
	childName string,
	mode string,
	template any,
	vars map[string]any,
	modeOverride string,
) (*v1alpha1.StepRun, error) {
	if storyRun == nil {
		return nil, fmt.Errorf("storyrun is required for materialize")
	}
	engramName := resolveMaterializeEngramName(resolver)
	if strings.TrimSpace(engramName) == "" {
		return nil, fmt.Errorf("materialize engram is not configured")
	}

	stepID := materializeStepID(purpose, stepName, childName)
	stepRunName := materializeStepRunName(storyRun.Name, stepID)
	key := types.NamespacedName{Name: stepRunName, Namespace: storyRun.Namespace}

	existing := &v1alpha1.StepRun{}
	if err := c.Get(ctx, key, existing); err == nil {
		if err := validateMaterializeStepRunIdentity(existing, storyRun, stepID, purpose, stepName, engramName, modeOverride); err != nil {
			return nil, fmt.Errorf("materialize steprun %q failed identity validation: %w", stepRunName, err)
		}
		return existing, nil
	} else if !apierrors.IsNotFound(err) {
		return nil, err
	}

	if err := ensureMaterializeEngram(ctx, c, storyRun.Namespace, engramName); err != nil {
		return nil, err
	}

	input, err := buildMaterializeInput(mode, template, vars)
	if err != nil {
		return nil, err
	}

	labels := runsidentity.SelectorLabels(storyRun.Name)
	if storyName := strings.TrimSpace(storyRun.Spec.StoryRef.Name); storyName != "" {
		labels[contracts.StoryNameLabelKey] = storyName
	}
	labels = applySchedulingLabelsFromStoryRun(labels, storyRun)
	labels[contracts.MaterializeLabelKey] = "true" //nolint:goconst // literal boolean string

	annotations := map[string]string{
		contracts.MaterializePurposeAnnotation: purpose,
	}
	if stepName != "" {
		annotations[contracts.MaterializeTargetAnnotation] = stepName
	}
	if modeOverride != "" {
		annotations[contracts.MaterializeModeAnnotation] = modeOverride
	}

	stepRun := v1alpha1.StepRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:        stepRunName,
			Namespace:   storyRun.Namespace,
			Labels:      labels,
			Annotations: annotations,
		},
		Spec: v1alpha1.StepRunSpec{
			StoryRunRef: refs.StoryRunReference{ObjectReference: refs.ObjectReference{Name: storyRun.Name}},
			StepID:      stepID,
			EngramRef: &refs.EngramReference{
				ObjectReference: refs.ObjectReference{Name: engramName},
			},
			Input: input,
		},
	}

	if err := controllerutil.SetControllerReference(storyRun, &stepRun, scheme); err != nil {
		return nil, err
	}
	if err := c.Create(ctx, &stepRun); err != nil {
		if apierrors.IsAlreadyExists(err) {
			// Cache was stale; re-read the existing object from the API server.
			if getErr := c.Get(ctx, key, existing); getErr != nil {
				return nil, getErr
			}
			if validateErr := validateMaterializeStepRunIdentity(existing, storyRun, stepID, purpose, stepName, engramName, modeOverride); validateErr != nil {
				return nil, fmt.Errorf("materialize steprun %q failed identity validation: %w", stepRunName, validateErr)
			}
			return existing, nil
		}
		return nil, err
	}
	return &stepRun, nil
}

//nolint:gocyclo // complex by design
func validateMaterializeStepRunIdentity(
	stepRun *v1alpha1.StepRun,
	storyRun *v1alpha1.StoryRun,
	expectedStepID string,
	expectedPurpose string,
	expectedStepName string,
	expectedEngramName string,
	expectedModeOverride string,
) error {
	if stepRun == nil {
		return fmt.Errorf("steprun is nil")
	}
	if storyRun == nil {
		return fmt.Errorf("storyrun is nil")
	}
	if !isMaterializeStepRun(stepRun) {
		return fmt.Errorf("missing materialize label")
	}
	if strings.TrimSpace(stepRun.Spec.StepID) != expectedStepID {
		return fmt.Errorf("spec.stepId mismatch: expected %q, got %q", expectedStepID, strings.TrimSpace(stepRun.Spec.StepID))
	}
	if strings.TrimSpace(stepRun.Spec.StoryRunRef.Name) != storyRun.Name {
		return fmt.Errorf("spec.storyRunRef.name mismatch: expected %q, got %q", storyRun.Name, strings.TrimSpace(stepRun.Spec.StoryRunRef.Name))
	}
	if stepRun.Spec.EngramRef == nil || strings.TrimSpace(stepRun.Spec.EngramRef.Name) != expectedEngramName {
		got := ""
		if stepRun.Spec.EngramRef != nil {
			got = strings.TrimSpace(stepRun.Spec.EngramRef.Name)
		}
		return fmt.Errorf("spec.engramRef.name mismatch: expected %q, got %q", expectedEngramName, got)
	}

	annotations := stepRun.GetAnnotations()
	if strings.TrimSpace(annotations[contracts.MaterializePurposeAnnotation]) != expectedPurpose {
		return fmt.Errorf("annotation %q mismatch: expected %q, got %q", contracts.MaterializePurposeAnnotation, expectedPurpose, strings.TrimSpace(annotations[contracts.MaterializePurposeAnnotation]))
	}
	if expectedStepName != "" && strings.TrimSpace(annotations[contracts.MaterializeTargetAnnotation]) != expectedStepName {
		return fmt.Errorf("annotation %q mismatch: expected %q, got %q", contracts.MaterializeTargetAnnotation, expectedStepName, strings.TrimSpace(annotations[contracts.MaterializeTargetAnnotation]))
	}
	if expectedModeOverride != "" && strings.TrimSpace(annotations[contracts.MaterializeModeAnnotation]) != expectedModeOverride {
		return fmt.Errorf("annotation %q mismatch: expected %q, got %q", contracts.MaterializeModeAnnotation, expectedModeOverride, strings.TrimSpace(annotations[contracts.MaterializeModeAnnotation]))
	}

	owner := metav1.GetControllerOf(stepRun)
	if owner == nil || owner.Controller == nil || !*owner.Controller {
		return fmt.Errorf("controller owner reference is required")
	}
	if owner.APIVersion != v1alpha1.GroupVersion.String() {
		return fmt.Errorf("owner apiVersion mismatch: expected %q, got %q", v1alpha1.GroupVersion.String(), owner.APIVersion)
	}
	if owner.Kind != "StoryRun" {
		return fmt.Errorf("owner kind mismatch: expected %q, got %q", "StoryRun", owner.Kind)
	}
	if owner.Name != storyRun.Name {
		return fmt.Errorf("owner name mismatch: expected %q, got %q", storyRun.Name, owner.Name)
	}
	if storyRun.UID != "" && owner.UID != storyRun.UID {
		return fmt.Errorf("owner uid mismatch: expected %q, got %q", storyRun.UID, owner.UID)
	}

	return nil
}

func readMaterializeResult(step *v1alpha1.StepRun) (any, error) {
	if step == nil || step.Status.Output == nil || len(step.Status.Output.Raw) == 0 {
		return nil, fmt.Errorf("materialize step %s has no output", stepNameOrUnknown(step))
	}
	var payload map[string]any
	if err := json.Unmarshal(step.Status.Output.Raw, &payload); err != nil {
		return nil, fmt.Errorf("materialize step %s output decode failed: %w", stepNameOrUnknown(step), err)
	}
	result, ok := payload["result"]
	if !ok {
		return nil, fmt.Errorf("materialize step %s output missing result", stepNameOrUnknown(step))
	}
	return result, nil
}

func stepNameOrUnknown(step *v1alpha1.StepRun) string {
	if step == nil || step.Name == "" {
		return "<unknown>"
	}
	return step.Name
}

func resolveMaterialize(
	ctx context.Context,
	c client.Client,
	scheme *runtime.Scheme,
	resolver *config.Resolver,
	storyRun *v1alpha1.StoryRun,
	purpose string,
	stepName string,
	childName string,
	mode string,
	template any,
	vars map[string]any,
) (any, error) {
	modeOverride := string(enums.WorkloadModeJob)
	stepRun, err := ensureMaterializeStepRun(ctx, c, scheme, resolver, storyRun, purpose, stepName, childName, mode, template, vars, modeOverride)
	if err != nil {
		return nil, err
	}

	if stepRun.Status.Phase == enums.PhaseSucceeded {
		return readMaterializeResult(stepRun)
	}
	if stepRun.Status.Phase.IsTerminal() && stepRun.Status.Phase != enums.PhaseSucceeded {
		msg := stepRun.Status.LastFailureMsg
		if msg == "" {
			msg = fmt.Sprintf("materialize step %s ended with phase %s", stepRun.Name, stepRun.Status.Phase)
		}
		return nil, fmt.Errorf("%s", msg)
	}
	return nil, &templating.ErrEvaluationBlocked{Reason: fmt.Sprintf("materialize step %s is still running", stepRun.Name)}
}
