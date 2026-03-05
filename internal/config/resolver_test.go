package config

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	catalogv1alpha1 "github.com/bubustack/bobrapet/api/catalog/v1alpha1"
	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	v1alpha1 "github.com/bubustack/bobrapet/api/v1alpha1"
	"github.com/bubustack/bobrapet/pkg/enums"
	"github.com/bubustack/bobrapet/pkg/refs"
	runsidentity "github.com/bubustack/bobrapet/pkg/runs/identity"
)

const (
	testStoryServiceAccount = "story-sa"
	testStepServiceAccount  = "step-sa"
)

func TestApplyStoryExecutionPolicyDefaults(t *testing.T) {
	resolved := &ResolvedExecutionConfig{
		Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{},
			Limits:   corev1.ResourceList{},
		},
	}
	cpuReq := "250m"
	memReq := "128Mi"
	cpuLimit := "1"
	memLimit := "256Mi"
	runAsNonRoot := true
	allowEscalation := false
	readOnly := true
	runAsUser := int64(2000)
	backoffLimit := int32(5)
	ttl := int32(30)
	restart := string(corev1.RestartPolicyOnFailure)
	maxRetries := int32(4)
	timeout := "45s"
	serviceAccount := testStoryServiceAccount
	policy := &v1alpha1.ExecutionPolicy{
		Resources: &v1alpha1.ResourcePolicy{
			CPURequest:    &cpuReq,
			MemoryRequest: &memReq,
			CPULimit:      &cpuLimit,
			MemoryLimit:   &memLimit,
		},
		Security: &v1alpha1.SecurityPolicy{
			RunAsNonRoot:             &runAsNonRoot,
			AllowPrivilegeEscalation: &allowEscalation,
			ReadOnlyRootFilesystem:   &readOnly,
			RunAsUser:                &runAsUser,
		},
		Job: &v1alpha1.JobPolicy{
			BackoffLimit:            &backoffLimit,
			TTLSecondsAfterFinished: &ttl,
			RestartPolicy:           &restart,
		},
		Retry: &v1alpha1.RetryPolicy{
			MaxRetries: &maxRetries,
		},
		Timeout:            &timeout,
		ServiceAccountName: &serviceAccount,
		Storage: &v1alpha1.StoragePolicy{
			File: &v1alpha1.FileStorageProvider{Path: "/data"},
		},
	}
	story := &v1alpha1.Story{
		Spec: v1alpha1.StorySpec{
			Policy: &v1alpha1.StoryPolicy{
				Execution: policy,
			},
		},
	}

	resolver := &Resolver{}
	if err := resolver.applyStoryConfig(context.Background(), story, nil, nil, resolved); err != nil {
		t.Fatalf("applyStoryConfig returned error: %v", err)
	}

	assertResourceQuantity(t, resolved.Resources.Requests, corev1.ResourceCPU, resource.MustParse(cpuReq), "cpu request")
	assertResourceQuantity(t, resolved.Resources.Requests, corev1.ResourceMemory, resource.MustParse(memReq), "memory request")
	assertResourceQuantity(t, resolved.Resources.Limits, corev1.ResourceCPU, resource.MustParse(cpuLimit), "cpu limit")
	assertResourceQuantity(t, resolved.Resources.Limits, corev1.ResourceMemory, resource.MustParse(memLimit), "memory limit")
	assertEqual(t, resolved.RunAsNonRoot, runAsNonRoot, "RunAsNonRoot")
	assertEqual(t, resolved.AllowPrivilegeEscalation, allowEscalation, "AllowPrivilegeEscalation")
	assertEqual(t, resolved.ReadOnlyRootFilesystem, readOnly, "ReadOnlyRootFilesystem")
	assertEqual(t, resolved.RunAsUser, runAsUser, "RunAsUser")
	assertEqual(t, resolved.BackoffLimit, backoffLimit, "BackoffLimit")
	assertEqual(t, resolved.TTLSecondsAfterFinished, ttl, "TTLSecondsAfterFinished")
	assertEqual(t, resolved.RestartPolicy, corev1.RestartPolicyOnFailure, "RestartPolicy")
	assertEqual(t, resolved.MaxRetries, int(maxRetries), "MaxRetries")
	assertEqual(t, resolved.DefaultStepTimeout, 45*time.Second, "DefaultStepTimeout")
	assertEqual(t, resolved.ServiceAccountName, serviceAccount, "ServiceAccountName")
	assertStoragePath(t, resolved.Storage, "/data")
}

func TestApplyStoryConfigStepOverrides(t *testing.T) {
	resolved := &ResolvedExecutionConfig{
		ServiceAccountName: "default",
		RunAsNonRoot:       true,
		Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{},
			Limits:   corev1.ResourceList{},
		},
	}
	storySA := testStoryServiceAccount
	runAsNonRoot := true
	story := &v1alpha1.Story{
		Spec: v1alpha1.StorySpec{
			Policy: &v1alpha1.StoryPolicy{
				Execution: &v1alpha1.ExecutionPolicy{
					ServiceAccountName: &storySA,
					Security: &v1alpha1.SecurityPolicy{
						RunAsNonRoot: &runAsNonRoot,
					},
				},
			},
		},
	}

	stepSA := testStepServiceAccount
	stepRunAsNonRoot := false
	stepDebug := true
	step := &v1alpha1.Step{
		Name: "worker",
		Execution: &v1alpha1.ExecutionOverrides{
			ServiceAccountName: &stepSA,
			Debug:              &stepDebug,
			Security: &v1alpha1.WorkloadSecurity{
				RunAsNonRoot: &stepRunAsNonRoot,
			},
		},
	}

	resolver := &Resolver{}
	if err := resolver.applyStoryConfig(context.Background(), story, nil, step, resolved); err != nil {
		t.Fatalf("applyStoryConfig returned error: %v", err)
	}

	if resolved.ServiceAccountName != stepSA {
		t.Fatalf("expected step-level service account to win, got %s", resolved.ServiceAccountName)
	}
	if resolved.RunAsNonRoot != stepRunAsNonRoot {
		t.Fatalf("expected RunAsNonRoot=%v, got %v", stepRunAsNonRoot, resolved.RunAsNonRoot)
	}
	if resolved.DebugLogs != stepDebug {
		t.Fatalf("expected DebugLogs=%v, got %v", stepDebug, resolved.DebugLogs)
	}
}

func assertResourceQuantity(t *testing.T, list corev1.ResourceList, name corev1.ResourceName, want resource.Quantity, field string) {
	t.Helper()
	got, ok := list[name]
	if !ok {
		t.Fatalf("expected %s to be set", field)
	}
	if got.Cmp(want) != 0 {
		t.Fatalf("expected %s to be %s, got %s", field, want.String(), got.String())
	}
}

func assertEqual[T comparable](t *testing.T, got, want T, field string) {
	t.Helper()
	if got != want {
		t.Fatalf("expected %s=%v, got %v", field, want, got)
	}
}

func assertStoragePath(t *testing.T, storage *v1alpha1.StoragePolicy, path string) {
	t.Helper()
	if storage == nil || storage.File == nil || storage.File.Path != path {
		t.Fatalf("expected storage path %s, got %#v", path, storage)
	}
}

func TestResolveExecutionConfigDefaultsToIdentityHelper(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add corev1 scheme: %v", err)
	}
	manager := NewOperatorConfigManager(fake.NewClientBuilder().WithScheme(scheme).Build(), "default", "cfg")
	resolver := NewResolver(nil, manager)
	storyRunName := "demo-storyrun"
	step := &runsv1alpha1.StepRun{
		Spec: runsv1alpha1.StepRunSpec{
			StoryRunRef: refs.StoryRunReference{
				ObjectReference: refs.ObjectReference{Name: storyRunName},
			},
		},
	}
	story := &v1alpha1.Story{}
	engram := &v1alpha1.Engram{}
	cfg, err := resolver.ResolveExecutionConfig(
		context.Background(),
		step,
		story,
		engram,
		&catalogv1alpha1.EngramTemplate{},
		nil,
	)
	if err != nil {
		t.Fatalf("ResolveExecutionConfig returned error: %v", err)
	}
	expected := runsidentity.NewEngramRunner(storyRunName).ServiceAccountName()
	if cfg.ServiceAccountName != expected {
		t.Fatalf("expected runner service account %s, got %s", expected, cfg.ServiceAccountName)
	}
}

func TestResolveExecutionConfigTemplateInvalidResourceDoesNotOverride(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add corev1 scheme: %v", err)
	}
	manager := NewOperatorConfigManager(fake.NewClientBuilder().WithScheme(scheme).Build(), "default", "cfg")
	resolver := NewResolver(nil, manager)

	engramCPU := "200m"
	templateBadCPU := "definitely-not-a-quantity"
	template := &catalogv1alpha1.EngramTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: "tmpl"},
		Spec: catalogv1alpha1.EngramTemplateSpec{
			TemplateSpec: catalogv1alpha1.TemplateSpec{
				Version:        "v1",
				SupportedModes: []enums.WorkloadMode{enums.WorkloadModeJob},
				Execution: &catalogv1alpha1.TemplateExecutionPolicy{
					Resources: &catalogv1alpha1.TemplateResourcePolicy{
						RecommendedCPURequest: &templateBadCPU,
					},
				},
			},
		},
	}
	engram := &v1alpha1.Engram{
		ObjectMeta: metav1.ObjectMeta{Name: "engram"},
		Spec: v1alpha1.EngramSpec{
			ExecutionPolicy: &v1alpha1.ExecutionPolicy{
				Resources: &v1alpha1.ResourcePolicy{
					CPURequest: &engramCPU,
				},
			},
		},
	}
	story := &v1alpha1.Story{
		ObjectMeta: metav1.ObjectMeta{Name: "story"},
		Spec: v1alpha1.StorySpec{
			Steps: []v1alpha1.Step{{Name: "worker"}},
		},
	}
	stepRun := &runsv1alpha1.StepRun{
		ObjectMeta: metav1.ObjectMeta{Name: "step-run"},
		Spec: runsv1alpha1.StepRunSpec{
			StepID: "worker",
			StoryRunRef: refs.StoryRunReference{
				ObjectReference: refs.ObjectReference{Name: "storyrun"},
			},
		},
	}

	cfg, err := resolver.ResolveExecutionConfig(context.Background(), stepRun, story, engram, template, nil)
	if err != nil {
		t.Fatalf("ResolveExecutionConfig returned error: %v", err)
	}

	gotCPU := cfg.Resources.Requests[corev1.ResourceCPU]
	if want := resource.MustParse(engramCPU); gotCPU.Cmp(want) != 0 {
		t.Fatalf("expected Engram CPU request %s to remain after invalid template value, got %s", want.String(), gotCPU.String())
	}
}

func TestResolveExecutionConfigRejectsInvalidRunAsUser(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add corev1 scheme: %v", err)
	}
	manager := NewOperatorConfigManager(fake.NewClientBuilder().WithScheme(scheme).Build(), "default", "cfg")
	resolver := NewResolver(nil, manager)

	badUID := int64(0)
	story := &v1alpha1.Story{
		ObjectMeta: metav1.ObjectMeta{Name: "story"},
		Spec: v1alpha1.StorySpec{
			Steps: []v1alpha1.Step{
				{
					Name: "worker",
					Execution: &v1alpha1.ExecutionOverrides{
						Security: &v1alpha1.WorkloadSecurity{
							RunAsUser: &badUID,
						},
					},
				},
			},
		},
	}
	stepRun := &runsv1alpha1.StepRun{
		ObjectMeta: metav1.ObjectMeta{Name: "step-run"},
		Spec: runsv1alpha1.StepRunSpec{
			StepID: "worker",
			StoryRunRef: refs.StoryRunReference{
				ObjectReference: refs.ObjectReference{Name: "storyrun"},
			},
		},
	}

	_, err := resolver.ResolveExecutionConfig(context.Background(), stepRun, story, &v1alpha1.Engram{}, &catalogv1alpha1.EngramTemplate{}, nil)
	if err == nil {
		t.Fatalf("expected invalid runAsUser to return error")
	}
}

func TestResolveExecutionConfigPrecedenceOrder(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add corev1 scheme: %v", err)
	}
	manager := NewOperatorConfigManager(fake.NewClientBuilder().WithScheme(scheme).Build(), "default", "cfg")
	resolver := NewResolver(nil, manager)

	templateCPU := "250m"
	templateImage := "template:latest"
	templateNonRoot := true
	template := &catalogv1alpha1.EngramTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: "tmpl"},
		Spec: catalogv1alpha1.EngramTemplateSpec{
			TemplateSpec: catalogv1alpha1.TemplateSpec{
				Version:        "v1",
				SupportedModes: []enums.WorkloadMode{enums.WorkloadModeJob},
				Image:          templateImage,
				Execution: &catalogv1alpha1.TemplateExecutionPolicy{
					Resources: &catalogv1alpha1.TemplateResourcePolicy{
						RecommendedCPURequest: &templateCPU,
					},
					Security: &catalogv1alpha1.TemplateSecurityPolicy{
						RequiresNonRoot: &templateNonRoot,
					},
				},
			},
		},
	}

	engramCPU := "200m"
	engram := &v1alpha1.Engram{
		ObjectMeta: metav1.ObjectMeta{Name: "engram"},
		Spec: v1alpha1.EngramSpec{
			ExecutionPolicy: &v1alpha1.ExecutionPolicy{
				Resources: &v1alpha1.ResourcePolicy{
					CPURequest: &engramCPU,
				},
			},
		},
	}

	storyServiceAccount := testStoryServiceAccount
	stepSAStr := testStepServiceAccount
	story := &v1alpha1.Story{
		ObjectMeta: metav1.ObjectMeta{Name: "story"},
		Spec: v1alpha1.StorySpec{
			Policy: &v1alpha1.StoryPolicy{
				Execution: &v1alpha1.ExecutionPolicy{
					ServiceAccountName: &storyServiceAccount,
				},
			},
			Steps: []v1alpha1.Step{
				{
					Name: "worker",
					Execution: &v1alpha1.ExecutionOverrides{
						ServiceAccountName: &stepSAStr,
						Debug:              boolPtr(true),
					},
				},
			},
		},
	}

	stepCPU := "50m"
	stepRunTimeout := "75s"
	stepRun := &runsv1alpha1.StepRun{
		ObjectMeta: metav1.ObjectMeta{Name: "step-run"},
		Spec: runsv1alpha1.StepRunSpec{
			StepID: "worker",
			StoryRunRef: refs.StoryRunReference{
				ObjectReference: refs.ObjectReference{Name: "storyrun"},
			},
			ExecutionOverrides: &runsv1alpha1.StepExecutionOverrides{
				CPURequest: &stepCPU,
			},
			Timeout: stepRunTimeout,
		},
	}

	cfg, err := resolver.ResolveExecutionConfig(context.Background(), stepRun, story, engram, template, nil)
	if err != nil {
		t.Fatalf("ResolveExecutionConfig returned error: %v", err)
	}

	if cfg.Image != templateImage {
		t.Fatalf("expected template image %s, got %s", templateImage, cfg.Image)
	}
	if cfg.ServiceAccountName != stepSAStr {
		t.Fatalf("expected step ServiceAccount %s, got %s", stepSAStr, cfg.ServiceAccountName)
	}
	if !cfg.DebugLogs {
		t.Fatalf("expected DebugLogs=true from story step overrides")
	}
	if !cfg.RunAsNonRoot {
		t.Fatalf("expected RunAsNonRoot=true from template security")
	}
	if got := cfg.Resources.Requests[corev1.ResourceCPU]; got.Cmp(resource.MustParse(stepCPU)) != 0 {
		t.Fatalf("expected CPU request %s from step override, got %s", stepCPU, got.String())
	}
	if cfg.DefaultStepTimeout != 75*time.Second {
		t.Fatalf("expected DefaultStepTimeout=75s, got %s", cfg.DefaultStepTimeout)
	}
}

func TestResolveExecutionConfigChainTelemetryParity(t *testing.T) {
	recorder, logger := newRecordingLogger()
	previous := resolverLogger
	resolverLogger = logger.WithName("config-resolver")
	t.Cleanup(func() {
		resolverLogger = previous
	})

	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add corev1 scheme: %v", err)
	}
	manager := NewOperatorConfigManager(fake.NewClientBuilder().WithScheme(scheme).Build(), "default", "cfg")
	resolver := NewResolver(nil, manager)

	badTemplateCPU := "definitely-not-a-quantity"
	template := &catalogv1alpha1.EngramTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: "tmpl"},
		Spec: catalogv1alpha1.EngramTemplateSpec{
			TemplateSpec: catalogv1alpha1.TemplateSpec{
				Version:        "v1",
				SupportedModes: []enums.WorkloadMode{enums.WorkloadModeJob},
				Image:          "template:latest",
				Execution: &catalogv1alpha1.TemplateExecutionPolicy{
					Images: &catalogv1alpha1.TemplateImagePolicy{
						PullPolicy: stringPtr(string(corev1.PullAlways)),
					},
					Resources: &catalogv1alpha1.TemplateResourcePolicy{
						RecommendedCPURequest: &badTemplateCPU,
					},
					Security: &catalogv1alpha1.TemplateSecurityPolicy{
						RequiresNonRoot: boolPtr(true),
					},
					Job: &catalogv1alpha1.TemplateJobPolicy{
						RecommendedBackoffLimit: int32Ptr(2),
					},
					Retry: &catalogv1alpha1.TemplateRetryPolicy{
						RecommendedMaxRetries: intPtr(3),
					},
					Timeout: stringPtr("45s"),
					Service: &catalogv1alpha1.TemplateServicePolicy{
						Ports: []catalogv1alpha1.TemplateServicePort{{Name: "http", Port: 80, TargetPort: 8080}},
					},
					Probes: &catalogv1alpha1.TemplateProbePolicy{
						Liveness: &corev1.Probe{},
					},
					Storage: &catalogv1alpha1.TemplateStoragePolicy{
						File: &catalogv1alpha1.TemplateFileStorageProvider{Path: "/tmpl"},
					},
				},
			},
		},
	}

	engramCPU := "150m"
	engramSA := "engram-sa"
	engram := &v1alpha1.Engram{
		ObjectMeta: metav1.ObjectMeta{Name: "engram"},
		Spec: v1alpha1.EngramSpec{
			ExecutionPolicy: &v1alpha1.ExecutionPolicy{
				Resources: &v1alpha1.ResourcePolicy{
					CPURequest: &engramCPU,
				},
			},
			Overrides: &v1alpha1.ExecutionOverrides{
				ServiceAccountName: &engramSA,
			},
		},
	}

	storyServiceAccount := testStoryServiceAccount
	storyStepSA := "story-step-sa"
	story := &v1alpha1.Story{
		ObjectMeta: metav1.ObjectMeta{Name: "story"},
		Spec: v1alpha1.StorySpec{
			Policy: &v1alpha1.StoryPolicy{
				Execution: &v1alpha1.ExecutionPolicy{
					ServiceAccountName: &storyServiceAccount,
				},
				Storage: &v1alpha1.StoragePolicy{
					File: &v1alpha1.FileStorageProvider{Path: "/story"},
				},
			},
			Steps: []v1alpha1.Step{
				{
					Name: "worker",
					Execution: &v1alpha1.ExecutionOverrides{
						ServiceAccountName: &storyStepSA,
						Debug:              boolPtr(true),
					},
				},
			},
		},
	}

	stepCPU := "25m"
	stepRunRetry := int32(4)
	stepRun := &runsv1alpha1.StepRun{
		ObjectMeta: metav1.ObjectMeta{Name: "step-run"},
		Spec: runsv1alpha1.StepRunSpec{
			StepID: "worker",
			StoryRunRef: refs.StoryRunReference{
				ObjectReference: refs.ObjectReference{Name: "storyrun"},
			},
			ExecutionOverrides: &runsv1alpha1.StepExecutionOverrides{
				CPURequest: &stepCPU,
			},
			Timeout: "90s",
			Retry: &v1alpha1.RetryPolicy{
				MaxRetries: &stepRunRetry,
			},
		},
	}

	cfg, err := resolver.ResolveExecutionConfig(context.Background(), stepRun, story, engram, template, nil)
	if err != nil {
		t.Fatalf("ResolveExecutionConfig returned error: %v", err)
	}

	if got := cfg.Resources.Requests[corev1.ResourceCPU]; got.Cmp(resource.MustParse(stepCPU)) != 0 {
		t.Fatalf("expected StepRun CPU %s to win, got %s", stepCPU, got.String())
	}
	if cfg.ServiceAccountName != storyStepSA {
		t.Fatalf("expected story step service account %s, got %s", storyStepSA, cfg.ServiceAccountName)
	}

	entries := recorder.Entries()
	gotStages := collectStageSequence(entries)
	expectedStages := []string{
		"config-resolver/template-chain:imagePullPolicy",
		"config-resolver/template-chain:resources",
		"config-resolver/template-chain:security",
		"config-resolver/template-chain:job",
		"config-resolver/template-chain:timeoutRetry",
		"config-resolver/template-chain:probes",
		"config-resolver/template-chain:service",
		"config-resolver/template-chain:storage",
		"config-resolver/engram-chain:resources",
		"config-resolver/engram-chain:placement",
		"config-resolver/engram-chain:cache",
		"config-resolver/engram-chain:executionOverrides",
		"config-resolver/overrides-chain:basic",
		"config-resolver/overrides-chain:security",
		"config-resolver/overrides-chain:placement",
		"config-resolver/overrides-chain:cache",
		"config-resolver/overrides-chain:imagePolicy",
		"config-resolver/overrides-chain:timeoutRetry",
		"config-resolver/overrides-chain:probes",
		"config-resolver/overrides-chain:storage",
		"config-resolver/story-chain:policyExecution",
		"config-resolver/story-chain:stepExecutionOverrides",
		"config-resolver/overrides-chain:basic",
		"config-resolver/overrides-chain:security",
		"config-resolver/overrides-chain:placement",
		"config-resolver/overrides-chain:cache",
		"config-resolver/overrides-chain:imagePolicy",
		"config-resolver/overrides-chain:timeoutRetry",
		"config-resolver/overrides-chain:probes",
		"config-resolver/overrides-chain:storage",
		"config-resolver/story-chain:policyStorage",
		"config-resolver/steprun-chain:executionOverrides",
		"config-resolver/steprun-chain:timeout",
		"config-resolver/steprun-chain:retry",
	}
	if diff := cmp.Diff(expectedStages, gotStages); diff != "" {
		t.Fatalf("unexpected stage sequence (-want +got):\n%s", diff)
	}

	var skipLogs []logRecord
	for _, entry := range entries {
		if entry.msg == "resolver stage skipped" {
			skipLogs = append(skipLogs, entry)
		}
	}
	if len(skipLogs) == 0 {
		t.Fatalf("expected at least one resolver stage skipped log")
	}
	if stage := findValue(skipLogs[0].kv, "stage"); stage != "resources" {
		t.Fatalf("expected template resources stage to skip, got %s", stage)
	}
	if errMsg := findValue(skipLogs[0].kv, "error"); !strings.Contains(errMsg, "cpu request") {
		t.Fatalf("expected cpu request parse error, got %s", errMsg)
	}
}

func boolPtr(v bool) *bool { return &v }

func int32Ptr(v int32) *int32 { return &v }

func intPtr(v int) *int { return &v }

func stringPtr(v string) *string { return &v }

type logRecord struct {
	name  string
	msg   string
	level int
	kv    []any
	err   error
}

type logRecorder struct {
	mu      sync.Mutex
	entries []logRecord
}

func newRecordingLogger() (*logRecorder, logr.Logger) {
	recorder := &logRecorder{}
	return recorder, logr.New(&recordingSink{shared: recorder})
}

func (r *logRecorder) append(entry logRecord) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.entries = append(r.entries, entry)
}

func (r *logRecorder) Entries() []logRecord {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]logRecord, len(r.entries))
	copy(out, r.entries)
	return out
}

type recordingSink struct {
	shared *logRecorder
	name   string
	values []any
}

func (s *recordingSink) Init(logr.RuntimeInfo) {}

func (s *recordingSink) Enabled(level int) bool { return true }

func (s *recordingSink) Info(level int, msg string, kv ...any) {
	s.record(level, msg, kv, nil)
}

func (s *recordingSink) Error(err error, msg string, kv ...any) {
	s.record(0, msg, kv, err)
}

func (s *recordingSink) WithValues(kv ...any) logr.LogSink {
	child := s.clone()
	child.values = append(child.values, kv...)
	return child
}

func (s *recordingSink) WithName(name string) logr.LogSink {
	child := s.clone()
	if s.name == "" {
		child.name = name
	} else {
		child.name = s.name + "/" + name
	}
	return child
}

func (s *recordingSink) clone() *recordingSink {
	return &recordingSink{
		shared: s.shared,
		name:   s.name,
		values: append([]any{}, s.values...),
	}
}

func (s *recordingSink) record(level int, msg string, kv []any, err error) {
	entryKV := append([]any{}, s.values...)
	entryKV = append(entryKV, kv...)
	s.shared.append(logRecord{
		name:  s.name,
		msg:   msg,
		level: level,
		kv:    entryKV,
		err:   err,
	})
}

func collectStageSequence(entries []logRecord) []string {
	stages := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.msg != "resolver stage start" {
			continue
		}
		stage := findValue(entry.kv, "stage")
		stages = append(stages, fmt.Sprintf("%s:%s", entry.name, stage))
	}
	return stages
}

func findValue(kv []any, key string) string {
	for i := 0; i < len(kv)-1; i += 2 {
		if k, ok := kv[i].(string); ok && k == key {
			switch v := kv[i+1].(type) {
			case string:
				return v
			case fmt.Stringer:
				return v.String()
			default:
				return fmt.Sprint(v)
			}
		}
	}
	return ""
}
