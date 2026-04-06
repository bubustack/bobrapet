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

package transport

import (
	"testing"

	"github.com/bubustack/bobrapet/api/v1alpha1"
	"k8s.io/apimachinery/pkg/runtime"
)

func TestTopologyAnalyzer_LiveKitWorkflow(t *testing.T) {
	// Create a story similar to the livekit-voice-assistant workflow
	story := &v1alpha1.Story{
		Spec: v1alpha1.StorySpec{
			Steps: []v1alpha1.Step{
				{
					Name: "ingress",
					// No runtime config - can be P2P target
				},
				{
					Name:  "buffer",
					Needs: []string{"ingress"},
					// No runtime config - can be P2P target
				},
				{
					Name:  "transcribe",
					Needs: []string{"buffer"},
					// No runtime config - can be P2P target
				},
				{
					Name:  "respond",
					Needs: []string{"transcribe"},
					// HAS runtime config - needs hub routing
					Runtime: &runtime.RawExtension{
						Raw: []byte(`{"userPrompt": "{{ steps.transcribe.text }}"}`),
					},
				},
				{
					Name:  "synthesize",
					Needs: []string{"respond"},
					// HAS runtime config - needs hub routing
					Runtime: &runtime.RawExtension{
						Raw: []byte(`{"text": "{{ steps.respond.text }}"}`),
					},
				},
				{
					Name:  "playback",
					Needs: []string{"synthesize"},
					// No runtime config - can be P2P target
				},
			},
		},
	}

	analyzer := NewTopologyAnalyzer(story)

	tests := []struct {
		stepName        string
		expectedMode    RoutingMode
		expectedDownstr []string
	}{
		{
			stepName:        "ingress",
			expectedMode:    RoutingModeP2P, // buffer doesn't need hub
			expectedDownstr: []string{"buffer"},
		},
		{
			stepName:        "buffer",
			expectedMode:    RoutingModeP2P, // transcribe doesn't need hub
			expectedDownstr: []string{"transcribe"},
		},
		{
			stepName:        "transcribe",
			expectedMode:    RoutingModeHub, // respond needs hub (has runtime config)
			expectedDownstr: []string{"respond"},
		},
		{
			stepName:        "respond",
			expectedMode:    RoutingModeHub, // synthesize needs hub (has runtime config)
			expectedDownstr: []string{"synthesize"},
		},
		{
			stepName:        "synthesize",
			expectedMode:    RoutingModeHub, // synthesize has runtime config, must use hub
			expectedDownstr: []string{"playback"},
		},
		{
			stepName:        "playback",
			expectedMode:    RoutingModeHub, // no downstream
			expectedDownstr: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.stepName, func(t *testing.T) {
			info, err := analyzer.AnalyzeStepRouting(tt.stepName)
			if err != nil {
				t.Fatalf("AnalyzeStepRouting(%s) error = %v", tt.stepName, err)
			}

			if info.RoutingMode != tt.expectedMode {
				t.Errorf("RoutingMode = %v, want %v", info.RoutingMode, tt.expectedMode)
			}

			if len(info.DownstreamSteps) != len(tt.expectedDownstr) {
				t.Errorf("DownstreamSteps count = %d, want %d", len(info.DownstreamSteps), len(tt.expectedDownstr))
			}

			for i, expected := range tt.expectedDownstr {
				if i >= len(info.DownstreamSteps) || info.DownstreamSteps[i] != expected {
					t.Errorf("DownstreamSteps[%d] = %v, want %v", i, info.DownstreamSteps, expected)
				}
			}
		})
	}
}

func TestTopologyAnalyzer_MultipleDownstreams(t *testing.T) {
	// Test case where a step has multiple downstream steps (must use hub)
	story := &v1alpha1.Story{
		Spec: v1alpha1.StorySpec{
			Steps: []v1alpha1.Step{
				{
					Name: "source",
				},
				{
					Name:  "target1",
					Needs: []string{"source"},
				},
				{
					Name:  "target2",
					Needs: []string{"source"},
				},
			},
		},
	}

	analyzer := NewTopologyAnalyzer(story)
	info, err := analyzer.AnalyzeStepRouting("source")
	if err != nil {
		t.Fatalf("AnalyzeStepRouting(source) error = %v", err)
	}

	if info.RoutingMode != RoutingModeHub {
		t.Errorf("RoutingMode = %v, want %v (multiple downstream requires hub)", info.RoutingMode, RoutingModeHub)
	}

	if len(info.DownstreamSteps) != 2 {
		t.Errorf("DownstreamSteps count = %d, want 2", len(info.DownstreamSteps))
	}
}

func TestTopologyAnalyzer_PreventsMixedHubAndP2PUpstreamDelivery(t *testing.T) {
	// Reproduces livekit ingress fan-out:
	// ingress -> buffer
	// ingress -> greet
	//
	// ingress must use hub (multiple downstream). buffer has one downstream
	// (transcribe), but it must also stay on hub so hub-routed ingress packets
	// can be delivered to buffer.
	story := &v1alpha1.Story{
		Spec: v1alpha1.StorySpec{
			Steps: []v1alpha1.Step{
				{Name: "ingress"},
				{
					Name:  "buffer",
					Needs: []string{"ingress"},
				},
				{
					Name:  "greet",
					Needs: []string{"ingress"},
				},
				{
					Name:  "transcribe",
					Needs: []string{"buffer"},
				},
			},
		},
	}

	analyzer := NewTopologyAnalyzer(story)

	ingress, err := analyzer.AnalyzeStepRouting("ingress")
	if err != nil {
		t.Fatalf("AnalyzeStepRouting(ingress) error = %v", err)
	}
	if ingress.RoutingMode != RoutingModeHub {
		t.Fatalf("ingress RoutingMode = %v, want %v", ingress.RoutingMode, RoutingModeHub)
	}

	buffer, err := analyzer.AnalyzeStepRouting("buffer")
	if err != nil {
		t.Fatalf("AnalyzeStepRouting(buffer) error = %v", err)
	}
	if buffer.RoutingMode != RoutingModeHub {
		t.Fatalf("buffer RoutingMode = %v, want %v", buffer.RoutingMode, RoutingModeHub)
	}
}

func TestTopologyAnalyzer_HasP2PUpstream(t *testing.T) {
	story := &v1alpha1.Story{
		Spec: v1alpha1.StorySpec{
			Steps: []v1alpha1.Step{
				{Name: "ingress"},
				{
					Name:  "buffer",
					Needs: []string{"ingress"},
				},
				{
					Name:  "respond",
					Needs: []string{"buffer"},
					Runtime: &runtime.RawExtension{
						Raw: []byte(`{"prompt":"{{ steps.buffer.text }}"}`),
					},
				},
				{
					Name:  "sink",
					Needs: []string{"respond"},
				},
			},
		},
	}

	analyzer := NewTopologyAnalyzer(story)

	tests := []struct {
		step     string
		expected bool
	}{
		{step: "ingress", expected: false},
		{step: "buffer", expected: true},
		{step: "respond", expected: false},
		{step: "sink", expected: false},
	}

	for _, tt := range tests {
		t.Run(tt.step, func(t *testing.T) {
			if got := analyzer.HasP2PUpstream(tt.step); got != tt.expected {
				t.Fatalf("HasP2PUpstream(%s) = %v, want %v", tt.step, got, tt.expected)
			}
		})
	}
}

func TestCanUseP2PRouting(t *testing.T) {
	tests := []struct {
		name              string
		upstream          *v1alpha1.Step
		downstream        *v1alpha1.Step
		expectedCanUseP2P bool
	}{
		{
			name:     "both without runtime config",
			upstream: &v1alpha1.Step{Name: "step1"},
			downstream: &v1alpha1.Step{
				Name: "step2",
			},
			expectedCanUseP2P: true,
		},
		{
			name:     "downstream has runtime config",
			upstream: &v1alpha1.Step{Name: "step1"},
			downstream: &v1alpha1.Step{
				Name: "step2",
				Runtime: &runtime.RawExtension{
					Raw: []byte(`{"key": "value"}`),
				},
			},
			expectedCanUseP2P: false,
		},
		{
			name:     "downstream has if condition",
			upstream: &v1alpha1.Step{Name: "step1"},
			downstream: &v1alpha1.Step{
				Name: "step2",
				If:   stringPtr("condition"),
			},
			expectedCanUseP2P: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			canUse := CanUseDirectConnection(tt.upstream, tt.downstream)
			if canUse != tt.expectedCanUseP2P {
				t.Errorf("CanUseDirectConnection() = %v, want %v", canUse, tt.expectedCanUseP2P)
			}
		})
	}
}

func TestRoutingModeOverride(t *testing.T) {
	tests := []struct {
		name       string
		settings   map[string]any
		expectMode RoutingMode
		expectOK   bool
		expectRaw  string
	}{
		{
			name:     "no settings",
			settings: nil,
			expectOK: false,
		},
		{
			name:     "no routing block",
			settings: map[string]any{"other": map[string]any{"mode": "hub"}},
			expectOK: false,
		},
		{
			name:       "force hub",
			settings:   map[string]any{"routing": map[string]any{"mode": "hub"}},
			expectMode: RoutingModeHub,
			expectOK:   true,
			expectRaw:  "hub",
		},
		{
			name:       "force p2p",
			settings:   map[string]any{"routing": map[string]any{"mode": "p2p"}},
			expectMode: RoutingModeP2P,
			expectOK:   true,
			expectRaw:  "p2p",
		},
		{
			name:      "auto ignored",
			settings:  map[string]any{"routing": map[string]any{"mode": "auto"}},
			expectOK:  false,
			expectRaw: "auto",
		},
		{
			name:      "invalid ignored",
			settings:  map[string]any{"routing": map[string]any{"mode": "mesh"}},
			expectOK:  false,
			expectRaw: "mesh",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mode, ok, raw := RoutingModeOverride(tt.settings)
			if ok != tt.expectOK {
				t.Fatalf("ok = %v, want %v", ok, tt.expectOK)
			}
			if mode != tt.expectMode {
				t.Fatalf("mode = %v, want %v", mode, tt.expectMode)
			}
			if tt.expectRaw != "" && raw != tt.expectRaw {
				t.Fatalf("raw = %q, want %q", raw, tt.expectRaw)
			}
		})
	}
}
