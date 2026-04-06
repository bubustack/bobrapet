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
	"fmt"
	"slices"
	"strings"

	"github.com/bubustack/bobrapet/api/v1alpha1"
)

// RoutingMode describes how data flows between steps.
type RoutingMode string

const (
	// RoutingModeP2P indicates direct peer-to-peer connection between engrams
	RoutingModeP2P RoutingMode = "p2p"
	// RoutingModeHub indicates hub-mediated routing for template evaluation
	RoutingModeHub RoutingMode = "hub"
)

// StepRoutingInfo contains routing information for a step.
type StepRoutingInfo struct {
	StepName         string
	DownstreamSteps  []string
	RoutingMode      RoutingMode
	UpstreamEndpoint string // Empty means use hub, otherwise the direct peer endpoint
}

// TopologyAnalyzer analyzes Story DAG to determine routing topology.
type TopologyAnalyzer struct {
	story *v1alpha1.Story
	steps map[string]*v1alpha1.Step
}

// NewTopologyAnalyzer creates a new analyzer for the given story.
func NewTopologyAnalyzer(story *v1alpha1.Story) *TopologyAnalyzer {
	steps := make(map[string]*v1alpha1.Step)
	for i := range story.Spec.Steps {
		step := &story.Spec.Steps[i]
		steps[step.Name] = step
	}
	return &TopologyAnalyzer{
		story: story,
		steps: steps,
	}
}

// AnalyzeStepRouting determines the routing mode and upstream endpoint for a step.
// Returns the routing info for the step.
func (a *TopologyAnalyzer) AnalyzeStepRouting(stepName string) (*StepRoutingInfo, error) {
	currentStep, ok := a.steps[stepName]
	if !ok {
		return nil, fmt.Errorf("step %s not found in story", stepName)
	}

	info := &StepRoutingInfo{
		StepName:        stepName,
		DownstreamSteps: a.getDownstreamSteps(stepName),
	}

	// CRITICAL: If the CURRENT step has runtime config, it MUST use hub routing
	// to receive template-evaluated inputs from upstream, regardless of downstream needs.
	// Example: synthesize has runtime.text='{{ steps.respond.text }}' which requires
	// template evaluation by the hub, so synthesize must connect to hub even though its
	// downstream (playback) doesn't need hub routing.
	if StepNeedsHubRouting(currentStep) {
		info.RoutingMode = RoutingModeHub
		return info, nil
	}

	// Determine if this step can use P2P routing to downstream
	// P2P is only possible when:
	// 1. Current step does NOT have runtime config (checked above)
	// 2. There is exactly one downstream step
	// 3. The downstream step does not need hub routing (no runtime config)
	// 4. If the current step has upstreams, ALL upstream edges to this step are
	//    already direct P2P. Otherwise this step must stay on hub so hub-routed
	//    upstreams can deliver packets to it.
	if len(info.DownstreamSteps) == 1 {
		downstreamName := info.DownstreamSteps[0]
		downstreamStep, ok := a.steps[downstreamName]
		if ok && !StepNeedsHubRouting(downstreamStep) && a.hasOnlyP2PUpstreams(stepName, currentStep.Needs) {
			info.RoutingMode = RoutingModeP2P
			// UpstreamEndpoint will be populated later with the actual service endpoint
			// when we have access to the engram name/namespace
		} else {
			info.RoutingMode = RoutingModeHub
		}
	} else {
		// Multiple downstream or no downstream - must use hub
		info.RoutingMode = RoutingModeHub
	}

	return info, nil
}

func (a *TopologyAnalyzer) hasOnlyP2PUpstreams(stepName string, upstreamSteps []string) bool {
	if len(upstreamSteps) == 0 {
		// Source steps are allowed to choose P2P based on downstream topology.
		return true
	}
	for _, upstreamName := range upstreamSteps {
		info, err := a.AnalyzeStepRouting(upstreamName)
		if err != nil {
			return false
		}
		if info.RoutingMode != RoutingModeP2P {
			return false
		}
		if len(info.DownstreamSteps) != 1 || info.DownstreamSteps[0] != stepName {
			return false
		}
	}
	return true
}

// getDownstreamSteps finds all steps that depend on the given step.
func (a *TopologyAnalyzer) getDownstreamSteps(stepName string) []string {
	var downstream []string
	for _, step := range a.story.Spec.Steps {
		if slices.Contains(step.Needs, stepName) {
			downstream = append(downstream, step.Name)
		}
	}
	return downstream
}

// CanUseP2PRouting returns true if the connection from upstream to downstream can use P2P.
func (a *TopologyAnalyzer) CanUseP2PRouting(upstreamName, downstreamName string) bool {
	upstream, okUp := a.steps[upstreamName]
	downstream, okDown := a.steps[downstreamName]
	if !okUp || !okDown {
		return false
	}

	// Upstream must have exactly one downstream
	downstreamSteps := a.getDownstreamSteps(upstreamName)
	if len(downstreamSteps) != 1 || downstreamSteps[0] != downstreamName {
		return false
	}

	// Downstream must not need hub routing
	return CanUseDirectConnection(upstream, downstream)
}

// GetRoutingTopology returns routing information for all steps in the story.
func (a *TopologyAnalyzer) GetRoutingTopology() (map[string]*StepRoutingInfo, error) {
	topology := make(map[string]*StepRoutingInfo)
	for stepName := range a.steps {
		info, err := a.AnalyzeStepRouting(stepName)
		if err != nil {
			return nil, err
		}
		topology[stepName] = info
	}
	return topology, nil
}

// HasP2PUpstream reports whether any upstream step can connect directly to the
// given step using P2P routing. This is used to decide if a step should
// advertise a downstream host for direct connections.
func (a *TopologyAnalyzer) HasP2PUpstream(stepName string) bool {
	if a == nil {
		return false
	}
	for upstreamName := range a.steps {
		if upstreamName == stepName {
			continue
		}
		info, err := a.AnalyzeStepRouting(upstreamName)
		if err != nil {
			continue
		}
		if info.RoutingMode != RoutingModeP2P {
			continue
		}
		if len(info.DownstreamSteps) == 1 && info.DownstreamSteps[0] == stepName {
			return true
		}
	}
	return false
}

// RoutingModeOverride inspects transport settings for an explicit routing override.
// Returns the override mode, whether it is valid, and the raw string when present.
func RoutingModeOverride(transportSettings map[string]any) (RoutingMode, bool, string) {
	if transportSettings == nil {
		return "", false, ""
	}
	routing, ok := transportSettings["routing"].(map[string]any)
	if !ok {
		return "", false, ""
	}
	raw, ok := routing["mode"].(string)
	if !ok {
		return "", false, ""
	}
	normalized := strings.ToLower(strings.TrimSpace(raw))
	switch normalized {
	case "", "auto":
		return "", false, normalized
	case string(RoutingModeHub):
		return RoutingModeHub, true, normalized
	case string(RoutingModeP2P):
		return RoutingModeP2P, true, normalized
	default:
		return "", false, normalized
	}
}

// ShouldForceHubRouting checks if transport settings explicitly disable P2P routing.
// Transport settings can include: { "routing": { "mode": "hub" } }
// Returns true if routing should be forced to hub, false otherwise (allowing P2P).
func ShouldForceHubRouting(transportSettings map[string]any) bool {
	mode, ok, _ := RoutingModeOverride(transportSettings)
	return ok && mode == RoutingModeHub
}
