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
	"context"
	"fmt"

	"sigs.k8s.io/controller-runtime/pkg/client"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	transportv1alpha1 "github.com/bubustack/bobrapet/api/transport/v1alpha1"
	"github.com/bubustack/bobrapet/api/v1alpha1"
)

// RoutingResolver resolves routing endpoints for engrams based on Story topology.
type RoutingResolver struct {
	client   client.Client
	story    *v1alpha1.Story
	storyRun *runsv1alpha1.StoryRun
	analyzer *TopologyAnalyzer
}

// NewRoutingResolver creates a resolver for the given Story and StoryRun.
func NewRoutingResolver(
	cli client.Client,
	story *v1alpha1.Story,
	storyRun *runsv1alpha1.StoryRun,
) *RoutingResolver {
	return &RoutingResolver{
		client:   cli,
		story:    story,
		storyRun: storyRun,
		analyzer: NewTopologyAnalyzer(story),
	}
}

// ResolveUpstreamEndpoint determines the endpoint this step should connect to.
// Returns:
//   - endpoint string: The gRPC endpoint to connect to (host:port)
//   - isHub bool: True if connecting to hub, false if P2P to downstream engram
//   - error: Any error during resolution
func (r *RoutingResolver) ResolveUpstreamEndpoint(
	ctx context.Context,
	stepName string,
	hubEndpoint string,
) (endpoint string, isHub bool, err error) {
	info, err := r.analyzer.AnalyzeStepRouting(stepName)
	if err != nil {
		return "", false, err
	}

	// If routing mode is hub or no downstream, use hub endpoint
	if info.RoutingMode == RoutingModeHub || len(info.DownstreamSteps) == 0 {
		return hubEndpoint, true, nil
	}

	// For P2P routing, resolve the downstream engram's service endpoint
	if len(info.DownstreamSteps) == 1 {
		downstreamStepName := info.DownstreamSteps[0]
		downstreamEndpoint, err := r.resolveEngramEndpoint(ctx, downstreamStepName)
		if err != nil {
			// If we can't resolve downstream, fall back to hub
			return hubEndpoint, true, fmt.Errorf("falling back to hub, failed to resolve downstream endpoint: %w", err)
		}
		return downstreamEndpoint, false, nil
	}

	return hubEndpoint, true, nil
}

// resolveEngramEndpoint finds the service endpoint for a step's engram.
func (r *RoutingResolver) resolveEngramEndpoint(ctx context.Context, stepName string) (string, error) {
	// Find the StepRun for this step
	stepRunName := fmt.Sprintf("%s-%s", r.storyRun.Name, stepName)
	var stepRun runsv1alpha1.StepRun
	if err := r.client.Get(ctx, client.ObjectKey{
		Namespace: r.storyRun.Namespace,
		Name:      stepRunName,
	}, &stepRun); err != nil {
		return "", fmt.Errorf("get steprun %s: %w", stepRunName, err)
	}

	// Get the TransportBinding to find the connector endpoint
	bindingName := fmt.Sprintf("%s-voice", stepRun.Name) // Assumes transport name
	var binding transportv1alpha1.TransportBinding
	if err := r.client.Get(ctx, client.ObjectKey{
		Namespace: r.storyRun.Namespace,
		Name:      bindingName,
	}, &binding); err != nil {
		return "", fmt.Errorf("get transport binding %s: %w", bindingName, err)
	}

	// Build the service endpoint: <engram-name>.<namespace>.svc.cluster.local:<port>
	// The engram name is the StepRun name (which becomes the pod name)
	serviceName := stepRun.Name
	namespace := stepRun.Namespace
	port := "50051" // Default gRPC connector port

	endpoint := fmt.Sprintf("%s.%s.svc.cluster.local:%s", serviceName, namespace, port)
	return endpoint, nil
}

// PopulateBindingRoutingStatus updates a TransportBinding's status with routing information.
func (r *RoutingResolver) PopulateBindingRoutingStatus(
	ctx context.Context,
	binding *transportv1alpha1.TransportBinding,
	hubEndpoint string,
) error {
	stepName := binding.Spec.StepName

	// Resolve upstream endpoint
	upstreamEndpoint, isHub, err := r.ResolveUpstreamEndpoint(ctx, stepName, hubEndpoint)
	if err != nil {
		// Log but don't fail - routing can fall back to hub
		return nil
	}

	// Set upstream host in status
	if !isHub {
		binding.Status.UpstreamHost = upstreamEndpoint
	}

	// Set downstream host (this engram's service endpoint for peers to connect to)
	// Only set if this step can accept P2P connections (doesn't need hub routing)
	info, err := r.analyzer.AnalyzeStepRouting(stepName)
	if err == nil && info.RoutingMode == RoutingModeP2P {
		// This step can accept direct connections from upstream
		serviceName := binding.Spec.EngramName
		namespace := binding.Namespace
		port := "50051" // Default connector port
		binding.Status.DownstreamHost = fmt.Sprintf("%s.%s.svc.cluster.local:%s", serviceName, namespace, port)
	}

	return nil
}
