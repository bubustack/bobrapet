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

// Package transport provides utilities for gRPC transport management.
//
// This package contains helpers for Transport and TransportBinding resources,
// including capability aggregation, codec management, security configuration,
// and engram annotation patching.
//
// # Transport Capabilities
//
// Aggregate capabilities from multiple transports:
//
//	caps := transport.AggregateCapabilities(transports)
//
// Check for specific capabilities:
//
//	if transport.HasAudioCapability(caps, "opus") { ... }
//	if transport.HasVideoCapability(caps, "h264") { ... }
//
// # Codec Management
//
// Parse and validate codec specifications:
//
//	codec, err := transport.ParseAudioCodec(spec)
//	err := transport.ValidateCodecs(codecs)
//
// # Security Configuration
//
// Build TLS configuration for transports:
//
//	tlsConfig := transport.BuildTLSConfig(binding)
//
// # Engram Annotation Patching
//
// The [EngramAnnotationPatcher] patches transport annotations on Engrams:
//
//	patcher := transport.NewEngramAnnotationPatcher(client, logger, recorder)
//	err := patcher.PatchBindingRef(ctx, namespace, engramName, bindingRef)
//	err := patcher.PatchReadyStatus(ctx, namespace, engramName, ready, message)
//
// # Subpackages
//
// binding: TransportBinding naming and status utilities
//
//	name := binding.Name(storyRunName, stepID)
//	binding.SetReady(binding, true, "Transport ready")
//
// bindinginfo: Binding information aggregation
//
// capabilities: Capability aggregation and helpers
//
// validation: Transport spec and codec validation
//
//	errs := validation.ValidateTransport(transport)
package transport
