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

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// +kubebuilder:validation:Enum=openai;azure;cloudy;mistral;llama;anthropic;google;xai;groq;cohere
type ModelVendor string

const (
	VendorOpenAI    ModelVendor = "openai"
	VendorAzure     ModelVendor = "azure"
	VendorCloudy    ModelVendor = "cloudy" // Assuming 'cloudy' is a placeholder
	VendorMistral   ModelVendor = "mistral"
	VendorLlama     ModelVendor = "llama" // Meta's Llama models
	VendorAnthropic ModelVendor = "anthropic"
	VendorGoogle    ModelVendor = "google"
	VendorXAI       ModelVendor = "xai"
	VendorGroq      ModelVendor = "groq"
	VendorCohere    ModelVendor = "cohere"
)

// +kubebuilder:validation:Enum=manual;minor;patch;major
type AutoUpgradePolicy string

const (
	UpgradePolicyManual AutoUpgradePolicy = "manual"
	UpgradePolicyMinor  AutoUpgradePolicy = "minor"
	UpgradePolicyPatch  AutoUpgradePolicy = "patch"
	UpgradePolicyMajor  AutoUpgradePolicy = "major"
)

// +kubebuilder:validation:Enum=text;audio;image;video;code;tool_calls;tool_results
type ModalityType string

const (
	ModalityText        ModalityType = "text"
	ModalityAudio       ModalityType = "audio"
	ModalityImage       ModalityType = "image"
	ModalityVideo       ModalityType = "video"
	ModalityCode        ModalityType = "code"
	ModalityToolCalls   ModalityType = "tool_calls"   // E.g., OpenAI tool calls
	ModalityToolResults ModalityType = "tool_results" // E.g., OpenAI tool results
)

// +kubebuilder:validation:Enum=completions;responses;realtime;assistants;batch;fine-tuning;embeddings;image-generation;image-edit;speech-generation;transcription;translation;moderation;completions-legacy
type EndpointType string

const (
	EndpointCompletions       EndpointType = "completions"        // Legacy text completion
	EndpointResponses         EndpointType = "responses"          // OpenAI responses
	EndpointRealtime          EndpointType = "realtime"           // OpenAI realtime
	EndpointAssistant         EndpointType = "assistants"         // OpenAI assistant
	EndpointBatch             EndpointType = "batch"              // OpenAI batch
	EndpointFineTuning        EndpointType = "fine-tuning"        // OpenAI fine-tuning
	EndpointEmbeddings        EndpointType = "embeddings"         // Text embeddings
	EndpointImageGeneration   EndpointType = "image-generation"   // Text-to-image
	EndpointImageEdit         EndpointType = "image-edit"         // Image editing (inpainting/outpainting)
	EndpointSpeechGeneration  EndpointType = "speech-generation"  // Text-to-speech
	EndpointTranscription     EndpointType = "transcription"      // Speech-to-text
	EndpointTranslation       EndpointType = "translation"        // Speech-to-speech/text translation
	EndpointModeration        EndpointType = "moderation"         // Content moderation
	EndpointCompletionsLegacy EndpointType = "completions-legacy" // Legacy text completion
)

// +kubebuilder:validation:Enum=streaming;function-calling;structured-outputs;fine-tuning;distillation;predicted-outputs
type FeatureType string

const (
	FeatureStreaming        FeatureType = "streaming"          // Response streaming
	FeatureFunctionCalling  FeatureType = "function-calling"   // OpenAI legacy function calling
	FeatureStructuredOutput FeatureType = "structured-outputs" // OpenAI structured output
	FeatureFineTuning       FeatureType = "fine-tuning"        // Model fine-tuning capability
	FeatureDistillation     FeatureType = "distillation"       // Model distillation capability
	FeaturePredictedOutput  FeatureType = "predicted-outputs"  // OpenAI predicted output
)

// +kubebuilder:validation:Enum=low;medium;high;unknown
type SafetyLevel string

const (
	SafetyLevelLow     SafetyLevel = "low"
	SafetyLevelMedium  SafetyLevel = "medium"
	SafetyLevelHigh    SafetyLevel = "high"
	SafetyLevelUnknown SafetyLevel = "unknown" // Default or unspecified
)

// +kubebuilder:validation:Enum=token;image;embedding;second;minute;request;character;million_tokens
type BillingUnit string

const (
	BillingUnitToken         BillingUnit = "token"          // Per token
	BillingUnitImage         BillingUnit = "image"          // Per image
	BillingUnitEmbedding     BillingUnit = "embedding"      // Per embedding generated (often tied to tokens)
	BillingUnitSecond        BillingUnit = "second"         // Per second of audio/video
	BillingUnitMinute        BillingUnit = "minute"         // Per minute of audio/video
	BillingUnitRequest       BillingUnit = "request"        // Per API request
	BillingUnitCharacter     BillingUnit = "character"      // Per character
	BillingUnitMillionTokens BillingUnit = "million_tokens" // Per million tokens (common pricing unit)
)

// Characteristics defines qualitative performance characteristics of the model.
type Characteristics struct {
	// Qualitative measure of the model's reasoning and knowledge capability (e.g., on a scale of 1-10).
	// +optional
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=10
	Intelligence *int `json:"intelligence,omitempty"`

	// Qualitative measure of the model's response generation speed (e.g., on a scale of 1-10).
	// +optional
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=10
	Speed *int `json:"speed,omitempty"`

	// Qualitative measure of the model's reasoning ability (e.g., on a scale of 1-10).
	// +optional
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=10
	Reasoning *int `json:"reasoning,omitempty"`

	// Qualitative measure of the model's performance (e.g., on a scale of 1-10).
	// +optional
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=10
	Performance *int `json:"performance,omitempty"`

	// Qualitative measure of the model's tendency to generate factually incorrect information (lower is better, e.g., 1-10).
	// +optional
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=10
	HallucinationRate *int `json:"hallucinationRate,omitempty"`

	// Qualitative measure of the model's consistency in responses to similar prompts (e.g., on a scale of 1-10).
	// +optional
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=10
	SelfConsistency *int `json:"selfConsistency,omitempty"`
}

// Versioning defines the version management details for the model.
type Versioning struct {
	// List of all known versions for this model identifier (e.g., ["gpt-4-1106-preview", "gpt-4-0613"]).
	// +optional
	// +listType=set
	Versions []string `json:"versions,omitempty"`

	// Subset of versions that are currently active and recommended for use.
	// +optional
	// +listType=set
	SupportedVersions []string `json:"supportedVersions,omitempty"`

	// Subset of versions that are deprecated and should not be used.
	// +optional
	// +listType=set
	DeprecatedVersions []string `json:"deprecatedVersions,omitempty"`

	// The date when the primary/default version (or the version specified in `spec.model`) was released (RFC3339 format).
	// +optional
	ReleaseDate *string `json:"releaseDate,omitempty"`

	// The date when the primary/default version (or the version specified in `spec.model`) is scheduled for deprecation (RFC3339 format).
	// +optional
	DeprecationDate *string `json:"deprecationDate,omitempty"`

	// Policy for automatically upgrading clients using this model reference.
	// +optional
	// +kubebuilder:default=manual
	AutoUpgradePolicy AutoUpgradePolicy `json:"autoUpgradePolicy,omitempty"`
}

// Limits defines the operational constraints of the model.
type Limits struct {
	// Maximum number of tokens allowed in the input context window.
	// +optional
	// +kubebuilder:validation:Minimum=0
	ContextWindow *int `json:"contextWindow,omitempty"`

	// Maximum number of tokens the model can generate in a single response.
	// +optional
	// +kubebuilder:validation:Minimum=0
	MaxOutputTokens *int `json:"maxOutputTokens,omitempty"`

	// The date up to which the model's training data is current (e.g., "2023-04", "April 2023"). Free-form string.
	// +optional
	KnowledgeCutoff *string `json:"knowledgeCutoff,omitempty"`
}

// Modalities defines the types of input and output the model supports.
type Modalities struct {
	// List of supported input types (e.g., text, image, audio).
	// +optional
	// +listType=set
	Input []ModalityType `json:"input,omitempty"`

	// List of supported output types (e.g., text, image, audio).
	// +optional
	// +listType=set
	Output []ModalityType `json:"output,omitempty"`
}

// Latency defines latency characteristics in milliseconds.
type Latency struct {
	// 50th percentile latency (median) in milliseconds.
	// +optional
	// +kubebuilder:validation:Minimum=0
	P50 *int `json:"p50,omitempty"`

	// 90th percentile latency in milliseconds.
	// +optional
	// +kubebuilder:validation:Minimum=0
	P90 *int `json:"p90,omitempty"`

	// 99th percentile latency in milliseconds.
	// +optional
	// +kubebuilder:validation:Minimum=0
	P99 *int `json:"p99,omitempty"`
}

// PriceDetail holds the actual input/output cost values.
type PriceDetail struct {
	// Cost per unit for input processing.
	// +optional
	// +kubebuilder:validation:Type=number
	// +kubebuilder:validation:Format=double
	Input *float64 `json:"input,omitempty"`

	// Cost per unit for output generation.
	// +optional
	// +kubebuilder:validation:Type=number
	// +kubebuilder:validation:Format=double
	Output *float64 `json:"output,omitempty"`

	// Cost per unit for cached input processing, if applicable.
	// +optional
	// +kubebuilder:validation:Type=number
	// +kubebuilder:validation:Format=double
	CachedInput *float64 `json:"cachedInput,omitempty"`
}

// RateByType maps usage types (like "text", "image") to their specific pricing details.
// Example keys: "text", "image", "audio", "embedding_input", "multimodal".
// +kubebuilder:validation:Type=object
// +kubebuilder:pruning:PreserveUnknownFields
// +kubebuilder:validation:XPreserveUnknownFields
type RateByType map[string]*PriceDetail

// Rates defines the different pricing structures (standard, batch, etc.),
// each broken down by the type of usage (text, image, etc.).
type Rates struct {
	// Standard rate pricing, broken down by usage type (e.g., text, image).
	// +optional
	Standard RateByType `json:"standard,omitempty"`

	// Pricing for batch processing, if different, broken down by usage type.
	// +optional
	Batch RateByType `json:"batch,omitempty"`

	// Pricing for long context processing, if different, broken down by usage type.
	// +optional
	LongContext RateByType `json:"longContext,omitempty"`

	// Add other potential rate types here if needed (e.g., fineTuning, specific features)
}

// RateLimits defines the usage limits associated with a billing plan or overall model usage.
type RateLimits struct {
	// Requests Per Minute limit.
	// +optional
	// +kubebuilder:validation:Minimum=0
	RPM *int `json:"rpm,omitempty"`

	// Tokens Per Minute limit (sum of prompt and completion tokens).
	// +optional
	// +kubebuilder:validation:Minimum=0
	TPM *int `json:"tpm,omitempty"`

	// Maximum queue depth or concurrent request limit. Meaning can vary by provider.
	// +optional
	// +kubebuilder:validation:Minimum=0
	Queue *int `json:"queue,omitempty"`

	// Burst capacity (requests or tokens) allowed beyond the sustained rate limit.
	// +optional
	// +kubebuilder:validation:Minimum=0
	Burst *int `json:"burst,omitempty"`

	// Time window over which rate limits are measured, in seconds (e.g., 60 for per-minute limits).
	// +optional
	// +kubebuilder:validation:Minimum=1
	WindowSeconds *int `json:"windowSeconds,omitempty"`

	// Images Per Minute limit (for image generation/editing models).
	// +optional
	// +kubebuilder:validation:Minimum=0
	IPM *int `json:"ipm,omitempty"`

	// Requests Per Day limit.
	// +optional
	// +kubebuilder:validation:Minimum=0
	RPD *int `json:"rpd,omitempty"`

	// Rate limits specifically for long context requests.
	// +optional
	LongContext *LongContextRateLimitDetail `json:"longContext,omitempty"`
}

// LongContextRateLimitDetail defines specific rate limits for long context operations.
// It mirrors some fields from RateLimits but applies specifically to long context usage.
type LongContextRateLimitDetail struct {
	// Requests Per Minute limit for long context.
	// +optional
	// +kubebuilder:validation:Minimum=0
	RPM *int `json:"rpm,omitempty"`

	// Tokens Per Minute limit for long context.
	// +optional
	// +kubebuilder:validation:Minimum=0
	TPM *int `json:"tpm,omitempty"`

	// Maximum queue depth or concurrent request limit for long context.
	// +optional
	// +kubebuilder:validation:Minimum=0
	Queue *int `json:"queue,omitempty"`

	// Burst capacity for long context.
	// +optional
	// +kubebuilder:validation:Minimum=0
	Burst *int `json:"burst,omitempty"`

	// Time window for long context rate limits in seconds.
	// +optional
	// +kubebuilder:validation:Minimum=1
	WindowSeconds *int `json:"windowSeconds,omitempty"`
}

// BillingPlan defines a specific pricing plan for the model.
type BillingPlan struct {
	// Unique name identifying the billing plan within this Model resource (e.g., "pay-as-you-go", "standard-tier", "enterprise-commit"). Used as the key in the list map.
	// +kubebuilder:validation:Required
	Name string `json:"name"`

	// Human-readable description of the plan.
	// +optional
	Description *string `json:"description,omitempty"`

	// Specifies which modalities or operations this specific plan applies to (e.g., only applies to 'text' input/output, or only to the 'embeddings' endpoint).
	// If empty or omitted, the plan is assumed to apply generally based on the defined Unit and Rates.
	// +optional
	// +listType=set
	ApplicableTo []ModalityType `json:"applicableTo,omitempty"`

	// The primary unit of measurement for billing under this plan (e.g., token, million_tokens, image, request).
	// +kubebuilder:validation:Required
	Unit BillingUnit `json:"unit"`

	// Detailed pricing rates for different scenarios (standard, batch, long context) per the defined Unit.
	// +optional
	Rates *Rates `json:"rates,omitempty"`

	// Usage rate limits specifically associated with this billing plan.
	// +optional
	RateLimits *RateLimits `json:"rateLimits,omitempty"`
}

// Billing defines the overall billing and pricing structure for the model.
type Billing struct {
	// ISO 4217 currency code for all monetary values in this section (e.g., "USD", "EUR").
	// +optional
	// +kubebuilder:default="USD"
	// +kubebuilder:validation:Pattern=`^[A-Z]{3}$`
	Currency string `json:"currency,omitempty"`

	// URL linking to the vendor's official pricing documentation for this model or service.
	// +optional
	// +kubebuilder:validation:Format=uri
	DocsURL *string `json:"docsURL,omitempty"`

	// List of available billing plans. Use the 'name' field within each plan as the key.
	// +optional
	// +listType=map
	// +listMapKey=name
	Plans []BillingPlan `json:"plans,omitempty"`
}

// ModelSpec defines the desired state of Model. It contains metadata and characteristics of an AI/ML model.
type ModelSpec struct {
	// Provider is an optional internal system identifier used for routing API calls or selecting implementation logic.
	// +kubebuilder:validation:Required
	Provider *string `json:"provider"`

	// Vendor identifies the provider or source of the model (e.g., openai, azure, google, meta).
	// +kubebuilder:validation:Required
	Vendor ModelVendor `json:"vendor"`

	// Model is the specific identifier string used by the vendor for this model (e.g., "gpt-4-turbo", "claude-3-opus-20240229", "mistral-large-latest").
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	Model string `json:"model"`

	// ShortDescription provides a brief, one-line summary of the model.
	// +optional
	ShortDescription *string `json:"shortDescription,omitempty"`

	// Description provides a more detailed explanation of the model's capabilities, strengths, weaknesses, and intended use cases.
	// +optional
	Description *string `json:"description,omitempty"`

	// URL links to the vendor's official documentation or information page specifically for this model version.
	// +optional
	// +kubebuilder:validation:Format=uri
	URL *string `json:"url,omitempty"`

	// Characteristics includes qualitative metrics like intelligence, speed, etc.
	// +optional
	Characteristics *Characteristics `json:"characteristics,omitempty"`

	// Versioning information for the model, including lists of supported/deprecated versions and lifecycle dates.
	// +optional
	Versioning *Versioning `json:"versioning,omitempty"`

	// Limits defines operational constraints like context window and max output tokens.
	// +optional
	Limits *Limits `json:"limits,omitempty"`

	// Modalities specifies the types of input and output the model supports (e.g., text, image).
	// +optional
	Modalities *Modalities `json:"modalities,omitempty"`

	// List of API endpoints or specific functionalities supported by this model (e.g., chat, embeddings, image generation).
	// +optional
	// +listType=set
	Endpoints []EndpointType `json:"endpoints,omitempty"`

	// LatencyMs provides latency characteristics in milliseconds (e.g., p50, p90, p99). Values are indicative and may vary.
	// +optional
	LatencyMs *Latency `json:"latencyMs,omitempty"`

	// Estimated or benchmarked throughput in Requests Per Second (RPS). Value is indicative.
	// +optional
	// +kubebuilder:validation:Type=number
	// +kubebuilder:validation:Format=double
	// +kubebuilder:validation:Minimum=0
	ThroughputRPS *float64 `json:"throughputRPS,omitempty"`

	// Indicates if the model's API natively supports batching multiple requests into a single call.
	// +optional
	BatchSupport *bool `json:"batchSupport,omitempty"`

	// Maximum number of items (e.g., prompts, documents) allowed in a single batch request, if batching is supported.
	// +optional
	// +kubebuilder:validation:Minimum=1
	MaxBatchSize *int `json:"maxBatchSize,omitempty"`

	// List of additional features or capabilities supported by the model (e.g., streaming, tool-calling, JSON mode).
	// +optional
	// +listType=set
	Features []FeatureType `json:"features,omitempty"`

	// General level of safety filtering or content moderation applied by the model or platform (low, medium, high).
	// +optional
	// +kubebuilder:default=unknown
	SafetyLevel SafetyLevel `json:"safetyLevel,omitempty"`

	// List of compliance certifications the model or its hosting platform adheres to (e.g., "SOC2", "HIPAA", "ISO27001").
	// +optional
	// +listType=set
	ComplianceCertifications []string `json:"complianceCertifications,omitempty"`

	// Free-form string describing the vendor's policy regarding the retention of data sent to and generated by the model.
	// +optional
	DataRetentionPolicy *string `json:"dataRetentionPolicy,omitempty"`

	// Billing defines the pricing structure, plans, and associated rate limits for using the model.
	// +optional
	Billing *Billing `json:"billing,omitempty"`

	// For image generation/processing models: List of supported output resolutions (e.g., "1024x1024", "1792x1024", "512x512").
	// +optional
	// +listType=set
	SupportedResolutions []string `json:"supportedResolutions,omitempty"`

	// For image/audio models: List of supported file formats for input or output (e.g., "png", "jpeg", "webp", "wav", "mp3", "flac").
	// +optional
	// +listType=set
	SupportedFormats []string `json:"supportedFormats,omitempty"`

	// For audio models: List of supported audio sampling rates in Hertz (e.g., 16000, 24000, 44100, 48000).
	// +optional
	// +listType=set
	SamplingRates []int `json:"samplingRates,omitempty"`
}

// ModelStatus defines the observed state of Model.
type ModelStatus struct {
	// Represents the observations of a Model's current state.
	// Model controllers might use this to indicate validation status, effective settings, or other runtime information.
	// Important: Run "make" to regenerate code after modifying this file

	// Conditions represent the latest available observations of an object's state.
	// +optional
	// +listType=map
	// +listMapKey=type
	// +patchStrategy=merge
	// +patchMergeKey=type
	// +operator-sdk:csv:customresourcedefinitions:type=status
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type"`
}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status
//+kubebuilder:resource:path=models,scope=Cluster,shortName=mod;mdl,categories=bubu;ai
//+kubebuilder:printcolumn:name="Vendor",type="string",JSONPath=".spec.vendor",description="The vendor/provider of the model"
//+kubebuilder:printcolumn:name="Model",type="string",JSONPath=".spec.model",description="The specific model identifier"
//+kubebuilder:printcolumn:name="Context Window",type="integer",JSONPath=".spec.limits.contextWindow",description="Max input tokens",priority=1
//+kubebuilder:printcolumn:name="Features",type="string",JSONPath=".spec.features",description="Supported features",priority=1
//+kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"

// Model is the Schema for the models API. It represents AI/ML model metadata within the cluster.
type Model struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ModelSpec   `json:"spec,omitempty"`
	Status ModelStatus `json:"status,omitempty"`
}

//+kubebuilder:object:root=true

// ModelList contains a list of Model
type ModelList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Model `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Model{}, &ModelList{})
}
