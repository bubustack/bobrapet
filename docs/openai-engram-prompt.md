# OpenAI Integration Engram Generation Prompt (Go)

## Context: Bobrapet OpenAI Engram

You are creating an **OpenAI Integration Engram** for the Bobrapet workflow system. This engram will interact with OpenAI's API for chat completions, text generation, and AI-powered tasks with proper error handling, rate limiting, and secret management.

### Architecture Requirements:
- **Language**: Go (latest stable version)
- **Container**: Alpine-based for minimal size
- **Input**: JSON via `ENGRAM_INPUT` environment variable
- **Output**: JSON via stdout
- **Secrets**: Resolve `${{ secrets.openai-api-key }}` expressions
- **Logging**: Structured JSON to stderr

## EngramTemplate Specification

First, create the **EngramTemplate YAML** that defines this engram:

```yaml
apiVersion: catalog.bubu.sh/v1alpha1
kind: EngramTemplate
metadata:
  name: openai-client
spec:
  version: "1.0.0"
  description: "OpenAI API client for chat completions, text generation, and AI workflows"
  image: "your-registry/openai-client-engram:1.0.0"
  
  supportedModes:
    - "job"
    - "deployment"
  
  uiHints:
    category: "ai"
    displayName: "OpenAI Client"
    icon: "brain"
    color: "#00A67E"
    tags: ["openai", "ai", "gpt", "chat", "completion"]
  
  resources:
    cpu: "100m"
    memory: "256Mi"
  
  security:
    requiredSecrets: ["openai-api-key"]
    networkAccess: ["external"]
  
  defaults:
    timeout: "60s"
    retry:
      maxRetries: 3
      backoff: "exponential"
      baseDelay: "2s"
      maxDelay: "30s"
    resources:
      requests:
        cpu: "50m"
        memory: "128Mi"
      limits:
        cpu: "500m"
        memory: "512Mi"
    config:
      model: "gpt-4"
      max_tokens: 1000
      temperature: 0.7
      stream: false
  
  inputSchema:
    type: object
    required: ["api_key"]
    properties:
      api_key:
        type: string
        description: "OpenAI API key (use secrets: ${{ secrets.openai-api-key }})"
      base_url:
        type: string
        format: uri
        default: "https://api.openai.com/v1"
        description: "OpenAI API base URL (for custom endpoints)"
      model:
        type: string
        default: "gpt-4"
        description: "OpenAI model to use"
        enum: ["gpt-4", "gpt-4-turbo", "gpt-3.5-turbo", "gpt-4o", "gpt-4o-mini"]
      
      # Chat Completion Fields
      messages:
        type: array
        description: "Chat messages for completion"
        items:
          type: object
          required: ["role", "content"]
          properties:
            role:
              type: string
              enum: ["system", "user", "assistant", "tool"]
            content:
              type: string
              description: "Message content"
            name:
              type: string
              description: "Name of the user (optional)"
            tool_call_id:
              type: string
              description: "Tool call ID for tool messages"
      
      # Generation Parameters
      max_tokens:
        type: integer
        minimum: 1
        maximum: 8192
        default: 1000
        description: "Maximum tokens to generate"
      temperature:
        type: number
        minimum: 0
        maximum: 2
        default: 0.7
        description: "Sampling temperature (0-2)"
      top_p:
        type: number
        minimum: 0
        maximum: 1
        default: 1
        description: "Nucleus sampling parameter"
      frequency_penalty:
        type: number
        minimum: -2
        maximum: 2
        default: 0
        description: "Frequency penalty (-2 to 2)"
      presence_penalty:
        type: number
        minimum: -2
        maximum: 2
        default: 0
        description: "Presence penalty (-2 to 2)"
      
      # Control Parameters
      stream:
        type: boolean
        default: false
        description: "Whether to stream responses"
      stop:
        oneOf:
          - type: string
          - type: array
            items:
              type: string
        description: "Stop sequences"
      seed:
        type: integer
        description: "Random seed for deterministic outputs"
      
      # Function Calling
      tools:
        type: array
        description: "Available tools/functions"
        items:
          type: object
          properties:
            type:
              type: string
              enum: ["function"]
            function:
              type: object
              required: ["name", "description"]
              properties:
                name:
                  type: string
                description:
                  type: string
                parameters:
                  type: object
      tool_choice:
        oneOf:
          - type: string
            enum: ["none", "auto"]
          - type: object
        description: "Tool choice strategy"
      
      # Additional Options
      timeout:
        type: string
        pattern: "^[0-9]+(s|m|h)$"
        default: "60s"
        description: "Request timeout"
      retries:
        type: integer
        minimum: 0
        maximum: 5
        default: 3
        description: "Number of retry attempts"
      user:
        type: string
        description: "User identifier for abuse monitoring"
  
  outputSchema:
    type: object
    required: ["success", "model"]
    properties:
      success:
        type: boolean
        description: "Whether the request was successful"
      model:
        type: string
        description: "Model used for generation"
      
      # Response Content
      response:
        type: string
        description: "Generated text response"
      choices:
        type: array
        description: "All completion choices"
        items:
          type: object
          properties:
            index:
              type: integer
            message:
              type: object
              properties:
                role:
                  type: string
                content:
                  type: string
                tool_calls:
                  type: array
            finish_reason:
              type: string
              enum: ["stop", "length", "function_call", "tool_calls", "content_filter"]
      
      # Usage Information
      usage:
        type: object
        description: "Token usage statistics"
        properties:
          prompt_tokens:
            type: integer
          completion_tokens:
            type: integer
          total_tokens:
            type: integer
          prompt_tokens_details:
            type: object
          completion_tokens_details:
            type: object
      
      # Metadata
      created:
        type: integer
        description: "Unix timestamp of creation"
      system_fingerprint:
        type: string
        description: "System fingerprint"
      duration_ms:
        type: integer
        description: "Request duration in milliseconds"
      retries_used:
        type: integer
        description: "Number of retries actually used"
      
      # Error Information (if applicable)
      error:
        type: object
        description: "Error details if request failed"
        properties:
          type:
            type: string
          message:
            type: string
          code:
            type: string
          param:
            type: string
  
  examples:
    - name: "simple-chat"
      description: "Simple chat completion"
      input:
        api_key: "${{ secrets.openai-api-key }}"
        model: "gpt-4"
        messages:
          - role: "user"
            content: "Hello, how are you?"
        max_tokens: 100
      output:
        success: true
        model: "gpt-4"
        response: "I'm doing well, thank you for asking! How can I help you today?"
        usage:
          prompt_tokens: 12
          completion_tokens: 18
          total_tokens: 30
    
    - name: "system-prompt"
      description: "Chat with system prompt"
      input:
        api_key: "${{ secrets.openai-api-key }}"
        model: "gpt-4"
        messages:
          - role: "system"
            content: "You are a helpful assistant that speaks like a pirate."
          - role: "user"
            content: "What's the weather like?"
        temperature: 0.9
      output:
        success: true
        model: "gpt-4"
        response: "Ahoy matey! I can't be checkin' the weather for ye..."
    
    - name: "function-calling"
      description: "Function calling example"
      input:
        api_key: "${{ secrets.openai-api-key }}"
        model: "gpt-4"
        messages:
          - role: "user"
            content: "What's the weather in San Francisco?"
        tools:
          - type: "function"
            function:
              name: "get_weather"
              description: "Get current weather for a location"
              parameters:
                type: "object"
                properties:
                  location:
                    type: "string"
                    description: "City name"
                required: ["location"]
      output:
        success: true
        model: "gpt-4"
        choices:
          - message:
              role: "assistant"
              tool_calls:
                - function:
                    name: "get_weather"
                    arguments: '{"location": "San Francisco"}'
```

## Go Implementation Requirements

### Project Structure:
```
openai-client-engram/
├── Dockerfile
├── go.mod
├── go.sum
├── main.go
├── internal/
│   ├── client/
│   │   ├── openai_client.go
│   │   ├── completion.go
│   │   ├── streaming.go
│   │   └── retry.go
│   ├── auth/
│   │   └── auth.go
│   ├── secrets/
│   │   └── resolver.go
│   ├── ratelimit/
│   │   └── limiter.go
│   └── logging/
│       └── logger.go
├── pkg/
│   ├── types/
│   │   ├── input.go
│   │   ├── output.go
│   │   └── openai_types.go
│   └── validation/
│       └── validator.go
├── examples/
│   ├── chat-examples.json
│   ├── function-calling.json
│   └── streaming-examples.json
└── README.md
```

### Core Features to Implement:

#### 1. Input/Output Types (pkg/types/)
```go
type OpenAIRequest struct {
    APIKey          string         `json:"api_key" validate:"required"`
    BaseURL         string         `json:"base_url,omitempty"`
    Model           string         `json:"model" validate:"required"`
    Messages        []ChatMessage  `json:"messages,omitempty"`
    MaxTokens       int            `json:"max_tokens,omitempty"`
    Temperature     float32        `json:"temperature,omitempty"`
    TopP            float32        `json:"top_p,omitempty"`
    FrequencyPenalty float32       `json:"frequency_penalty,omitempty"`
    PresencePenalty float32        `json:"presence_penalty,omitempty"`
    Stream          bool           `json:"stream,omitempty"`
    Stop            interface{}    `json:"stop,omitempty"`
    Seed            *int           `json:"seed,omitempty"`
    Tools           []Tool         `json:"tools,omitempty"`
    ToolChoice      interface{}    `json:"tool_choice,omitempty"`
    Timeout         string         `json:"timeout,omitempty"`
    Retries         int            `json:"retries,omitempty"`
    User            string         `json:"user,omitempty"`
}

type ChatMessage struct {
    Role       string      `json:"role" validate:"required,oneof=system user assistant tool"`
    Content    string      `json:"content"`
    Name       string      `json:"name,omitempty"`
    ToolCallID string      `json:"tool_call_id,omitempty"`
    ToolCalls  []ToolCall  `json:"tool_calls,omitempty"`
}

type Tool struct {
    Type     string   `json:"type" validate:"oneof=function"`
    Function Function `json:"function"`
}

type Function struct {
    Name        string      `json:"name" validate:"required"`
    Description string      `json:"description" validate:"required"`
    Parameters  interface{} `json:"parameters,omitempty"`
}

type OpenAIResponse struct {
    Success           bool             `json:"success"`
    Model             string           `json:"model"`
    Response          string           `json:"response,omitempty"`
    Choices           []Choice         `json:"choices,omitempty"`
    Usage             *Usage           `json:"usage,omitempty"`
    Created           int64            `json:"created,omitempty"`
    SystemFingerprint string           `json:"system_fingerprint,omitempty"`
    DurationMs        int64            `json:"duration_ms"`
    RetriesUsed       int              `json:"retries_used"`
    Error             *OpenAIError     `json:"error,omitempty"`
}

type Usage struct {
    PromptTokens        int                      `json:"prompt_tokens"`
    CompletionTokens    int                      `json:"completion_tokens"`
    TotalTokens         int                      `json:"total_tokens"`
    PromptTokensDetails *PromptTokensDetails     `json:"prompt_tokens_details,omitempty"`
    CompletionTokensDetails *CompletionTokensDetails `json:"completion_tokens_details,omitempty"`
}
```

#### 2. OpenAI Client Implementation (internal/client/)

**Core Features:**
- Chat completions API integration
- Streaming support (server-sent events)
- Function/tool calling
- Rate limiting and retry logic
- Token usage tracking
- Error handling for all OpenAI error types

#### 3. Rate Limiting (internal/ratelimit/)
```go
type RateLimiter struct {
    requestsPerMinute int
    tokensPerMinute   int
    // Implement token bucket or sliding window
}
```

#### 4. Streaming Support (internal/client/streaming.go)
- Handle server-sent events
- Parse streaming JSON responses
- Collect partial responses into complete text
- Handle streaming errors and reconnection

#### 5. Secret Resolution (internal/secrets/)
- Resolve `${{ secrets.openai-api-key }}` in API key field
- Support for custom endpoint secrets
- Security: Mask API keys in logs

#### 6. Error Handling
```go
type OpenAIError struct {
    Type    string `json:"type"`
    Message string `json:"message"`
    Code    string `json:"code"`
    Param   string `json:"param,omitempty"`
}
```

**Handle OpenAI-specific errors:**
- Rate limit exceeded (429)
- Invalid API key (401)
- Insufficient quota (402)
- Model not found (404)
- Context length exceeded (400)
- Content filtering (400)

#### 7. Retry Logic (internal/client/retry.go)
- Exponential backoff: 2s, 4s, 8s, 16s, 30s
- Retry on: 429 (rate limit), 5xx errors, timeouts
- Don't retry on: 401, 402, 403, 404, 400 (invalid request)
- Respect Retry-After headers

#### 8. Logging (internal/logging/)
```go
type LogEntry struct {
    Level           string    `json:"level"`
    Timestamp       time.Time `json:"timestamp"`
    Message         string    `json:"message"`
    RequestID       string    `json:"request_id,omitempty"`
    Model           string    `json:"model,omitempty"`
    PromptTokens    int       `json:"prompt_tokens,omitempty"`
    CompletionTokens int      `json:"completion_tokens,omitempty"`
    Duration        int64     `json:"duration_ms,omitempty"`
    Error           string    `json:"error,omitempty"`
    // Never log API keys or message content (privacy)
}
```

#### 9. Function Calling Support
- Parse tool definitions from input
- Handle tool_choice parameter
- Process function call responses
- Support parallel function calls
- Validate function parameters

#### 10. Validation (pkg/validation/)
- Model name validation
- Parameter range validation
- Message format validation
- Tool definition validation
- Token limit awareness per model

### Dockerfile:
```dockerfile
FROM golang:1.21-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o openai-client main.go

FROM alpine:latest
RUN apk --no-cache add ca-certificates tzdata
WORKDIR /root/
COPY --from=builder /app/openai-client .
ENTRYPOINT ["./openai-client"]
```

### Required Go Dependencies:
- `github.com/go-playground/validator/v10` - Input validation
- `github.com/sirupsen/logrus` - Structured logging
- `golang.org/x/time` - Rate limiting
- Standard library: `net/http`, `bufio`, `encoding/json`, `context`

### Advanced Features to Implement:

#### 1. Smart Retry with Backoff
```go
func (c *Client) retryWithBackoff(ctx context.Context, fn func() error) error {
    backoff := []time.Duration{2*time.Second, 4*time.Second, 8*time.Second, 16*time.Second, 30*time.Second}
    
    for i, delay := range backoff {
        err := fn()
        if err == nil {
            return nil
        }
        
        // Check if error is retryable
        if !isRetryableError(err) {
            return err
        }
        
        if i < len(backoff)-1 {
            select {
            case <-time.After(delay):
            case <-ctx.Done():
                return ctx.Err()
            }
        }
    }
    return fmt.Errorf("max retries exceeded")
}
```

#### 2. Token Counting
```go
// Estimate token count for pricing and limits
func estimateTokens(text string) int {
    // Simple approximation: ~4 characters per token
    return len(text) / 4
}
```

#### 3. Context Window Management
```go
func (c *Client) validateContextLength(messages []ChatMessage, model string) error {
    totalTokens := 0
    for _, msg := range messages {
        totalTokens += estimateTokens(msg.Content)
    }
    
    limit := getModelContextLimit(model)
    if totalTokens > limit {
        return fmt.Errorf("context length %d exceeds model limit %d", totalTokens, limit)
    }
    return nil
}
```

### Testing Requirements:
- Unit tests for all components
- Integration tests with OpenAI API (using test key)
- Function calling tests
- Streaming tests
- Rate limiting tests
- Error handling tests
- Token counting accuracy tests

### Success Criteria:
- ✅ All OpenAI chat completion features supported
- ✅ Function calling works correctly
- ✅ Streaming responses handled properly
- ✅ Rate limiting and retry logic implemented
- ✅ Secret resolution secure and working
- ✅ Error handling for all OpenAI error types
- ✅ Token usage accurately tracked
- ✅ Context length validation
- ✅ Performance: <200ms overhead
- ✅ Memory usage: <100MB baseline

### Integration with Bobrapet:
```yaml
# Example Story using OpenAI engram
apiVersion: bubu.sh/v1alpha1
kind: Story
metadata:
  name: ai-analysis-workflow
spec:
  steps:
    - name: analyze-text
      engramRef: openai-client
      inputs:
        api_key: "${{ secrets.openai-api-key }}"
        model: "gpt-4"
        messages:
          - role: "system"
            content: "You are a data analyst. Analyze the following data and provide insights."
          - role: "user"
            content: "${{ inputs.data_to_analyze }}"
        max_tokens: 1000
        temperature: 0.3
    
    - name: generate-report
      engramRef: openai-client
      inputs:
        api_key: "${{ secrets.openai-api-key }}"
        model: "gpt-4"
        messages:
          - role: "system"
            content: "Generate a professional report based on this analysis."
          - role: "user"
            content: "${{ steps.analyze-text.outputs.response }}"
```

Generate the complete implementation with all files, comprehensive error handling, streaming support, function calling, rate limiting, and production-ready code!
