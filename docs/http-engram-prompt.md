# HTTP Client Engram Generation Prompt (Go)

## Context: Bobrapet HTTP Client Engram

You are creating an **HTTP Client Engram** for the Bobrapet workflow system. This engram will perform HTTP requests with proper error handling, retries, and secret resolution.

### Architecture Requirements:
- **Language**: Go (latest stable version)
- **Container**: Alpine-based for minimal size
- **Input**: JSON via `ENGRAM_INPUT` environment variable
- **Output**: JSON via stdout
- **Secrets**: Resolve `${{ secrets.name }}` expressions
- **Logging**: Structured JSON to stderr

## EngramTemplate Specification

First, create the **EngramTemplate YAML** that defines this engram:

```yaml
apiVersion: catalog.bubu.sh/v1alpha1
kind: EngramTemplate
metadata:
  name: http-client
spec:
  version: "1.0.0"
  description: "HTTP client for making REST API calls with retry logic and secret support"
  image: "your-registry/http-client-engram:1.0.0"
  
  supportedModes:
    - "job"
    - "deployment"
  
  uiHints:
    category: "http"
    displayName: "HTTP Client"
    icon: "globe"
    color: "#4CAF50"
    tags: ["http", "api", "rest", "client"]
  
  resources:
    cpu: "100m"
    memory: "128Mi"
  
  security:
    requiredSecrets: ["api-token"]  # Example - will be resolved dynamically
    networkAccess: ["external"]
  
  defaults:
    timeout: "30s"
    retry:
      maxRetries: 3
      backoff: "exponential"
      baseDelay: "1s"
      maxDelay: "30s"
    resources:
      requests:
        cpu: "50m"
        memory: "64Mi"
      limits:
        cpu: "200m"
        memory: "256Mi"
    config:
      follow_redirects: true
      verify_ssl: true
      max_redirects: 10
  
  inputSchema:
    type: object
    required: ["url", "method"]
    properties:
      url:
        type: string
        format: uri
        description: "Target URL for the HTTP request"
      method:
        type: string
        enum: ["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"]
        description: "HTTP method to use"
      headers:
        type: object
        description: "HTTP headers (supports secret resolution)"
        additionalProperties:
          type: string
      body:
        description: "Request body (JSON object or string)"
        oneOf:
          - type: object
          - type: string
      query_params:
        type: object
        description: "URL query parameters"
        additionalProperties:
          type: string
      timeout:
        type: string
        pattern: "^[0-9]+(s|m|h)$"
        default: "30s"
        description: "Request timeout (e.g., '30s', '5m')"
      retries:
        type: integer
        minimum: 0
        maximum: 10
        default: 3
        description: "Number of retry attempts"
      retry_delay:
        type: string
        pattern: "^[0-9]+(s|m|h)$"
        default: "1s"
        description: "Base delay between retries"
      follow_redirects:
        type: boolean
        default: true
        description: "Whether to follow HTTP redirects"
      verify_ssl:
        type: boolean
        default: true
        description: "Whether to verify SSL certificates"
      auth:
        type: object
        description: "Authentication configuration"
        properties:
          type:
            type: string
            enum: ["bearer", "basic", "api_key"]
          token:
            type: string
            description: "Bearer token or API key (supports secrets)"
          username:
            type: string
            description: "Basic auth username"
          password:
            type: string
            description: "Basic auth password (supports secrets)"
          header_name:
            type: string
            description: "Header name for API key auth"
  
  outputSchema:
    type: object
    required: ["status_code", "success"]
    properties:
      status_code:
        type: integer
        description: "HTTP response status code"
      success:
        type: boolean
        description: "Whether the request was successful (2xx status)"
      headers:
        type: object
        description: "Response headers"
        additionalProperties:
          type: string
      body:
        description: "Response body (parsed JSON or raw string)"
        oneOf:
          - type: object
          - type: string
          - type: "null"
      duration_ms:
        type: integer
        description: "Request duration in milliseconds"
      retries_used:
        type: integer
        description: "Number of retries actually used"
      final_url:
        type: string
        description: "Final URL after redirects"
      content_type:
        type: string
        description: "Response content type"
      content_length:
        type: integer
        description: "Response content length in bytes"
  
  examples:
    - name: "simple-get"
      description: "Simple GET request"
      input:
        url: "https://httpbin.org/get"
        method: "GET"
      output:
        status_code: 200
        success: true
        body: {"url": "https://httpbin.org/get"}
    
    - name: "post-with-auth"
      description: "POST request with Bearer authentication"
      input:
        url: "https://api.example.com/data"
        method: "POST"
        headers:
          Authorization: "Bearer ${{ secrets.api-token }}"
          Content-Type: "application/json"
        body:
          key: "value"
      output:
        status_code: 201
        success: true
    
    - name: "authenticated-api-call"
      description: "API call with custom authentication"
      input:
        url: "https://api.service.com/endpoint"
        method: "GET"
        auth:
          type: "api_key"
          header_name: "X-API-Key"
          token: "${{ secrets.service-api-key }}"
      output:
        status_code: 200
        success: true
```

## Go Implementation Requirements

### Project Structure:
```
http-client-engram/
├── Dockerfile
├── go.mod
├── go.sum
├── main.go
├── internal/
│   ├── client/
│   │   ├── http_client.go
│   │   └── retry.go
│   ├── auth/
│   │   └── auth.go
│   ├── secrets/
│   │   └── resolver.go
│   └── logging/
│       └── logger.go
├── pkg/
│   ├── types/
│   │   ├── input.go
│   │   └── output.go
│   └── validation/
│       └── validator.go
├── examples/
│   ├── input-examples.json
│   └── output-examples.json
└── README.md
```

### Core Features to Implement:

#### 1. Input/Output Types (pkg/types/)
```go
type HTTPRequest struct {
    URL           string            `json:"url" validate:"required,url"`
    Method        string            `json:"method" validate:"required,oneof=GET POST PUT DELETE PATCH HEAD OPTIONS"`
    Headers       map[string]string `json:"headers,omitempty"`
    Body          interface{}       `json:"body,omitempty"`
    QueryParams   map[string]string `json:"query_params,omitempty"`
    Timeout       string            `json:"timeout,omitempty"`
    Retries       int               `json:"retries,omitempty"`
    RetryDelay    string            `json:"retry_delay,omitempty"`
    FollowRedirects bool            `json:"follow_redirects,omitempty"`
    VerifySSL     bool              `json:"verify_ssl,omitempty"`
    Auth          *AuthConfig       `json:"auth,omitempty"`
}

type AuthConfig struct {
    Type       string `json:"type" validate:"oneof=bearer basic api_key"`
    Token      string `json:"token,omitempty"`
    Username   string `json:"username,omitempty"`
    Password   string `json:"password,omitempty"`
    HeaderName string `json:"header_name,omitempty"`
}

type HTTPResponse struct {
    StatusCode    int               `json:"status_code"`
    Success       bool              `json:"success"`
    Headers       map[string]string `json:"headers"`
    Body          interface{}       `json:"body"`
    DurationMs    int64             `json:"duration_ms"`
    RetriesUsed   int               `json:"retries_used"`
    FinalURL      string            `json:"final_url"`
    ContentType   string            `json:"content_type"`
    ContentLength int64             `json:"content_length"`
}
```

#### 2. Secret Resolution (internal/secrets/)
- Implement `${{ secrets.name }}` pattern resolution
- Support nested secret references in headers, auth, body
- Security: Mask secrets in logs

#### 3. HTTP Client (internal/client/)
- Configurable timeouts and retries
- Exponential backoff with jitter
- Redirect handling
- SSL verification control
- Request/response logging (without secrets)

#### 4. Authentication (internal/auth/)
- Bearer token authentication
- Basic authentication
- API key authentication (custom headers)
- Support secret resolution in auth fields

#### 5. Retry Logic (internal/client/retry.go)
- Exponential backoff: 1s, 2s, 4s, 8s...
- Maximum retry limits
- Retry on: 5xx errors, timeouts, connection errors
- Don't retry on: 4xx client errors (except 429)

#### 6. Error Handling
- Network errors
- Timeout errors
- HTTP error status codes
- JSON parsing errors
- Validation errors

#### 7. Logging (internal/logging/)
```go
type LogEntry struct {
    Level     string    `json:"level"`
    Timestamp time.Time `json:"timestamp"`
    Message   string    `json:"message"`
    RequestID string    `json:"request_id,omitempty"`
    URL       string    `json:"url,omitempty"`
    Method    string    `json:"method,omitempty"`
    StatusCode int      `json:"status_code,omitempty"`
    Duration  int64     `json:"duration_ms,omitempty"`
    Error     string    `json:"error,omitempty"`
}
```

### Dockerfile:
```dockerfile
FROM golang:1.21-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o http-client main.go

FROM alpine:latest
RUN apk --no-cache add ca-certificates tzdata
WORKDIR /root/
COPY --from=builder /app/http-client .
ENTRYPOINT ["./http-client"]
```

### Required Go Dependencies:
- `github.com/go-playground/validator/v10` - Input validation
- `github.com/sirupsen/logrus` - Structured logging
- Standard library: `net/http`, `time`, `context`, `encoding/json`

### Main Implementation Points:

1. **Parse ENGRAM_INPUT** environment variable
2. **Validate input** using struct tags and validator
3. **Resolve secrets** in all string fields recursively
4. **Configure HTTP client** with timeouts and SSL settings
5. **Apply authentication** based on auth config
6. **Execute request** with retry logic
7. **Parse response** and handle different content types
8. **Output structured JSON** to stdout
9. **Log all operations** to stderr (mask secrets)
10. **Handle errors** with proper exit codes

### Testing Requirements:
- Unit tests for all components
- Integration tests with httpbin.org
- Secret resolution tests
- Retry logic tests
- Error handling tests

### Success Criteria:
- ✅ Supports all HTTP methods
- ✅ Secret resolution works correctly
- ✅ Retry logic with exponential backoff
- ✅ Proper error handling and logging
- ✅ Authentication methods work
- ✅ SSL verification configurable
- ✅ Response parsing handles JSON/text
- ✅ Performance: <100ms overhead
- ✅ Memory usage: <50MB baseline

Generate the complete implementation with all files, proper error handling, comprehensive logging, and production-ready code!
