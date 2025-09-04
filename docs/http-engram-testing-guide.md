# HTTP Engram Testing & Integration Guide

## 🎯 **Overview: How HTTP Engrams Work in Bobrapet**

Based on the StepRun controller analysis, here's how HTTP engrams integrate with the workflow system:

### **Input/Output Flow:**
1. **Input**: JSON via `ENGRAM_INPUT` environment variable
2. **Processing**: HTTP engram performs the request
3. **Output**: JSON to stdout (captured by StepRun controller)
4. **Storage**: Output stored in `StepRun.status.output.raw`
5. **Chaining**: Next steps access via `${{ steps.step-name.outputs.field }}`

## 🧪 **Testing the HTTP Engram**

### **Step 1: Create Test EngramTemplate**

First, create the HTTP client template:

```yaml
# test-http-template.yaml
apiVersion: catalog.bubu.sh/v1alpha1
kind: EngramTemplate
metadata:
  name: http-client-go
spec:
  version: "1.0.0"
  description: "HTTP client for testing API calls"
  image: "http-client-engram:latest"
  
  supportedModes:
    - "job"
  
  uiHints:
    category: "http"
    displayName: "HTTP Test Client"
    icon: "globe"
    tags: ["http", "test", "api"]
  
  inputSchema:
    type: object
    required: ["url", "method"]
    properties:
      url:
        type: string
        format: uri
      method:
        type: string
        enum: ["GET", "POST", "PUT", "DELETE"]
      headers:
        type: object
        additionalProperties:
          type: string
      body:
        oneOf:
          - type: object
          - type: string
      timeout:
        type: string
        default: "30s"
  
  outputSchema:
    type: object
    required: ["status_code", "success"]
    properties:
      status_code:
        type: integer
      success:
        type: boolean
      headers:
        type: object
      body:
        oneOf:
          - type: object
          - type: string
      duration_ms:
        type: integer
```

### **Step 2: Create Test Engram**

```yaml
# test-http-engram.yaml
apiVersion: bubu.sh/v1alpha1
kind: Engram
metadata:
  name: http-test-engram
  namespace: default
spec:
  engine:
    templateRef: "http-client"
    mode: "job"
  
  # Override defaults if needed
  timeout: "60s"
  retryPolicy:
    maxRetries: 2
```

### **Step 3: Create Test Story**

```yaml
# test-http-story.yaml
apiVersion: bubu.sh/v1alpha1
kind: Story
metadata:
  name: http-workflow-test
  namespace: default
spec:
  steps:
    # Step 1: Simple GET request
    - name: get-request
      engramRef: http-test-engram
      inputs:
        url: "https://httpbin.org/get"
        method: "GET"
        headers:
          User-Agent: "Bobrapet-Test/1.0"
    
    # Step 2: POST with data (uses output from step 1)
    - name: post-request
      engramRef: http-test-engram
      needs: ["get-request"]
      inputs:
        url: "https://httpbin.org/post"
        method: "POST"
        headers:
          Content-Type: "application/json"
        body:
          test_data: "${{ steps.get-request.outputs.body.url }}"
          timestamp: "2024-01-01T00:00:00Z"
    
    # Step 3: Test error handling
    - name: error-request
      engramRef: http-test-engram
      needs: ["post-request"]
      inputs:
        url: "https://httpbin.org/status/404"
        method: "GET"
```

### **Step 4: Create Test StoryRun**

```yaml
# test-http-storyrun.yaml
apiVersion: runs.bubu.sh/v1alpha1
kind: StoryRun
metadata:
  name: http-test-run
  namespace: default
spec:
  storyRef: "http-workflow-test"
  
  # Optional: Override inputs
  inputs:
    test_mode: "integration"
```

## 🔍 **Expected Output Structure**

Based on the StepRun status analysis, your HTTP engram should output JSON to stdout like this:

### **Successful Response:**
```json
{
  "status_code": 200,
  "success": true,
  "headers": {
    "content-type": "application/json",
    "content-length": "345"
  },
  "body": {
    "url": "https://httpbin.org/get",
    "headers": {
      "User-Agent": "Bobrapet-Test/1.0"
    }
  },
  "duration_ms": 234,
  "retries_used": 0,
  "final_url": "https://httpbin.org/get",
  "content_type": "application/json",
  "content_length": 345
}
```

### **Error Response:**
```json
{
  "status_code": 404,
  "success": false,
  "headers": {
    "content-type": "text/html"
  },
  "body": "Not Found",
  "duration_ms": 123,
  "retries_used": 1,
  "final_url": "https://httpbin.org/status/404",
  "content_type": "text/html",
  "content_length": 9
}
```

## 🔗 **How Chaining Works**

### **StepRun Output Storage:**
1. **HTTP engram** prints JSON to stdout
2. **StepRun controller** captures stdout via `captureJobOutput()`
3. **Output stored** in `StepRun.status.output.raw` (as RawExtension)
4. **Next step** can access via expressions

### **Expression Resolution:**
```yaml
# In the next step's inputs:
inputs:
  api_response: "${{ steps.get-request.outputs.body }}"
  status_code: "${{ steps.get-request.outputs.status_code }}"
  was_successful: "${{ steps.get-request.outputs.success }}"
```

### **Expression Resolver Logic:**
The `ExpressionResolver` in `internal/controller/runs/expressions.go` handles:
- `${{ steps.step-name.outputs.field }}` - Access step outputs
- `${{ secrets.secret-name }}` - Access secrets
- `${{ inputs.field }}` - Access story inputs

## 🧪 **Complete Testing Workflow**

### **1. Deploy and Test:**

```bash
# Apply the resources
kubectl apply -f test-http-template.yaml
kubectl apply -f test-http-engram.yaml
kubectl apply -f test-http-story.yaml
kubectl apply -f test-http-storyrun.yaml

# Watch the progress
kubectl get storyrun http-test-run -w

# Check StepRun status
kubectl get steprun -l bubu.sh/story-run=http-test-run

# View detailed status
kubectl describe steprun <steprun-name>
```

### **2. Verify Output Chaining:**

```bash
# Check the first step's output
kubectl get steprun <get-request-steprun> -o jsonpath='{.status.output}' | jq .

# Check the second step's input resolution
kubectl logs <post-request-pod> --previous | grep "ENGRAM_INPUT"
```

### **3. Test Output Access:**

```yaml
# Create a simple debug story to test output access
apiVersion: bubu.sh/v1alpha1
kind: Story
metadata:
  name: output-test
spec:
  steps:
    - name: http-call
      engramRef: http-test-engram
      inputs:
        url: "https://httpbin.org/json"
        method: "GET"
    
    - name: debug-output
      engramRef: debug-engram  # Simple echo engram
      needs: ["http-call"]
      inputs:
        received_status: "${{ steps.http-call.outputs.status_code }}"
        received_body: "${{ steps.http-call.outputs.body }}"
        was_success: "${{ steps.http-call.outputs.success }}"
```

## 📋 **HTTP Engram Implementation Checklist**

Your HTTP engram **MUST** implement:

### **✅ Input Processing:**
- [ ] Parse `ENGRAM_INPUT` environment variable
- [ ] Validate input against schema
- [ ] Resolve `${{ secrets.name }}` patterns
- [ ] Handle missing/invalid fields gracefully

### **✅ HTTP Operations:**
- [ ] Support all HTTP methods (GET, POST, PUT, DELETE, etc.)
- [ ] Handle headers correctly
- [ ] Process request body (JSON/string)
- [ ] Implement timeout handling
- [ ] Implement retry logic with exponential backoff

### **✅ Output Generation:**
- [ ] Always output valid JSON to stdout
- [ ] Include all required fields: `status_code`, `success`
- [ ] Include all optional fields: `headers`, `body`, `duration_ms`, etc.
- [ ] Handle different response content types
- [ ] Parse JSON responses when possible

### **✅ Error Handling:**
- [ ] Use appropriate exit codes (0=success, 10=retry, 11=terminal)
- [ ] Output errors to stderr as structured JSON
- [ ] Still output success=false to stdout for failed HTTP requests
- [ ] Handle network errors, timeouts, DNS issues

### **✅ Logging:**
- [ ] Structured JSON logs to stderr
- [ ] Never log secrets/sensitive data
- [ ] Include request ID for traceability
- [ ] Log performance metrics

## 🎯 **Testing Different Scenarios**

### **1. Success Case:**
```yaml
inputs:
  url: "https://httpbin.org/get"
  method: "GET"
```
**Expected**: `status_code: 200`, `success: true`

### **2. Client Error (4xx):**
```yaml
inputs:
  url: "https://httpbin.org/status/404"
  method: "GET"
```
**Expected**: `status_code: 404`, `success: false`, exit code 0

### **3. Server Error (5xx):**
```yaml
inputs:
  url: "https://httpbin.org/status/500"
  method: "GET"
```
**Expected**: `status_code: 500`, `success: false`, exit code 10 (retry)

### **4. Network Error:**
```yaml
inputs:
  url: "https://nonexistent-domain-12345.com"
  method: "GET"
```
**Expected**: Exit code 10 (retry), error to stderr

### **5. Authentication:**
```yaml
inputs:
  url: "https://httpbin.org/bearer"
  method: "GET"
  headers:
    Authorization: "Bearer ${{ secrets.test-token }}"
```

### **6. JSON POST:**
```yaml
inputs:
  url: "https://httpbin.org/post"
  method: "POST"
  headers:
    Content-Type: "application/json"
  body:
    key: "value"
    number: 42
```

## 🔧 **Debugging Tips**

### **1. Check StepRun Status:**
```bash
kubectl get steprun <name> -o yaml
```

### **2. View Pod Logs:**
```bash
kubectl logs <pod-name> --previous  # If job completed
kubectl logs <pod-name>             # If job running
```

### **3. Check Environment Variables:**
```bash
# Add debug output to your engram
echo "ENGRAM_INPUT: $ENGRAM_INPUT" >&2
```

### **4. Validate Output JSON:**
```bash
kubectl get steprun <name> -o jsonpath='{.status.output.raw}' | jq .
```

### **5. Test Expression Resolution:**
Create a simple test story that echoes the resolved inputs to verify the chaining works correctly.

## 🎉 **Success Criteria**

Your HTTP engram is working correctly when:

1. ✅ **StepRun status** shows `phase: "Succeeded"`
2. ✅ **Output field** contains valid JSON with all required fields
3. ✅ **Next steps** can access outputs via expressions
4. ✅ **Error cases** handled appropriately with correct exit codes
5. ✅ **Secrets resolution** works without exposing values in logs
6. ✅ **Performance** is reasonable (typically <10s for simple requests)

This completes the HTTP engram testing and integration guide! Your engram should now work seamlessly in Bobrapet workflow chains.
