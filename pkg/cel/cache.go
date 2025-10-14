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

package cel

import (
	"context"
	"crypto/sha256"
	"fmt"
	"sync"
	"time"

	"github.com/google/cel-go/cel"

	"github.com/bubustack/bobrapet/pkg/metrics"
	"github.com/bubustack/bobrapet/pkg/observability"
)

// CachedProgram represents a compiled CEL program with metadata
type CachedProgram struct {
	Program  cel.Program
	AST      *cel.Ast
	CachedAt time.Time
	HitCount int64
	LastUsed time.Time
	ExprHash string
}

// CompilationCache provides thread-safe caching of compiled CEL expressions
type CompilationCache struct {
	cache     map[string]*CachedProgram
	mu        sync.RWMutex
	maxSize   int
	ttl       time.Duration
	env       *cel.Env
	hitCount  int64
	missCount int64
	logger    observability.Logger
	stopCh    chan struct{}
}

// CacheConfig holds configuration for the CEL cache
type CacheConfig struct {
	MaxSize int           // Maximum number of cached programs
	TTL     time.Duration // Time to live for cached entries
}

// DefaultCacheConfig returns default cache configuration
func DefaultCacheConfig() *CacheConfig {
	return &CacheConfig{
		MaxSize: 1000,             // Cache up to 1000 compiled expressions
		TTL:     30 * time.Minute, // Cache for 30 minutes
	}
}

// NewCompilationCache creates a new CEL compilation cache
func NewCompilationCache(config *CacheConfig, env *cel.Env, logger observability.Logger) *CompilationCache {
	if config == nil {
		config = DefaultCacheConfig()
	}

	cache := &CompilationCache{
		cache:   make(map[string]*CachedProgram),
		maxSize: config.MaxSize,
		ttl:     config.TTL,
		env:     env,
		logger:  logger,
		stopCh:  make(chan struct{}),
	}

	// Start background cleanup routine
	go cache.cleanupRoutine()

	return cache
}

// CompileAndCache compiles a CEL expression and caches the result
func (cc *CompilationCache) CompileAndCache(ctx context.Context, expression, expressionType string) (*CachedProgram, error) {
	// Generate cache key
	key := cc.generateCacheKey(expression, expressionType)

	// Check cache first
	cc.mu.RLock()
	if cached, exists := cc.cache[key]; exists && !cc.isExpired(cached) {
		// Update hit statistics
		cached.HitCount++
		cached.LastUsed = time.Now()
		cc.hitCount++
		cc.mu.RUnlock()

		// Record cache hit
		metrics.RecordCELCacheHit("compilation")
		if cc.logger != nil {
			cc.logger.CacheHit(expression, expressionType)
		}

		return cached, nil
	}
	cc.mu.RUnlock()

	// Cache miss - compile the expression
	cc.missCount++

	if cc.logger != nil {
		cc.logger.EvaluationStart(expression, expressionType)
	}
	start := time.Now()

	// Compile the expression
	ast, issues := cc.env.Compile(expression)
	if issues != nil && issues.Err() != nil {
		duration := time.Since(start)
		if cc.logger != nil {
			cc.logger.EvaluationError(issues.Err(), expression, expressionType, duration)
		}
		metrics.RecordCELEvaluation(expressionType, duration, issues.Err())
		return nil, issues.Err()
	}

	program, err := cc.env.Program(ast)
	if err != nil {
		duration := time.Since(start)
		if cc.logger != nil {
			cc.logger.EvaluationError(err, expression, expressionType, duration)
		}
		metrics.RecordCELEvaluation(expressionType, duration, err)
		return nil, err
	}

	duration := time.Since(start)
	if cc.logger != nil {
		cc.logger.EvaluationSuccess(expression, expressionType, duration, "compiled")
	}
	metrics.RecordCELEvaluation(expressionType, duration, nil)

	// Create cached program
	cached := &CachedProgram{
		Program:  program,
		AST:      ast,
		CachedAt: time.Now(),
		HitCount: 0,
		LastUsed: time.Now(),
		ExprHash: key,
	}

	// Store in cache with write lock
	cc.mu.Lock()
	defer cc.mu.Unlock()

	// Check if we need to evict entries
	if len(cc.cache) >= cc.maxSize {
		cc.evictLRU()
	}

	cc.cache[key] = cached
	return cached, nil
}

// Evaluate compiles (with caching) and evaluates a CEL expression
func (cc *CompilationCache) Evaluate(ctx context.Context, expression, expressionType string, vars map[string]interface{}) (interface{}, error) {
	// Get or compile the program
	cached, err := cc.CompileAndCache(ctx, expression, expressionType)
	if err != nil {
		return nil, err
	}

	// Evaluate the program
	start := time.Now()
	result, _, err := cached.Program.Eval(vars)
	duration := time.Since(start)

	if err != nil {
		if cc.logger != nil {
			cc.logger.EvaluationError(err, expression, expressionType, duration)
		}
		metrics.RecordCELEvaluation(expressionType+"_eval", duration, err)
		return nil, err
	}

	// Extract the actual value
	value := result.Value()
	if cc.logger != nil {
		cc.logger.EvaluationSuccess(expression, expressionType, duration, value)
	}
	metrics.RecordCELEvaluation(expressionType+"_eval", duration, nil)

	return value, nil
}

// EvaluateCondition evaluates a boolean CEL expression with caching
func (cc *CompilationCache) EvaluateCondition(ctx context.Context, expression string, vars map[string]interface{}) (bool, error) {
	result, err := cc.Evaluate(ctx, expression, "condition", vars)
	if err != nil {
		return false, err
	}

	if b, ok := result.(bool); ok {
		return b, nil
	}

	return false, fmt.Errorf("expression did not evaluate to boolean, got %T", result)
}

// EvaluateTransform evaluates a transform CEL expression with caching
func (cc *CompilationCache) EvaluateTransform(ctx context.Context, expression string, vars map[string]interface{}) (interface{}, error) {
	return cc.Evaluate(ctx, expression, "transform", vars)
}

// EvaluateFilter evaluates a filter CEL expression with caching
func (cc *CompilationCache) EvaluateFilter(ctx context.Context, expression string, vars map[string]interface{}) (bool, error) {
	result, err := cc.Evaluate(ctx, expression, "filter", vars)
	if err != nil {
		return false, err
	}

	if b, ok := result.(bool); ok {
		return b, nil
	}

	return false, fmt.Errorf("filter expression did not evaluate to boolean, got %T", result)
}

// GetStats returns cache statistics
func (cc *CompilationCache) GetStats() CacheStats {
	cc.mu.RLock()
	defer cc.mu.RUnlock()

	return CacheStats{
		Size:      len(cc.cache),
		MaxSize:   cc.maxSize,
		HitCount:  cc.hitCount,
		MissCount: cc.missCount,
		HitRatio:  float64(cc.hitCount) / float64(cc.hitCount+cc.missCount),
	}
}

// CacheStats represents cache performance statistics
type CacheStats struct {
	Size      int     `json:"size"`
	MaxSize   int     `json:"maxSize"`
	HitCount  int64   `json:"hitCount"`
	MissCount int64   `json:"missCount"`
	HitRatio  float64 `json:"hitRatio"`
}

// Clear removes all entries from the cache
func (cc *CompilationCache) Clear() {
	cc.mu.Lock()
	defer cc.mu.Unlock()

	cc.cache = make(map[string]*CachedProgram)
	cc.hitCount = 0
	cc.missCount = 0
}

// Stop terminates background maintenance goroutines.
func (cc *CompilationCache) Stop() {
	select {
	case <-cc.stopCh:
		// already closed
	default:
		close(cc.stopCh)
	}
}

// generateCacheKey creates a unique cache key for an expression
func (cc *CompilationCache) generateCacheKey(expression, expressionType string) string {
	data := fmt.Sprintf("%s:%s", expressionType, expression)
	hash := sha256.Sum256([]byte(data))
	return fmt.Sprintf("%x", hash)
}

// isExpired checks if a cached program has expired
func (cc *CompilationCache) isExpired(cached *CachedProgram) bool {
	return time.Since(cached.CachedAt) > cc.ttl
}

// evictLRU removes the least recently used entry from the cache
func (cc *CompilationCache) evictLRU() {
	var oldestKey string
	var oldestTime time.Time

	for key, cached := range cc.cache {
		if oldestKey == "" || cached.LastUsed.Before(oldestTime) {
			oldestKey = key
			oldestTime = cached.LastUsed
		}
	}

	if oldestKey != "" {
		delete(cc.cache, oldestKey)
	}
}

// cleanupRoutine periodically removes expired entries
func (cc *CompilationCache) cleanupRoutine() {
	ticker := time.NewTicker(5 * time.Minute) // Cleanup every 5 minutes
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			cc.cleanup()
		case <-cc.stopCh:
			return
		}
	}
}

// cleanup removes expired entries from the cache
func (cc *CompilationCache) cleanup() {
	cc.mu.Lock()
	defer cc.mu.Unlock()

	for key, cached := range cc.cache {
		if cc.isExpired(cached) {
			delete(cc.cache, key)
		}
	}
}

// ProcessList evaluates a CEL expression over a list of items with caching
func (cc *CompilationCache) ProcessList(ctx context.Context, expression, expressionType string, items []interface{}, baseVars map[string]interface{}) ([]interface{}, error) {
	var results []interface{}

	// Compile the expression once and reuse for all items
	cached, err := cc.CompileAndCache(ctx, expression, expressionType)
	if err != nil {
		return nil, err
	}

	for i, item := range items {
		// Create evaluation context for this item
		vars := make(map[string]interface{})
		for k, v := range baseVars {
			vars[k] = v
		}
		vars["item"] = item
		vars["index"] = i

		// Evaluate the expression
		start := time.Now()
		result, _, err := cached.Program.Eval(vars)
		duration := time.Since(start)

		if err != nil {
			if cc.logger != nil {
				cc.logger.EvaluationError(err, expression, expressionType, duration)
			}
			metrics.RecordCELEvaluation(expressionType+"_list", duration, err)
			return nil, fmt.Errorf("evaluation failed for item %d: %w", i, err)
		}

		results = append(results, result.Value())
		metrics.RecordCELEvaluation(expressionType+"_list", duration, nil)
	}

	return results, nil
}

// Global cache instance
var globalCache *CompilationCache
var globalCacheOnce sync.Once

// GetGlobalCache returns the global CEL compilation cache
func GetGlobalCache(env *cel.Env) *CompilationCache {
	globalCacheOnce.Do(func() {
		globalCache = NewCompilationCache(DefaultCacheConfig(), env, nil)
	})
	return globalCache
}
