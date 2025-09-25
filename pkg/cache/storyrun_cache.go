package cache

import (
	"sync"

	runsv1alpha1 "github.com/bubustack/bobrapet/api/runs/v1alpha1"
	"k8s.io/apimachinery/pkg/types"
)

// StoryRunCache is a simple thread-safe in-memory cache for StoryRun objects.
type StoryRunCache struct {
	mu    sync.RWMutex
	store map[types.NamespacedName]*runsv1alpha1.StoryRun
}

// NewStoryRunCache creates a new StoryRunCache.
func NewStoryRunCache() *StoryRunCache {
	return &StoryRunCache{
		store: make(map[types.NamespacedName]*runsv1alpha1.StoryRun),
	}
}

// Get retrieves a StoryRun from the cache.
func (c *StoryRunCache) Get(name types.NamespacedName) (*runsv1alpha1.StoryRun, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	srun, found := c.store[name]
	return srun, found
}

// AddOrUpdate adds or updates a StoryRun in the cache.
func (c *StoryRunCache) AddOrUpdate(srun *runsv1alpha1.StoryRun) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.store[types.NamespacedName{Name: srun.Name, Namespace: srun.Namespace}] = srun
}

// Delete removes a StoryRun from the cache.
func (c *StoryRunCache) Delete(name types.NamespacedName) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.store, name)
}
