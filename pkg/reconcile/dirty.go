package reconcile

import "sync"

// Set is a tiny "dirty flag" set.
//
// Key rule: controllers must NOT "consume" (delete) a dirty flag
// until the corresponding recount/status update has succeeded.
type Set struct {
	m sync.Map // map[string]struct{}
}

func (s *Set) Mark(key string) {
	if key == "" {
		return
	}
	s.m.Store(key, struct{}{})
}

func (s *Set) IsDirty(key string) bool {
	if key == "" {
		return false
	}
	_, ok := s.m.Load(key)
	return ok
}

// Clear deletes the dirty flag and returns true if it existed.
func (s *Set) Clear(key string) bool {
	if key == "" {
		return false
	}
	_, existed := s.m.LoadAndDelete(key)
	return existed
}
