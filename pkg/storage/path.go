package storage

import (
	"fmt"
	"strconv"
	"strings"
)

type storagePathTokenKind int

const (
	storagePathField storagePathTokenKind = iota
	storagePathIndex
)

type storagePathToken struct {
	kind  storagePathTokenKind
	field string
	index int
}

func resolveStoragePath(value any, path string) (any, error) {
	tokens, err := parseStoragePath(path)
	if err != nil {
		return nil, err
	}
	current := value
	for _, token := range tokens {
		switch token.kind {
		case storagePathField:
			obj, ok := current.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("expected object for field %q, got %T", token.field, current)
			}
			next, exists := obj[token.field]
			if !exists {
				return nil, fmt.Errorf("missing field %q", token.field)
			}
			current = next
		case storagePathIndex:
			list, ok := current.([]any)
			if !ok {
				return nil, fmt.Errorf("expected array for index %d, got %T", token.index, current)
			}
			if token.index < 0 || token.index >= len(list) {
				return nil, fmt.Errorf("index %d out of bounds", token.index)
			}
			current = list[token.index]
		default:
			return nil, fmt.Errorf("unsupported path token")
		}
	}
	return current, nil
}

func parseStoragePath(path string) ([]storagePathToken, error) {
	trimmed := strings.TrimSpace(path)
	if trimmed == "" {
		return nil, fmt.Errorf("path is empty")
	}
	tokens := make([]storagePathToken, 0)
	i := 0
	for i < len(trimmed) {
		switch trimmed[i] {
		case '.':
			i++
			continue
		case '[':
			end := strings.IndexByte(trimmed[i:], ']')
			if end == -1 {
				return nil, fmt.Errorf("unterminated index in path")
			}
			rawIndex := strings.TrimSpace(trimmed[i+1 : i+end])
			if rawIndex == "" {
				return nil, fmt.Errorf("empty index in path")
			}
			idx, err := strconv.Atoi(rawIndex)
			if err != nil {
				return nil, fmt.Errorf("invalid index %q", rawIndex)
			}
			tokens = append(tokens, storagePathToken{kind: storagePathIndex, index: idx})
			i = i + end + 1
		default:
			start := i
			for i < len(trimmed) && trimmed[i] != '.' && trimmed[i] != '[' {
				i++
			}
			field := strings.TrimSpace(trimmed[start:i])
			if field == "" {
				return nil, fmt.Errorf("empty field in path")
			}
			tokens = append(tokens, storagePathToken{kind: storagePathField, field: field})
		}
	}
	return tokens, nil
}
