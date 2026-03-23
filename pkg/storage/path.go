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
	storagePathWildcard
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
	value, _, err = resolveStorageTokens(value, tokens, false)
	if err != nil {
		return nil, err
	}
	return value, nil
}

func resolveStorageTokens(value any, tokens []storagePathToken, allowMissing bool) (any, bool, error) {
	if len(tokens) == 0 {
		if value == nil {
			return nil, false, nil
		}
		return value, true, nil
	}

	token := tokens[0]
	switch token.kind {
	case storagePathField:
		obj, ok := value.(map[string]any)
		if !ok {
			return nil, false, fmt.Errorf("expected object for field %q, got %T", token.field, value)
		}
		next, exists := obj[token.field]
		if !exists {
			if allowMissing {
				return nil, false, nil
			}
			return nil, false, fmt.Errorf("missing field %q", token.field)
		}
		return resolveStorageTokens(next, tokens[1:], allowMissing)
	case storagePathIndex:
		list, ok := value.([]any)
		if !ok {
			return nil, false, fmt.Errorf("expected array for index %d, got %T", token.index, value)
		}
		if token.index < 0 || token.index >= len(list) {
			if allowMissing {
				return nil, false, nil
			}
			return nil, false, fmt.Errorf("index %d out of bounds", token.index)
		}
		return resolveStorageTokens(list[token.index], tokens[1:], allowMissing)
	case storagePathWildcard:
		list, ok := value.([]any)
		if !ok {
			return nil, false, fmt.Errorf("expected array for wildcard index, got %T", value)
		}
		results := make([]any, len(list))
		for i, item := range list {
			resolved, exists, err := resolveStorageTokens(item, tokens[1:], true)
			if err != nil {
				return nil, false, err
			}
			if exists {
				results[i] = resolved
			} else {
				results[i] = nil
			}
		}
		return results, true, nil
	default:
		return nil, false, fmt.Errorf("unsupported path token")
	}
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
			if rawIndex == "*" {
				tokens = append(tokens, storagePathToken{kind: storagePathWildcard})
				i = i + end + 1
				continue
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
