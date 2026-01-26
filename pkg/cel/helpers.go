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
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
)

// EnhancedCELEnvironment creates a CEL environment with additional functions for data operations
func EnhancedCELEnvironment() (*cel.Env, error) {
	return cel.NewEnv(
		// Standard library extensions
		cel.Lib(&dataOperationsLib{}),

		// Enable optional types for null handling
		cel.OptionalTypes(),

		// Enable comprehension for advanced list operations
		cel.EnableMacroCallTracking(),

		// Declare common variables that will be available in templates
		cel.Variable("inputs", cel.MapType(cel.StringType, cel.DynType)),
		cel.Variable("steps", cel.MapType(cel.StringType, cel.DynType)),
		cel.Variable("now", cel.TimestampType),
		cel.Variable("run", cel.MapType(cel.StringType, cel.DynType)),
		cel.Variable("item", cel.DynType), // For loop iterations
	)
}

// dataOperationsLib provides custom CEL functions for data operations
type dataOperationsLib struct{}

func (d *dataOperationsLib) CompileOptions() []cel.EnvOption {
	return []cel.EnvOption{
		// Array/List operations
		cel.Function("sort_by",
			cel.MemberOverload("list_sort_by_string",
				[]*cel.Type{cel.ListType(cel.DynType), cel.StringType},
				cel.ListType(cel.DynType),
				cel.BinaryBinding(sortByField))),

		cel.Function("dedupe",
			cel.MemberOverload("list_dedupe",
				[]*cel.Type{cel.ListType(cel.DynType)},
				cel.ListType(cel.DynType),
				cel.UnaryBinding(deduplicate))),

		cel.Function("group_by",
			cel.MemberOverload("list_group_by_string",
				[]*cel.Type{cel.ListType(cel.DynType), cel.StringType},
				cel.MapType(cel.StringType, cel.ListType(cel.DynType)),
				cel.BinaryBinding(groupByField))),

		cel.Function("chunk",
			cel.MemberOverload("list_chunk_int",
				[]*cel.Type{cel.ListType(cel.DynType), cel.IntType},
				cel.ListType(cel.ListType(cel.DynType)),
				cel.BinaryBinding(chunkList))),

		// Aggregation operations
		cel.Function("sum_by",
			cel.MemberOverload("list_sum_by_string",
				[]*cel.Type{cel.ListType(cel.DynType), cel.StringType},
				cel.DoubleType,
				cel.BinaryBinding(sumByField))),

		cel.Function("count_by",
			cel.MemberOverload("list_count_by_string",
				[]*cel.Type{cel.ListType(cel.DynType), cel.StringType},
				cel.MapType(cel.StringType, cel.IntType),
				cel.BinaryBinding(countByField))),

		// String operations
		cel.Function("rename_keys",
			cel.MemberOverload("map_rename_keys_map",
				[]*cel.Type{cel.MapType(cel.StringType, cel.DynType), cel.MapType(cel.StringType, cel.StringType)},
				cel.MapType(cel.StringType, cel.DynType),
				cel.BinaryBinding(renameKeys))),

		// Date/Time operations
		cel.Function("now",
			cel.Overload("now_timestamp",
				[]*cel.Type{},
				cel.TimestampType,
				cel.FunctionBinding(func(values ...ref.Val) ref.Val {
					return types.Timestamp{Time: time.Now()}
				}))),

		cel.Function("format_time",
			cel.MemberOverload("timestamp_format_string",
				[]*cel.Type{cel.TimestampType, cel.StringType},
				cel.StringType,
				cel.BinaryBinding(formatTime))),

		// Utility functions
		cel.Function("take",
			cel.MemberOverload("list_take_int",
				[]*cel.Type{cel.ListType(cel.DynType), cel.IntType},
				cel.ListType(cel.DynType),
				cel.BinaryBinding(takeN))),

		cel.Function("skip",
			cel.MemberOverload("list_skip_int",
				[]*cel.Type{cel.ListType(cel.DynType), cel.IntType},
				cel.ListType(cel.DynType),
				cel.BinaryBinding(skipN))),
	}
}

func (d *dataOperationsLib) ProgramOptions() []cel.ProgramOption {
	return []cel.ProgramOption{}
}

// CEL function implementations

func sortByField(lhs, rhs ref.Val) ref.Val {
	list, ok := lhs.Value().([]any)
	if !ok {
		return types.NewErr("sort_by requires a list")
	}

	field, ok := rhs.Value().(string)
	if !ok {
		return types.NewErr("sort_by requires a string field name")
	}

	// Create a copy to sort
	sorted := make([]any, len(list))
	copy(sorted, list)

	var compareErr error
	sort.Slice(sorted, func(i, j int) bool {
		if compareErr != nil {
			return false
		}
		valI, err := getFieldValue(sorted[i], field)
		if err != nil {
			compareErr = fmt.Errorf("sort_by[%d]: %w", i, err)
			return false
		}
		valJ, err := getFieldValue(sorted[j], field)
		if err != nil {
			compareErr = fmt.Errorf("sort_by[%d]: %w", j, err)
			return false
		}
		return compareValues(valI, valJ) < 0
	})

	if compareErr != nil {
		return types.NewErr("sort_by invalid item: %v", compareErr)
	}

	return types.DefaultTypeAdapter.NativeToValue(sorted)
}

func deduplicate(arg ref.Val) ref.Val {
	list, ok := arg.Value().([]any)
	if !ok {
		return types.NewErr("dedupe requires a list")
	}

	seen := make(map[string]bool)
	var result []any

	for _, item := range list {
		key := fmt.Sprintf("%v", item)
		if !seen[key] {
			seen[key] = true
			result = append(result, item)
		}
	}

	return types.DefaultTypeAdapter.NativeToValue(result)
}

func groupByField(lhs, rhs ref.Val) ref.Val {
	list, ok := lhs.Value().([]any)
	if !ok {
		return types.NewErr("group_by requires a list")
	}

	field, ok := rhs.Value().(string)
	if !ok {
		return types.NewErr("group_by requires a string field name")
	}

	groups := make(map[string][]any)

	for idx, item := range list {
		value, err := getFieldValue(item, field)
		if err != nil {
			return types.NewErr("group_by item %d: %v", idx, err)
		}
		key := fmt.Sprintf("%v", value)
		groups[key] = append(groups[key], item)
	}

	return types.DefaultTypeAdapter.NativeToValue(groups)
}

func chunkList(lhs, rhs ref.Val) ref.Val {
	list, ok := lhs.Value().([]any)
	if !ok {
		return types.NewErr("chunk requires a list")
	}

	size, ok := rhs.Value().(int64)
	if !ok {
		return types.NewErr("chunk requires an integer size")
	}

	if size <= 0 {
		return types.NewErr("chunk size must be positive")
	}

	var chunks [][]any
	for i := 0; i < len(list); i += int(size) {
		end := i + int(size)
		if end > len(list) {
			end = len(list)
		}
		chunks = append(chunks, list[i:end])
	}

	return types.DefaultTypeAdapter.NativeToValue(chunks)
}

func sumByField(lhs, rhs ref.Val) ref.Val {
	list, ok := lhs.Value().([]any)
	if !ok {
		return types.NewErr("sum_by requires a list")
	}

	field, ok := rhs.Value().(string)
	if !ok {
		return types.NewErr("sum_by requires a string field name")
	}

	var sum float64
	for idx, item := range list {
		val, err := getFieldValue(item, field)
		if err != nil {
			return types.NewErr("sum_by item %d: %v", idx, err)
		}
		if num, ok := toNumber(val); ok {
			sum += num
		}
	}

	return types.Double(sum)
}

func countByField(lhs, rhs ref.Val) ref.Val {
	list, ok := lhs.Value().([]any)
	if !ok {
		return types.NewErr("count_by requires a list")
	}

	field, ok := rhs.Value().(string)
	if !ok {
		return types.NewErr("count_by requires a string field name")
	}

	counts := make(map[string]int64)

	for idx, item := range list {
		value, err := getFieldValue(item, field)
		if err != nil {
			return types.NewErr("count_by item %d: %v", idx, err)
		}
		key := fmt.Sprintf("%v", value)
		counts[key]++
	}

	return types.DefaultTypeAdapter.NativeToValue(counts)
}

func renameKeys(lhs, rhs ref.Val) ref.Val {
	data, ok := lhs.Value().(map[string]any)
	if !ok {
		return types.NewErr("rename_keys requires a map")
	}

	mapping, ok := rhs.Value().(map[string]any)
	if !ok {
		return types.NewErr("rename_keys requires a mapping")
	}

	result := make(map[string]any)

	for key, value := range data {
		if newKey, exists := mapping[key]; exists {
			if newKeyStr, ok := newKey.(string); ok {
				result[newKeyStr] = value
			} else {
				result[key] = value
			}
		} else {
			result[key] = value
		}
	}

	return types.DefaultTypeAdapter.NativeToValue(result)
}

func formatTime(lhs, rhs ref.Val) ref.Val {
	timestamp, ok := lhs.Value().(time.Time)
	if !ok {
		return types.NewErr("format_time requires a timestamp")
	}

	format, ok := rhs.Value().(string)
	if !ok {
		return types.NewErr("format_time requires a format string")
	}

	// Convert Go time format to standard format
	goFormat := convertTimeFormat(format)
	return types.String(timestamp.Format(goFormat))
}

func takeN(lhs, rhs ref.Val) ref.Val {
	list, ok := lhs.Value().([]any)
	if !ok {
		return types.NewErr("take requires a list")
	}

	n, ok := rhs.Value().(int64)
	if !ok {
		return types.NewErr("take requires an integer")
	}

	if n <= 0 {
		return types.DefaultTypeAdapter.NativeToValue([]any{})
	}

	if int(n) >= len(list) {
		return types.DefaultTypeAdapter.NativeToValue(list)
	}

	return types.DefaultTypeAdapter.NativeToValue(list[:n])
}

func skipN(lhs, rhs ref.Val) ref.Val {
	list, ok := lhs.Value().([]any)
	if !ok {
		return types.NewErr("skip requires a list")
	}

	n, ok := rhs.Value().(int64)
	if !ok {
		return types.NewErr("skip requires an integer")
	}

	if n <= 0 {
		return types.DefaultTypeAdapter.NativeToValue(list)
	}

	if int(n) >= len(list) {
		return types.DefaultTypeAdapter.NativeToValue([]any{})
	}

	return types.DefaultTypeAdapter.NativeToValue(list[n:])
}

// Helper functions

// getFieldValue returns item[field] when the item is a map[string]any and
// returns an error for other inputs so higher-level helpers can flag invalid
// rows (`pkg/cel/helpers.go:150-268,364-386`).
func getFieldValue(item any, field string) (any, error) {
	m, ok := item.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("expected map[string]any for field %q, got %T", field, item)
	}
	return m[field], nil
}

// compareValues orders two CEL-friendly values, supporting string, int64,
// float64, and time.Time, and returns 0 when types differ or are unsupported
// (`pkg/cel/helpers.go:369-408`).
func compareValues(a, b any) int {
	if result, ok := compareSameType(a, b); ok {
		return result
	}
	if result, ok := compareNumericFallbacks(a, b); ok {
		return result
	}
	return 0
}

func compareSameType(a, b any) (int, bool) {
	switch va := a.(type) {
	case string:
		if vb, ok := b.(string); ok {
			return strings.Compare(va, vb), true
		}
	case int64:
		if vb, ok := b.(int64); ok {
			switch {
			case va < vb:
				return -1, true
			case va > vb:
				return 1, true
			default:
				return 0, true
			}
		}
	case float64:
		if vb, ok := b.(float64); ok {
			switch {
			case va < vb:
				return -1, true
			case va > vb:
				return 1, true
			default:
				return 0, true
			}
		}
	case time.Time:
		if vb, ok := b.(time.Time); ok {
			return va.Compare(vb), true
		}
	}

	return 0, false
}

func compareNumericFallbacks(a, b any) (int, bool) {
	aNum, ok := toNumber(a)
	if !ok {
		return 0, false
	}
	bNum, ok := toNumber(b)
	if !ok {
		return 0, false
	}
	switch {
	case aNum < bNum:
		return -1, true
	case aNum > bNum:
		return 1, true
	default:
		return 0, true
	}
}

// toNumber coerces known Go numeric types (int64, float64, int) into a
// float64 result and reports whether the conversion succeeded
// (`pkg/cel/helpers.go:401-412`).
func toNumber(val any) (float64, bool) {
	switch v := val.(type) {
	case int64:
		return float64(v), true
	case float64:
		return v, true
	case int:
		return float64(v), true
	default:
		return 0, false
	}
}

// convertTimeFormat swaps a limited set of YYYY/MM/DD/HH/mm/ss tokens with Go's
// reference layout so formatTime can accept strftime-like patterns
// (`pkg/cel/helpers.go:465-485`).
func convertTimeFormat(format string) string {
	// Convert common time formats to Go's reference time format
	replacements := map[string]string{
		"YYYY": "2006",
		"MM":   "01",
		"DD":   "02",
		"HH":   "15",
		"mm":   "04",
		"ss":   "05",
	}

	result := format
	for pattern, replacement := range replacements {
		result = strings.ReplaceAll(result, pattern, replacement)
	}

	return result
}
