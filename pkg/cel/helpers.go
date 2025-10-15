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

	sort.Slice(sorted, func(i, j int) bool {
		valI := getFieldValue(sorted[i], field)
		valJ := getFieldValue(sorted[j], field)
		return compareValues(valI, valJ) < 0
	})

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

	for _, item := range list {
		key := fmt.Sprintf("%v", getFieldValue(item, field))
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
	for _, item := range list {
		val := getFieldValue(item, field)
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

	for _, item := range list {
		key := fmt.Sprintf("%v", getFieldValue(item, field))
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

func getFieldValue(item any, field string) any {
	if m, ok := item.(map[string]any); ok {
		return m[field]
	}
	return nil
}

func compareValues(a, b any) int {
	switch va := a.(type) {
	case string:
		if vb, ok := b.(string); ok {
			return strings.Compare(va, vb)
		}
	case int64:
		if vb, ok := b.(int64); ok {
			if va < vb {
				return -1
			} else if va > vb {
				return 1
			}
			return 0
		}
	case float64:
		if vb, ok := b.(float64); ok {
			if va < vb {
				return -1
			} else if va > vb {
				return 1
			}
			return 0
		}
	case time.Time:
		if vb, ok := b.(time.Time); ok {
			return va.Compare(vb)
		}
	}
	return 0
}

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
