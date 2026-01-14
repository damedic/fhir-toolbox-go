package fhirpath

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/cockroachdb/apd/v3"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

var fixedEvaluationInstant = time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC)

// testElement is a helper type for testing FHIRPath functions
type testElement struct {
	value any
}

// fakeFHIRPrimitive implements hasValuer by wrapping an Element.
type fakeFHIRPrimitive struct {
	Element
	hasValue bool
}

func (f fakeFHIRPrimitive) HasValue() bool {
	return f.hasValue
}

func (e testElement) Children(name ...string) Collection {
	if len(name) > 0 {
		if m, ok := e.value.(map[string]Collection); ok {
			if child, ok := m[name[0]]; ok {
				return child
			}
			return nil
		}
		return Collection{}
	}

	switch v := e.value.(type) {
	case []any:
		var result Collection
		for _, item := range v {
			if el, ok := item.(Element); ok {
				result = append(result, el)
			} else {
				// Convert primitives to FHIRPath types
				switch val := item.(type) {
				case int:
					result = append(result, Integer(val))
				case string:
					result = append(result, String(val))
				case bool:
					result = append(result, Boolean(val))
				case float64:
					result = append(result, Decimal{Value: apd.New(int64(val), 0)})
				default:
					// For other types, wrap in testElement
					result = append(result, testElement{value: val})
				}
			}
		}
		return result
	case []int:
		var result Collection
		for _, item := range v {
			result = append(result, Integer(item))
		}
		return result
	case []string:
		var result Collection
		for _, item := range v {
			result = append(result, String(item))
		}
		return result
	case []bool:
		var result Collection
		for _, item := range v {
			result = append(result, Boolean(item))
		}
		return result
	default:
		// For non-array values, return the value itself
		switch v := e.value.(type) {
		case int:
			return Collection{Integer(v)}
		case string:
			return Collection{String(v)}
		case bool:
			return Collection{Boolean(v)}
		case float64:
			return Collection{Decimal{Value: apd.New(int64(v), 0)}}
		default:
			return Collection{e}
		}
	}
}

func (e testElement) ToBoolean(explicit bool) (Boolean, bool, error) {
	switch v := e.value.(type) {
	case bool:
		return Boolean(v), true, nil
	case []bool:
		if len(v) == 0 {
			return false, false, nil
		}
		return Boolean(v[0]), true, nil
	default:
		return false, false, nil
	}
}

func (e testElement) ToString(explicit bool) (String, bool, error) {
	switch v := e.value.(type) {
	case string:
		return String(v), true, nil
	case []string:
		if len(v) == 0 {
			return "", false, nil
		}
		return String(v[0]), true, nil
	default:
		return "", false, nil
	}
}

func (e testElement) ToInteger(explicit bool) (Integer, bool, error) {
	switch v := e.value.(type) {
	case int:
		return Integer(v), true, nil
	case []int:
		if len(v) == 0 {
			return 0, false, nil
		}
		return Integer(v[0]), true, nil
	default:
		return 0, false, nil
	}
}
func (e testElement) ToLong(explicit bool) (Long, bool, error) {
	switch v := e.value.(type) {
	case int:
		return Long(v), true, nil
	case []int:
		if len(v) == 0 {
			return 0, false, nil
		}
		return Long(v[0]), true, nil
	case Long:
		return v, true, nil
	case []Long:
		if len(v) == 0 {
			return 0, false, nil
		}
		return v[0], true, nil
	default:
		return 0, false, nil
	}
}

func (e testElement) ToDecimal(explicit bool) (Decimal, bool, error) {
	switch v := e.value.(type) {
	case Decimal:
		return v, true, nil
	case []Decimal:
		if len(v) == 0 {
			return Decimal{}, false, nil
		}
		return v[0], true, nil
	default:
		return Decimal{}, false, nil
	}
}

func (e testElement) ToDate(explicit bool) (Date, bool, error) {
	switch v := e.value.(type) {
	case Date:
		return v, true, nil
	case []Date:
		if len(v) == 0 {
			return Date{}, false, nil
		}
		return v[0], true, nil
	default:
		return Date{}, false, nil
	}
}

func (e testElement) ToTime(explicit bool) (Time, bool, error) {
	switch v := e.value.(type) {
	case Time:
		return v, true, nil
	case []Time:
		if len(v) == 0 {
			return Time{}, false, nil
		}
		return v[0], true, nil
	default:
		return Time{}, false, nil
	}
}

func (e testElement) ToDateTime(explicit bool) (DateTime, bool, error) {
	switch v := e.value.(type) {
	case DateTime:
		return v, true, nil
	case []DateTime:
		if len(v) == 0 {
			return DateTime{}, false, nil
		}
		return v[0], true, nil
	default:
		return DateTime{}, false, nil
	}
}

func (e testElement) ToQuantity(explicit bool) (Quantity, bool, error) {
	switch v := e.value.(type) {
	case Quantity:
		return v, true, nil
	case []Quantity:
		if len(v) == 0 {
			return Quantity{}, false, nil
		}
		return v[0], true, nil
	default:
		return Quantity{}, false, nil
	}
}

func (e testElement) Equal(other Element) (bool, bool) {
	o, ok := other.(testElement)
	if !ok {
		return false, false
	}
	return reflect.DeepEqual(e.value, o.value), true
}

func (e testElement) Equivalent(other Element) bool {
	eq, _ := e.Equal(other)
	return eq
}

func (e testElement) TypeInfo() TypeInfo {
	return ClassInfo{
		Namespace: "System",
		Name:      "testElement",
		BaseType:  TypeSpecifier{Namespace: "System", Name: "Any"},
		Element: []ClassInfoElement{
			{Name: "value", Type: TypeSpecifier{Namespace: "System", Name: "Any"}},
		},
	}
}

func (e testElement) MarshalJSON() ([]byte, error) {
	return nil, nil
}

func (e testElement) String() string {
	return ""
}

// testFunction is a helper to test FHIRPath functions directly
func testFunction(t *testing.T, fn Function, target Collection, params []Expression, expected Collection, expectedOrdered bool, mockTime bool) {
	t.Helper()
	ctx := context.Background()
	// Set APD context with precision
	apdCtx := apd.BaseContext.WithPrecision(20)
	ctx = WithAPDContext(ctx, apdCtx)
	ctx = WithEvaluationTime(ctx, fixedEvaluationInstant)
	ctx = withEvaluationInstant(ctx)
	root := testElement{value: nil}

	// Mock evaluate function that can handle simple expressions
	mockEvaluate := func(ctx context.Context, target Collection, expr Expression, scope *FunctionScope) (Collection, bool, error) {
		if expr.tree == nil {
			return Collection{}, false, fmt.Errorf("unexpected expression <nil>")
		}

		var this Element
		if len(target) == 1 {
			this = target[0]
		}

		// Handle simple expressions
		switch expr.String() {
		case "true":
			return Collection{Boolean(true)}, true, nil
		case "false":
			return Collection{Boolean(false)}, true, nil
		case "$this > 0", "$this>0":
			switch v := this.(type) {
			case testElement:
				if val, ok := v.value.(int); ok {
					return Collection{Boolean(val > 0)}, true, nil
				}
			case Integer:
				return Collection{Boolean(int(v) > 0)}, true, nil
			}
			return Collection{Boolean(false)}, true, nil
		case "$this > 2", "$this>2":
			switch v := this.(type) {
			case testElement:
				if val, ok := v.value.(int); ok {
					return Collection{Boolean(val > 2)}, true, nil
				}
			case Integer:
				return Collection{Boolean(int(v) > 2)}, true, nil
			}
			return Collection{Boolean(false)}, true, nil
		case "$this > 5", "$this>5":
			switch v := this.(type) {
			case testElement:
				if val, ok := v.value.(int); ok {
					return Collection{Boolean(val > 5)}, true, nil
				}
			case Integer:
				return Collection{Boolean(int(v) > 5)}, true, nil
			}
			return Collection{Boolean(false)}, true, nil
		case "$this * 2", "$this*2":
			switch v := this.(type) {
			case testElement:
				if val, ok := v.value.(int); ok {
					return Collection{Integer(val * 2)}, true, nil
				}
			case Integer:
				return Collection{Integer(int(v) * 2)}, true, nil
			}
			return Collection{}, true, nil
		case "1":
			return Collection{Integer(1)}, true, nil
		case "2":
			return Collection{Integer(2)}, true, nil
		case "3":
			return Collection{Integer(3)}, true, nil
		case "4":
			return Collection{Integer(4)}, true, nil
		case "children()":
			return Collection{}, true, nil
		case "'test'":
			return Collection{String("test")}, true, nil
		case "'http://example.com/ext'":
			return Collection{String("http://example.com/ext")}, true, nil
		case "'hello'":
			return Collection{String("hello")}, true, nil
		case "'world'":
			return Collection{String("world")}, true, nil
		case "'hi'":
			return Collection{String("hi")}, true, nil
		case "decrement()":
			switch v := this.(type) {
			case Integer:
				if v > 0 {
					return Collection{Integer(v - 1)}, true, nil
				}
			case testElement:
				if val, ok := v.value.(int); ok && val > 0 {
					return Collection{Integer(val - 1)}, true, nil
				}
			}
			return Collection{}, true, nil
		case "boom()":
			return nil, false, fmt.Errorf("boom() should not be evaluated")
		case "'^hello.*'":
			return Collection{String("^hello.*")}, true, nil
		case "','":
			return Collection{String(",")}, true, nil
		case "{}":
			return Collection{}, true, nil
		case "{2, 3}":
			return Collection{Integer(2), Integer(3)}, true, nil
		case "{2, 3, 4}":
			return Collection{Integer(2), Integer(3), Integer(4)}, true, nil
		case "{3, 4, 5}":
			return Collection{Integer(3), Integer(4), Integer(5)}, true, nil
		case "2 | 3", "2|3":
			return Collection{Integer(2), Integer(3)}, true, nil
		case "2 | 3 | 4", "2|3|4":
			return Collection{Integer(2), Integer(3), Integer(4)}, true, nil
		case "3|4|5", "3 | 4 | 5":
			return Collection{Integer(3), Integer(4), Integer(5)}, true, nil
		default:
			// Try to parse as integer literal
			var intVal int
			if _, err := fmt.Sscanf(expr.String(), "%d", &intVal); err == nil {
				return Collection{Integer(intVal)}, true, nil
			}
			// Try to parse as boolean literal
			if expr.String() == "true" {
				return Collection{Boolean(true)}, true, nil
			}
			if expr.String() == "false" {
				return Collection{Boolean(false)}, true, nil
			}
			return Collection{}, false, fmt.Errorf("evaluate not implemented for expression: %s", expr.String())
		}
	}

	if fn == nil {
		t.Fatal("Function is nil")
	}

	result, ordered, err := fn(ctx, root, target, true, params, mockEvaluate)

	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if ordered != expectedOrdered {
		t.Errorf("Expected ordered=%v, got ordered=%v", expectedOrdered, ordered)
	}

	eq := result.Equivalent(expected)

	if !eq {
		t.Errorf("Expected %v, got %v", expected, result)
	}
}

func runFunctionWithEval(t *testing.T, fn Function, target Collection, params []Expression) (Collection, bool) {
	t.Helper()
	ctx := context.Background()
	apdCtx := apd.BaseContext.WithPrecision(20)
	ctx = WithAPDContext(ctx, apdCtx)
	ctx = WithEvaluationTime(ctx, fixedEvaluationInstant)
	ctx = withEvaluationInstant(ctx)
	root := testElement{}

	evaluate := func(ctx context.Context, target Collection, expr Expression, scope *FunctionScope) (Collection, bool, error) {
		if expr.tree == nil {
			return nil, false, fmt.Errorf("unexpected expression <nil>")
		}

		if scope != nil {
			fnScope := functionScope{
				index: scope.index,
			}
			if len(target) == 1 {
				fnScope.this = target[0]
			}
			ctx = withFunctionScope(ctx, fnScope)
		}

		return evalExpression(ctx, root, target, true, expr.tree, true)
	}

	result, ordered, err := fn(ctx, root, target, true, params, evaluate)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	return result, ordered
}

func TestExistenceFunctions(t *testing.T) {
	tests := []struct {
		name     string
		fn       Function
		target   Collection
		params   []Expression
		expected Collection
	}{
		{
			name:     "empty()",
			fn:       defaultFunctions["empty"],
			target:   Collection{},
			params:   nil,
			expected: Collection{Boolean(true)},
		},
		{
			name:     "empty() with non-empty collection",
			fn:       defaultFunctions["empty"],
			target:   Collection{Integer(1)},
			params:   nil,
			expected: Collection{Boolean(false)},
		},
		{
			name:     "exists()",
			fn:       defaultFunctions["exists"],
			target:   Collection{Integer(1)},
			params:   nil,
			expected: Collection{Boolean(true)},
		},
		{
			name:     "exists() with empty collection",
			fn:       defaultFunctions["exists"],
			target:   Collection{},
			params:   nil,
			expected: Collection{Boolean(false)},
		},
		{
			name:     "all() with empty collection",
			fn:       defaultFunctions["allTrue"],
			target:   Collection{},
			params:   nil,
			expected: Collection{Boolean(true)},
		},
		{
			name:     "all() with all true",
			fn:       defaultFunctions["allTrue"],
			target:   Collection{Boolean(true), Boolean(true)},
			params:   nil,
			expected: Collection{Boolean(true)},
		},
		{
			name:     "all() with some false",
			fn:       defaultFunctions["allTrue"],
			target:   Collection{Boolean(true), Boolean(false)},
			params:   nil,
			expected: Collection{Boolean(false)},
		},
		{
			name:     "any() with empty collection",
			fn:       defaultFunctions["anyTrue"],
			target:   Collection{},
			params:   nil,
			expected: Collection{Boolean(false)},
		},
		{
			name:     "any() with some true",
			fn:       defaultFunctions["anyTrue"],
			target:   Collection{Boolean(false), Boolean(true)},
			params:   nil,
			expected: Collection{Boolean(true)},
		},
		{
			name:     "any() with all false",
			fn:       defaultFunctions["anyTrue"],
			target:   Collection{Boolean(false), Boolean(false)},
			params:   nil,
			expected: Collection{Boolean(false)},
		},
		{
			name:     "allFalse() with empty collection",
			fn:       defaultFunctions["allFalse"],
			target:   Collection{},
			params:   nil,
			expected: Collection{Boolean(true)},
		},
		{
			name:     "allFalse() with all false",
			fn:       defaultFunctions["allFalse"],
			target:   Collection{Boolean(false), Boolean(false)},
			params:   nil,
			expected: Collection{Boolean(true)},
		},
		{
			name:     "allFalse() with some true",
			fn:       defaultFunctions["allFalse"],
			target:   Collection{Boolean(false), Boolean(true)},
			params:   nil,
			expected: Collection{Boolean(false)},
		},
		{
			name:     "anyFalse() with empty collection",
			fn:       defaultFunctions["anyFalse"],
			target:   Collection{},
			params:   nil,
			expected: Collection{Boolean(false)},
		},
		{
			name:     "anyFalse() with some false",
			fn:       defaultFunctions["anyFalse"],
			target:   Collection{Boolean(true), Boolean(false)},
			params:   nil,
			expected: Collection{Boolean(true)},
		},
		{
			name:     "anyFalse() with all true",
			fn:       defaultFunctions["anyFalse"],
			target:   Collection{Boolean(true), Boolean(true)},
			params:   nil,
			expected: Collection{Boolean(false)},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testFunction(t, tt.fn, tt.target, tt.params, tt.expected, true, false)
		})
	}
}

func TestFilteringAndProjectionFunctions(t *testing.T) {
	tests := []struct {
		name            string
		fn              Function
		target          Collection
		params          []Expression
		expected        Collection
		expectedOrdered bool
	}{
		{
			name:            "where()",
			fn:              defaultFunctions["where"],
			target:          Collection{Integer(1), Integer(2), Integer(3), Integer(4)},
			params:          []Expression{MustParse("$this > 2")},
			expected:        Collection{Integer(3), Integer(4)},
			expectedOrdered: true,
		},
		{
			name:            "select()",
			fn:              defaultFunctions["select"],
			target:          Collection{Integer(1), Integer(2), Integer(3)},
			params:          []Expression{MustParse("$this * 2")},
			expected:        Collection{Integer(2), Integer(4), Integer(6)},
			expectedOrdered: true,
		},
		{
			name:            "repeat()",
			fn:              defaultFunctions["repeat"],
			target:          Collection{String("test")},
			params:          []Expression{MustParse("children()")},
			expected:        Collection{},
			expectedOrdered: false,
		},
		{
			name:            "repeatAll()",
			fn:              defaultFunctions["repeatAll"],
			target:          Collection{Integer(2)},
			params:          []Expression{MustParse("decrement()")},
			expected:        Collection{Integer(1), Integer(0)},
			expectedOrdered: false,
		},
		{
			name:            "ofType()",
			fn:              defaultFunctions["ofType"],
			target:          Collection{String("test"), Integer(1)},
			params:          []Expression{MustParse("System.String")},
			expected:        Collection{String("test")},
			expectedOrdered: true,
		},
		{
			name:            "where() with empty result",
			fn:              defaultFunctions["where"],
			target:          Collection{Integer(1), Integer(2)},
			params:          []Expression{MustParse("$this > 5")},
			expected:        Collection{},
			expectedOrdered: true,
		},
		{
			name:            "select() with empty collection",
			fn:              defaultFunctions["select"],
			target:          Collection{},
			params:          []Expression{MustParse("$this * 2")},
			expected:        Collection{},
			expectedOrdered: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testFunction(t, tt.fn, tt.target, tt.params, tt.expected, tt.expectedOrdered, false)
		})
	}
}

func TestSortFunction(t *testing.T) {
	fn := defaultFunctions["sort"]

	descExpr := MustParse("$this")
	descExpr.sortDirection = sortDirectionDesc

	parityExpr := MustParse("$this mod 2")
	parityExpr.sortDirection = sortDirectionDesc

	tests := []struct {
		name            string
		target          Collection
		params          []Expression
		expected        Collection
		expectedOrdered bool
	}{
		{
			name:            "default numeric ordering",
			target:          Collection{Integer(3), Integer(1), Integer(2)},
			params:          nil,
			expected:        Collection{Integer(1), Integer(2), Integer(3)},
			expectedOrdered: true,
		},
		{
			name:            "explicit descending ordering",
			target:          Collection{Integer(1), Integer(3), Integer(2)},
			params:          []Expression{descExpr},
			expected:        Collection{Integer(3), Integer(2), Integer(1)},
			expectedOrdered: true,
		},
		{
			name:            "multi-key ordering",
			target:          Collection{Integer(2), Integer(4), Integer(3), Integer(1)},
			params:          []Expression{parityExpr, MustParse("$this")},
			expected:        Collection{Integer(1), Integer(3), Integer(2), Integer(4)},
			expectedOrdered: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, ordered := runFunctionWithEval(t, fn, tt.target, tt.params)

			if ordered != tt.expectedOrdered {
				t.Fatalf("expected ordered=%v, got %v", tt.expectedOrdered, ordered)
			}

			if !result.Equivalent(tt.expected) {
				t.Fatalf("expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestSubsettingFunctions(t *testing.T) {
	tests := []struct {
		name            string
		fn              Function
		target          Collection
		params          []Expression
		expected        Collection
		expectedOrdered bool
	}{
		{
			name:            "first()",
			fn:              defaultFunctions["first"],
			target:          Collection{Integer(1), Integer(2), Integer(3)},
			params:          nil,
			expected:        Collection{Integer(1)},
			expectedOrdered: true,
		},
		{
			name:            "last()",
			fn:              defaultFunctions["last"],
			target:          Collection{Integer(1), Integer(2), Integer(3)},
			params:          nil,
			expected:        Collection{Integer(3)},
			expectedOrdered: true,
		},
		{
			name:            "tail()",
			fn:              defaultFunctions["tail"],
			target:          Collection{Integer(1), Integer(2), Integer(3)},
			params:          nil,
			expected:        Collection{Integer(2), Integer(3)},
			expectedOrdered: true,
		},
		{
			name:            "skip()",
			fn:              defaultFunctions["skip"],
			target:          Collection{Integer(1), Integer(2), Integer(3), Integer(4)},
			params:          []Expression{MustParse("2")},
			expected:        Collection{Integer(3), Integer(4)},
			expectedOrdered: true,
		},
		{
			name:            "take()",
			fn:              defaultFunctions["take"],
			target:          Collection{Integer(1), Integer(2), Integer(3), Integer(4)},
			params:          []Expression{MustParse("2")},
			expected:        Collection{Integer(1), Integer(2)},
			expectedOrdered: true,
		},
		{
			name:            "intersect()",
			fn:              defaultFunctions["intersect"],
			target:          Collection{Integer(1), Integer(2), Integer(3)},
			params:          []Expression{MustParse("2 | 3 | 4")},
			expected:        Collection{Integer(2), Integer(3)},
			expectedOrdered: false,
		},
		{
			name:            "exclude()",
			fn:              defaultFunctions["exclude"],
			target:          Collection{Integer(1), Integer(2), Integer(3), Integer(4)},
			params:          []Expression{MustParse("2 | 3")},
			expected:        Collection{Integer(1), Integer(4)},
			expectedOrdered: true,
		},
		{
			name:            "first() with empty collection",
			fn:              defaultFunctions["first"],
			target:          Collection{},
			params:          nil,
			expected:        Collection{},
			expectedOrdered: true,
		},
		{
			name:            "last() with empty collection",
			fn:              defaultFunctions["last"],
			target:          Collection{},
			params:          nil,
			expected:        Collection{},
			expectedOrdered: true,
		},
		{
			name:            "tail() with empty collection",
			fn:              defaultFunctions["tail"],
			target:          Collection{},
			params:          nil,
			expected:        Collection{},
			expectedOrdered: true,
		},
		{
			name:            "skip() with empty collection",
			fn:              defaultFunctions["skip"],
			target:          Collection{},
			params:          []Expression{MustParse("2")},
			expected:        Collection{},
			expectedOrdered: true,
		},
		{
			name:            "take() with empty collection",
			fn:              defaultFunctions["take"],
			target:          Collection{},
			params:          []Expression{MustParse("2")},
			expected:        Collection{},
			expectedOrdered: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testFunction(t, tt.fn, tt.target, tt.params, tt.expected, tt.expectedOrdered, false)
		})
	}
}

func TestCombiningFunctions(t *testing.T) {
	tests := []struct {
		name            string
		fn              Function
		target          Collection
		params          []Expression
		expected        Collection
		expectedOrdered bool
	}{
		{
			name:            "union()",
			fn:              defaultFunctions["union"],
			target:          Collection{Integer(1), Integer(2), Integer(3)},
			params:          []Expression{MustParse("3|4|5")},
			expected:        Collection{Integer(1), Integer(2), Integer(3), Integer(4), Integer(5)},
			expectedOrdered: false,
		},
		{
			name:            "combine()",
			fn:              defaultFunctions["combine"],
			target:          Collection{Integer(1), Integer(2), Integer(3)},
			params:          []Expression{MustParse("3|4|5")},
			expected:        Collection{Integer(1), Integer(2), Integer(3), Integer(3), Integer(4), Integer(5)},
			expectedOrdered: false,
		},
		{
			name:            "union() with empty collections",
			fn:              defaultFunctions["union"],
			target:          Collection{},
			params:          []Expression{MustParse("{}")},
			expected:        Collection{},
			expectedOrdered: false,
		},
		{
			name:            "combine() with empty collections",
			fn:              defaultFunctions["combine"],
			target:          Collection{},
			params:          []Expression{MustParse("{}")},
			expected:        Collection{},
			expectedOrdered: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testFunction(t, tt.fn, tt.target, tt.params, tt.expected, tt.expectedOrdered, false)
		})
	}
}

func TestCoalesceFunction(t *testing.T) {
	tests := []struct {
		name     string
		target   Collection
		params   []Expression
		expected Collection
	}{
		{
			name:     "returns first non-empty argument",
			target:   nil,
			params:   []Expression{MustParse("'test'"), MustParse("'hello'")},
			expected: Collection{String("test")},
		},
		{
			name:     "skips leading empty collections",
			target:   nil,
			params:   []Expression{MustParse("{}"), MustParse("'hello'")},
			expected: Collection{String("hello")},
		},
		{
			name:     "all arguments empty",
			target:   nil,
			params:   []Expression{MustParse("{}"), MustParse("{}")},
			expected: Collection{},
		},
		{
			name:     "short-circuits after first non-empty",
			target:   nil,
			params:   []Expression{MustParse("'test'"), MustParse("boom()")},
			expected: Collection{String("test")},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testFunction(t, defaultFunctions["coalesce"], tt.target, tt.params, tt.expected, true, false)
		})
	}
}

func TestStringFunctions(t *testing.T) {
	tests := []struct {
		name     string
		fn       Function
		target   Collection
		params   []Expression
		expected Collection
	}{
		{
			name:     "indexOf()",
			fn:       defaultFunctions["indexOf"],
			target:   Collection{String("hello test world")},
			params:   []Expression{MustParse("'test'")},
			expected: Collection{Integer(6)},
		},
		{
			name:     "substring()",
			fn:       defaultFunctions["substring"],
			target:   Collection{String("hello test world")},
			params:   []Expression{MustParse("6"), MustParse("4")},
			expected: Collection{String("test")},
		},
		{
			name:     "substring() empty start argument propagates empty",
			fn:       defaultFunctions["substring"],
			target:   Collection{String("hello")},
			params:   []Expression{MustParse("{}")},
			expected: Collection{},
		},
		{
			name:     "substring() empty length behaves as omitted",
			fn:       defaultFunctions["substring"],
			target:   Collection{String("hello")},
			params:   []Expression{MustParse("1"), MustParse("{}")},
			expected: Collection{String("ello")},
		},
		{
			name:     "startsWith()",
			fn:       defaultFunctions["startsWith"],
			target:   Collection{String("hello world")},
			params:   []Expression{MustParse("'hello'")},
			expected: Collection{Boolean(true)},
		},
		{
			name:     "startsWith() empty prefix argument propagates empty",
			fn:       defaultFunctions["startsWith"],
			target:   Collection{String("hello world")},
			params:   []Expression{MustParse("{}")},
			expected: Collection{},
		},
		{
			name:     "endsWith()",
			fn:       defaultFunctions["endsWith"],
			target:   Collection{String("hello world")},
			params:   []Expression{MustParse("'world'")},
			expected: Collection{Boolean(true)},
		},
		{
			name:     "endsWith() empty suffix argument propagates empty",
			fn:       defaultFunctions["endsWith"],
			target:   Collection{String("hello world")},
			params:   []Expression{MustParse("{}")},
			expected: Collection{},
		},
		{
			name:     "contains()",
			fn:       defaultFunctions["contains"],
			target:   Collection{String("hello test world")},
			params:   []Expression{MustParse("'test'")},
			expected: Collection{Boolean(true)},
		},
		{
			name:     "contains() empty substring argument propagates empty",
			fn:       defaultFunctions["contains"],
			target:   Collection{String("hello test world")},
			params:   []Expression{MustParse("{}")},
			expected: Collection{},
		},
		{
			name:     "upper()",
			fn:       defaultFunctions["upper"],
			target:   Collection{String("hello")},
			params:   nil,
			expected: Collection{String("HELLO")},
		},
		{
			name:     "lower()",
			fn:       defaultFunctions["lower"],
			target:   Collection{String("HELLO")},
			params:   nil,
			expected: Collection{String("hello")},
		},
		{
			name:     "replace()",
			fn:       defaultFunctions["replace"],
			target:   Collection{String("hello world")},
			params:   []Expression{MustParse("'hello'"), MustParse("'hi'")},
			expected: Collection{String("hi world")},
		},
		{
			name:     "matches()",
			fn:       defaultFunctions["matches"],
			target:   Collection{String("hello world")},
			params:   []Expression{MustParse("'^hello.*'")},
			expected: Collection{Boolean(true)},
		},
		{
			name:     "length()",
			fn:       defaultFunctions["length"],
			target:   Collection{String("hello")},
			params:   nil,
			expected: Collection{Integer(5)},
		},
		{
			name:     "toChars()",
			fn:       defaultFunctions["toChars"],
			target:   Collection{String("hi")},
			params:   nil,
			expected: Collection{String("h"), String("i")},
		},
		{
			name:     "trim()",
			fn:       defaultFunctions["trim"],
			target:   Collection{String("  hello  ")},
			params:   nil,
			expected: Collection{String("hello")},
		},
		{
			name:     "split()",
			fn:       defaultFunctions["split"],
			target:   Collection{String("hello,world")},
			params:   []Expression{MustParse("','")},
			expected: Collection{String("hello"), String("world")},
		},
		{
			name:     "join()",
			fn:       defaultFunctions["join"],
			target:   Collection{String("hello"), String("world")},
			params:   []Expression{MustParse("','")},
			expected: Collection{String("hello,world")},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testFunction(t, tt.fn, tt.target, tt.params, tt.expected, true, false)
		})
	}
}

func TestMathFunctions(t *testing.T) {
	tests := []struct {
		name     string
		fn       Function
		target   Collection
		params   []Expression
		expected Collection
	}{
		{
			name:     "abs()",
			fn:       defaultFunctions["abs"],
			target:   Collection{Integer(-5)},
			params:   nil,
			expected: Collection{Integer(5)},
		},
		{
			name:     "ceiling()",
			fn:       defaultFunctions["ceiling"],
			target:   Collection{Decimal{Value: apd.New(5, -1)}}, // 0.5
			params:   nil,
			expected: Collection{Integer(1)},
		},
		{
			name:     "exp()",
			fn:       defaultFunctions["exp"],
			target:   Collection{Decimal{Value: apd.New(1, 0)}}, // 1
			params:   nil,
			expected: Collection{Decimal{Value: apd.New(2718281828, -9)}}, // e ≈ 2.718281828
		},
		{
			name:     "floor()",
			fn:       defaultFunctions["floor"],
			target:   Collection{Decimal{Value: apd.New(5, -1)}}, // 0.5
			params:   nil,
			expected: Collection{Integer(0)},
		},
		{
			name:     "ln()",
			fn:       defaultFunctions["ln"],
			target:   Collection{Decimal{Value: apd.New(1, 0)}}, // 1
			params:   nil,
			expected: Collection{Decimal{Value: apd.New(0, 0)}}, // ln(1) = 0
		},
		{
			name:     "log()",
			fn:       defaultFunctions["log"],
			target:   Collection{Decimal{Value: apd.New(100, 0)}}, // 100
			params:   []Expression{MustParse("10")},
			expected: Collection{Decimal{Value: apd.New(2, 0)}}, // log10(100) = 2
		},
		{
			name:     "power()",
			fn:       defaultFunctions["power"],
			target:   Collection{Decimal{Value: apd.New(3, 0)}}, // 3
			params:   []Expression{MustParse("2")},
			expected: Collection{Decimal{Value: apd.New(9, 0)}}, // 3^2 = 9
		},
		{
			name:     "round()",
			fn:       defaultFunctions["round"],
			target:   Collection{Decimal{Value: apd.New(5, -1)}}, // 0.5
			params:   nil,
			expected: Collection{Decimal{Value: apd.New(1, 0)}},
		},
		{
			name:     "sqrt()",
			fn:       defaultFunctions["sqrt"],
			target:   Collection{Decimal{Value: apd.New(4, 0)}}, // 4
			params:   nil,
			expected: Collection{Decimal{Value: apd.New(2, 0)}}, // √4 = 2
		},
		{
			name:     "truncate()",
			fn:       defaultFunctions["truncate"],
			target:   Collection{Decimal{Value: apd.New(5, -1)}}, // 0.5
			params:   nil,
			expected: Collection{Integer(0)},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testFunction(t, tt.fn, tt.target, tt.params, tt.expected, true, false)
		})
	}
}

func TestTreeNavigationFunctions(t *testing.T) {
	tests := []struct {
		name            string
		fn              Function
		target          Collection
		params          []Expression
		expected        Collection
		expectedOrdered bool
	}{
		{
			name:            "children()",
			fn:              defaultFunctions["children"],
			target:          Collection{testElement{value: []any{}}},
			params:          nil,
			expected:        Collection{},
			expectedOrdered: false,
		},
		{
			name:            "descendants()",
			fn:              defaultFunctions["descendants"],
			target:          Collection{testElement{value: []any{}}},
			params:          nil,
			expected:        Collection{},
			expectedOrdered: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testFunction(t, tt.fn, tt.target, tt.params, tt.expected, tt.expectedOrdered, false)
		})
	}
}

func TestUtilityFunctions(t *testing.T) {
	tests := []struct {
		name            string
		fn              Function
		target          Collection
		params          []Expression
		expected        Collection
		expectedOrdered bool
	}{
		{
			name:            "trace()",
			fn:              defaultFunctions["trace"],
			target:          Collection{String("value")},
			params:          []Expression{MustParse("'test'")},
			expected:        Collection{String("value")},
			expectedOrdered: true,
		},
		{
			name:            "now()",
			fn:              defaultFunctions["now"],
			target:          Collection{},
			params:          nil,
			expected:        Collection{DateTime{Value: time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC), Precision: DateTimePrecisionFull, HasTimeZone: true}},
			expectedOrdered: true,
		},
		{
			name:            "timeOfDay()",
			fn:              defaultFunctions["timeOfDay"],
			target:          Collection{},
			params:          nil,
			expected:        Collection{Time{Value: time.Date(0, 1, 1, 3, 4, 5, 0, time.UTC), Precision: TimePrecisionFull}},
			expectedOrdered: true,
		},
		{
			name:            "today()",
			fn:              defaultFunctions["today"],
			target:          Collection{},
			params:          nil,
			expected:        Collection{Date{Value: time.Date(2020, 1, 2, 0, 0, 0, 0, time.UTC), Precision: DatePrecisionFull}},
			expectedOrdered: true,
		},
		{
			name: "extension()",
			fn:   FHIRFunctions["extension"],
			target: Collection{testElement{value: map[string]Collection{
				"extension": Collection{
					testElement{value: map[string]Collection{
						"url": Collection{String("http://example.com/ext")},
					}},
					testElement{value: map[string]Collection{
						"url": Collection{String("http://example.com/other")},
					}},
				},
			}}},
			params: []Expression{MustParse("'http://example.com/ext'")},
			expected: Collection{
				testElement{value: map[string]Collection{
					"url": Collection{String("http://example.com/ext")},
				}},
			},
			expectedOrdered: true,
		},
		{
			name:            "hasValue() true for primitive with value",
			fn:              FHIRFunctions["hasValue"],
			target:          Collection{fakeFHIRPrimitive{Element: String("abc"), hasValue: true}},
			params:          nil,
			expected:        Collection{Boolean(true)},
			expectedOrdered: true,
		},
		{
			name:            "hasValue() false for primitive without value",
			fn:              FHIRFunctions["hasValue"],
			target:          Collection{fakeFHIRPrimitive{Element: String("abc"), hasValue: false}},
			params:          nil,
			expected:        Collection{Boolean(false)},
			expectedOrdered: true,
		},
		{
			name:            "hasValue() empty for non FHIR primitive",
			fn:              FHIRFunctions["hasValue"],
			target:          Collection{String("abc")},
			params:          nil,
			expected:        Collection{},
			expectedOrdered: true,
		},
		{
			name:            "getValue() unwraps FHIR primitive",
			fn:              FHIRFunctions["getValue"],
			target:          Collection{fakeFHIRPrimitive{Element: String("abc"), hasValue: true}},
			params:          nil,
			expected:        Collection{String("abc")},
			expectedOrdered: true,
		},
		{
			name:            "getValue() empty when primitive lacks value",
			fn:              FHIRFunctions["getValue"],
			target:          Collection{fakeFHIRPrimitive{Element: String("abc"), hasValue: false}},
			params:          nil,
			expected:        Collection{},
			expectedOrdered: true,
		},
		{
			name:            "getValue() empty for non FHIR primitive",
			fn:              FHIRFunctions["getValue"],
			target:          Collection{String("abc")},
			params:          nil,
			expected:        Collection{},
			expectedOrdered: true,
		},
		{
			name:            "isDistinct()",
			fn:              defaultFunctions["isDistinct"],
			target:          Collection{Integer(1), Integer(2), Integer(3)},
			params:          nil,
			expected:        Collection{Boolean(true)},
			expectedOrdered: true,
		},
		{
			name:            "distinct()",
			fn:              defaultFunctions["distinct"],
			target:          Collection{Integer(1), Integer(2), Integer(2), Integer(3)},
			params:          nil,
			expected:        Collection{Integer(1), Integer(2), Integer(3)},
			expectedOrdered: false,
		},
		{
			name:            "count()",
			fn:              defaultFunctions["count"],
			target:          Collection{Integer(1), Integer(2), Integer(3)},
			params:          nil,
			expected:        Collection{Integer(3)},
			expectedOrdered: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testFunction(t, tt.fn, tt.target, tt.params, tt.expected, tt.expectedOrdered, false)
		})
	}
}

func TestTemporalFunctionsDeterministic(t *testing.T) {
	ctx := context.Background()
	apdCtx := apd.BaseContext.WithPrecision(20)
	ctx = WithAPDContext(ctx, apdCtx)
	ctx = WithEvaluationTime(ctx, fixedEvaluationInstant)
	ctx = withEvaluationInstant(ctx)
	root := testElement{}
	noEval := func(ctx context.Context, target Collection, expr Expression, scope *FunctionScope) (Collection, bool, error) {
		return nil, false, fmt.Errorf("unexpected evaluation")
	}

	call := func(fn Function) Collection {
		result, _, err := fn(ctx, root, Collection{}, true, nil, noEval)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		return result
	}

	nowFirst := call(defaultFunctions["now"])
	nowSecond := call(defaultFunctions["now"])
	if !nowFirst.Equivalent(nowSecond) {
		t.Fatalf("now() results differ within same context: %v vs %v", nowFirst, nowSecond)
	}

	timeFirst := call(defaultFunctions["timeOfDay"])
	timeSecond := call(defaultFunctions["timeOfDay"])
	if !timeFirst.Equivalent(timeSecond) {
		t.Fatalf("timeOfDay() results differ within same context: %v vs %v", timeFirst, timeSecond)
	}
	timeValue, ok := timeFirst[0].(Time)
	if !ok {
		t.Fatalf("expected Time result, got %T", timeFirst[0])
	}
	if timeValue.Value.Year() != 0 || timeValue.Value.Month() != 1 || timeValue.Value.Day() != 1 {
		t.Fatalf("timeOfDay() should zero-out date component, got %v", timeValue.Value)
	}

	todayFirst := call(defaultFunctions["today"])
	todaySecond := call(defaultFunctions["today"])
	if !todayFirst.Equivalent(todaySecond) {
		t.Fatalf("today() results differ within same context: %v vs %v", todayFirst, todaySecond)
	}
	todayValue, ok := todayFirst[0].(Date)
	if !ok {
		t.Fatalf("expected Date result, got %T", todayFirst[0])
	}
	if todayValue.Value.Hour() != 0 || todayValue.Value.Minute() != 0 || todayValue.Value.Second() != 0 || todayValue.Value.Nanosecond() != 0 {
		t.Fatalf("today() should truncate to midnight, got %v", todayValue.Value)
	}
}

func TestPrecisionFunctionTemporalTypes(t *testing.T) {
	fn := defaultFunctions["precision"]
	tests := []struct {
		name     string
		target   Collection
		expected Collection
	}{
		{
			name:     "Date precision digits",
			target:   Collection{Date{Value: time.Date(2020, 5, 1, 0, 0, 0, 0, time.UTC), Precision: DatePrecisionMonth}},
			expected: Collection{Integer(6)},
		},
		{
			name: "DateTime precision digits",
			target: Collection{DateTime{
				Value:     time.Date(2020, 5, 1, 10, 30, 0, 0, time.UTC),
				Precision: DateTimePrecisionMillisecond,
			}},
			expected: Collection{Integer(17)},
		},
		{
			name: "Time precision digits",
			target: Collection{Time{
				Value:     time.Date(0, 1, 1, 10, 30, 0, 0, time.UTC),
				Precision: TimePrecisionMinute,
			}},
			expected: Collection{Integer(4)},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testFunction(t, fn, tt.target, nil, tt.expected, true, false)
		})
	}
}

func TestWithEvaluationTimeControlsTemporalFunctions(t *testing.T) {
	loc := time.FixedZone("UTC+5", 5*60*60)
	sourceInstant := time.Date(2024, 7, 3, 11, 22, 33, 987654321, loc)
	ctx := WithEvaluationTime(nil, sourceInstant)

	want := sourceInstant.Truncate(time.Millisecond)
	got := evaluationInstant(ctx)
	if !got.Equal(want) {
		t.Fatalf("evaluationInstant() = %v, want %v", got, want)
	}

	root := testElement{}

	call := func(fnName string) Collection {
		fn := defaultFunctions[fnName]
		result, _, err := fn(ctx, root, Collection{}, true, nil, nil)
		if err != nil {
			t.Fatalf("%s() unexpected error: %v", fnName, err)
		}
		return result
	}

	nowResult := call("now")
	dt, ok := nowResult[0].(DateTime)
	if !ok {
		t.Fatalf("now() result not DateTime: %T", nowResult[0])
	}
	if !dt.Value.Equal(want) {
		t.Fatalf("now() = %v, want %v", dt.Value, want)
	}
	if !dt.HasTimeZone {
		t.Fatalf("now() should include timezone information")
	}

	timeResult := call("timeOfDay")
	tod, ok := timeResult[0].(Time)
	if !ok {
		t.Fatalf("timeOfDay() result not Time: %T", timeResult[0])
	}
	if tod.Value.Hour() != want.Hour() || tod.Value.Minute() != want.Minute() || tod.Value.Second() != want.Second() {
		t.Fatalf("timeOfDay() = %v, want %v", tod.Value, want)
	}
	if tod.Value.Year() != 0 || tod.Value.Month() != 1 || tod.Value.Day() != 1 {
		t.Fatalf("timeOfDay() should zero out date component, got %v", tod.Value)
	}

	todayResult := call("today")
	day, ok := todayResult[0].(Date)
	if !ok {
		t.Fatalf("today() result not Date: %T", todayResult[0])
	}
	if day.Value.Year() != want.Year() || day.Value.Month() != want.Month() || day.Value.Day() != want.Day() {
		t.Fatalf("today() = %v, want date component %v", day.Value, want)
	}
	if day.Value.Hour() != 0 || day.Value.Minute() != 0 || day.Value.Second() != 0 || day.Value.Nanosecond() != 0 {
		t.Fatalf("today() should truncate time component, got %v", day.Value)
	}
}

func TestDefineVariable(t *testing.T) {
	tests := []struct {
		name        string
		target      Collection
		params      []Expression
		expected    Collection
		expectError bool
		varName     string
		varValue    Collection
	}{
		{
			name:        "define_variable_with_value",
			target:      Collection{},
			params:      []Expression{MustParse("'test'"), MustParse("'value'")},
			expected:    Collection{},
			expectError: false,
			varName:     "test",
			varValue:    Collection{String("value")},
		},
		{
			name:        "define_variable_using_input_collection",
			target:      Collection{String("inputValue")},
			params:      []Expression{MustParse("'myVar'")},
			expected:    Collection{String("inputValue")},
			expectError: false,
			varName:     "myVar",
			varValue:    Collection{String("inputValue")},
		},
		{
			name: "value_expression_evaluated_on_entire_collection",
			target: Collection{
				String("first"),
				String("second"),
				String("third"),
			},
			params: []Expression{
				MustParse("'n2'"),
				MustParse("skip(1).first()"),
			},
			expected: Collection{
				String("first"),
				String("second"),
				String("third"),
			},
			expectError: false,
			varName:     "n2",
			varValue:    Collection{String("second")},
		},
		{
			name:        "invalid_number_of_parameters",
			target:      Collection{},
			params:      []Expression{MustParse("'myVar'"), MustParse("'test'"), MustParse("'extra'")},
			expected:    nil,
			expectError: true,
		},
		{
			name:        "non_string_name_parameter",
			target:      Collection{},
			params:      []Expression{MustParse("123"), MustParse("'test'")},
			expected:    nil,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockEvaluate := func(ctx context.Context, target Collection, expr Expression, scope *FunctionScope) (Collection, bool, error) {
				if expr.tree == nil {
					return Collection{}, false, fmt.Errorf("unexpected expression <nil>")
				}
				switch expr.String() {
				case "'test'":
					return Collection{String("test")}, true, nil
				case "'myVar'":
					return Collection{String("myVar")}, true, nil
				case "'n2'":
					return Collection{String("n2")}, true, nil
				case "'testVar'":
					return Collection{String("testVar")}, true, nil
				case "'value'":
					return Collection{String("value")}, true, nil
				case "'invalid'":
					return Collection{String("invalid")}, true, nil
				case "'multiple'":
					return Collection{String("multiple")}, true, nil
				case "skip(1).first()":
					if len(target) <= 1 {
						return nil, true, nil
					}
					return Collection{target[1]}, true, nil
				default:
					return Collection{}, false, fmt.Errorf("evaluate not implemented for expression: %s", expr.String())
				}
			}
			ctx := context.Background()
			ctx, _ = withNewEnvStackFrame(ctx)
			result, _, err := defaultFunctions["defineVariable"](ctx, testElement{}, tt.target, true, tt.params, mockEvaluate)
			if tt.expectError {
				if err == nil {
					t.Errorf("Expected error but got none")
				}
				return
			} else if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}
			if !cmp.Equal(result, tt.expected, cmpopts.IgnoreUnexported(testElement{})) {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}

			// Verify variable was set in environment
			if !tt.expectError {
				val, exists := envValue(ctx, tt.varName)
				if !exists {
					t.Errorf("Variable %s was not set in environment", tt.varName)
				} else if !cmp.Equal(val, tt.varValue, cmpopts.IgnoreUnexported(testElement{})) {
					t.Errorf("Expected variable value %v, got %v", tt.varValue, val)
				}
			}
		})
	}
}

func TestTypeFunctions(t *testing.T) {
	tests := []struct {
		name     string
		fn       Function
		target   Collection
		params   []Expression
		expected Collection
	}{
		{
			name:     "is()",
			fn:       defaultFunctions["is"],
			target:   Collection{String("test")},
			params:   []Expression{MustParse("System.String")},
			expected: Collection{Boolean(true)},
		},
		{
			name:     "as()",
			fn:       defaultFunctions["as"],
			target:   Collection{String("test")},
			params:   []Expression{MustParse("System.String")},
			expected: Collection{String("test")},
		},
		{
			name:     "ofType()",
			fn:       defaultFunctions["ofType"],
			target:   Collection{String("test"), Integer(1)},
			params:   []Expression{MustParse("System.String")},
			expected: Collection{String("test")},
		},
		{
			name:   "type()",
			fn:     defaultFunctions["type"],
			target: Collection{String("test")},
			params: nil,
			expected: Collection{SimpleTypeInfo{
				Namespace: "System",
				Name:      "String",
				BaseType:  TypeSpecifier{Namespace: "System", Name: "Any"},
			}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testFunction(t, tt.fn, tt.target, tt.params, tt.expected, true, false)
		})
	}
}

func TestToLongFunction(t *testing.T) {
	fn := defaultFunctions["toLong"]
	testCases := []struct {
		name     string
		target   Collection
		expected Collection
	}{
		{
			name:     "integer input",
			target:   Collection{Integer(5)},
			expected: Collection{Long(5)},
		},
		{
			name:     "string input",
			target:   Collection{String("9223372036854775807")},
			expected: Collection{Long(9223372036854775807)},
		},
		{
			name:     "non-convertible",
			target:   Collection{String("abc")},
			expected: nil,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			testFunction(t, fn, tc.target, nil, tc.expected, true, false)
		})
	}
}

func TestConvertsToLongFunction(t *testing.T) {
	fn := defaultFunctions["convertsToLong"]
	testCases := []struct {
		name     string
		target   Collection
		expected Collection
	}{
		{
			name:     "convertible string",
			target:   Collection{String("123")},
			expected: Collection{Boolean(true)},
		},
		{
			name:     "non-convertible",
			target:   Collection{String("foo")},
			expected: Collection{Boolean(false)},
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			testFunction(t, fn, tc.target, nil, tc.expected, true, false)
		})
	}
}

func TestIIFWithThisContext(t *testing.T) {
	fn := defaultFunctions["iif"]

	testCases := []struct {
		name            string
		target          Collection
		params          []Expression
		expected        Collection
		expectedOrdered bool
	}{
		{
			name:            "iif with $this in criterion (boolean result)",
			target:          Collection{Boolean(true)},
			params:          []Expression{MustParse("$this"), MustParse("'true'"), MustParse("'false'")},
			expected:        Collection{String("true")},
			expectedOrdered: true,
		},
		{
			name:            "iif with $this in criterion (false)",
			target:          Collection{Boolean(false)},
			params:          []Expression{MustParse("$this"), MustParse("'true'"), MustParse("'false'")},
			expected:        Collection{String("false")},
			expectedOrdered: true,
		},
		{
			name:            "iif with $this in true-result branch",
			target:          Collection{Boolean(true)},
			params:          []Expression{MustParse("$this"), MustParse("$this.not()"), MustParse("'otherwise'")},
			expected:        Collection{Boolean(false)},
			expectedOrdered: true,
		},
		{
			name:            "iif with $this in otherwise-result branch",
			target:          Collection{Boolean(false)},
			params:          []Expression{MustParse("$this"), MustParse("'true'"), MustParse("$this.not()")},
			expected:        Collection{Boolean(true)},
			expectedOrdered: true,
		},
		{
			name:            "iif with empty string as criterion via $this (singleton eval rule)",
			target:          Collection{String("")},
			params:          []Expression{MustParse("$this"), MustParse("'true'"), MustParse("'false'")},
			expected:        Collection{String("true")},
			expectedOrdered: true,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			ctx = withEvaluationInstant(ctx)
			result, ordered, err := fn(ctx, nil, tc.target, true, tc.params, func(ctx context.Context, target Collection, expr Expression, scope *FunctionScope) (Collection, bool, error) {
				// Set up function scope if provided
				if scope != nil {
					fnScope := functionScope{
						index: scope.index,
					}
					if len(target) == 1 {
						fnScope.this = target[0]
					}
					ctx = withFunctionScope(ctx, fnScope)
				}
				// Determine evaluation target
				evalTarget := target
				if len(evalTarget) == 0 {
					if scope, ok := getFunctionScope(ctx); ok && scope.this != nil {
						evalTarget = Collection{scope.this}
					}
				}
				return evalExpression(ctx, nil, evalTarget, true, expr.tree, false)
			})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !reflect.DeepEqual(result, tc.expected) {
				t.Errorf("expected %v, got %v", tc.expected, result)
			}
			if ordered != tc.expectedOrdered {
				t.Errorf("expected ordered=%v, got %v", tc.expectedOrdered, ordered)
			}
		})
	}
}

func TestIIFWithNilElement(t *testing.T) {
	fn := defaultFunctions["iif"]

	testCases := []struct {
		name            string
		target          Collection
		params          []Expression
		expected        Collection
		expectedOrdered bool
		expectError     bool
	}{
		{
			name:            "iif with nil element in criterion should not panic",
			target:          Collection{nil},
			params:          []Expression{MustParse("$this"), MustParse("'true'"), MustParse("'false'")},
			expected:        Collection{String("false")},
			expectedOrdered: true,
			expectError:     false,
		},
		{
			name:            "iif with empty collection in criterion",
			target:          Collection{},
			params:          []Expression{MustParse("$this"), MustParse("'true'"), MustParse("'false'")},
			expected:        Collection{String("false")},
			expectedOrdered: true,
			expectError:     false,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			ctx = withEvaluationInstant(ctx)
			result, ordered, err := fn(ctx, nil, tc.target, true, tc.params, func(ctx context.Context, target Collection, expr Expression, scope *FunctionScope) (Collection, bool, error) {
				// Set up function scope if provided
				if scope != nil {
					fnScope := functionScope{
						index: scope.index,
					}
					if len(target) == 1 {
						fnScope.this = target[0]
					}
					ctx = withFunctionScope(ctx, fnScope)
				}
				// Determine evaluation target
				evalTarget := target
				if len(evalTarget) == 0 {
					if scope, ok := getFunctionScope(ctx); ok && scope.this != nil {
						evalTarget = Collection{scope.this}
					}
				}
				return evalExpression(ctx, nil, evalTarget, true, expr.tree, false)
			})

			if tc.expectError {
				if err == nil {
					t.Fatal("expected error but got none")
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !reflect.DeepEqual(result, tc.expected) {
				t.Errorf("expected %v, got %v", tc.expected, result)
			}
			if ordered != tc.expectedOrdered {
				t.Errorf("expected ordered=%v, got %v", tc.expectedOrdered, ordered)
			}
		})
	}
}
