package fhirpath

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"html"
	"maps"
	"math"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/cockroachdb/apd/v3"
)

// Tracer defines the interface for logging trace messages
type Tracer interface {
	// Log logs a trace message with the given name and collection
	Log(name string, collection Collection) error
}

// StdoutTracer writes traces to io.Stdout.
type StdoutTracer struct{}

func (w StdoutTracer) Log(name string, collection Collection) error {
	_, err := fmt.Printf("%s: %v\n", name, collection)
	return err
}

type tracerKey struct{}

// WithTracer installs the given trace logger into the context.
//
// By default, traces are logged to stdout.
// To redirect trace logs to a custom output, use:
//
//	ctx = fhirpath.WithTracer(ctx, MyCustomTraceLogger(true, file))
func WithTracer(ctx context.Context, logger Tracer) context.Context {
	return context.WithValue(ctx, tracerKey{}, logger)
}

// GetTraceLogger gets the trace logger from the context
// If no trace logger is found, a NoOpTraceLogger is returned
func tracer(ctx context.Context) (Tracer, error) {
	logger, ok := ctx.Value(tracerKey{}).(Tracer)
	if !ok {
		return StdoutTracer{}, nil
	}
	if logger == nil {
		return StdoutTracer{}, fmt.Errorf("no trace logger provided")
	}
	return logger, nil
}

type Functions map[string]Function
type Function = func(
	ctx context.Context,
	root Element, target Collection,
	inputOrdered bool,
	parameters []Expression,
	evaluate EvaluateFunc,
) (result Collection, resultOrdered bool, err error)

type EvaluateFunc = func(
	ctx context.Context,
	target Collection,
	expr Expression,
	scope *FunctionScope, // nil preserves parent scope
) (result Collection, resultOrdered bool, err error)

type functionCtxKey struct{}

type FunctionScope struct {
	index int
	total Collection
}

type functionScope struct {
	this      Element
	index     int
	aggregate bool
	total     Collection
}

func withFunctionScope(
	ctx context.Context,
	fnScope functionScope,
) context.Context {
	return context.WithValue(
		ctx,
		functionCtxKey{},
		fnScope,
	)
}

func getFunctionScope(ctx context.Context) (functionScope, bool) {
	fnCtx, ok := ctx.Value(functionCtxKey{}).(functionScope)
	return fnCtx, ok
}

type functionsKey struct{}

// WithFunctions installs the given functions into the context.
func WithFunctions(
	ctx context.Context,
	functions Functions,
) context.Context {
	allFns := getFunctions(ctx)

	maps.Copy(allFns, functions)

	return context.WithValue(ctx, functionsKey{}, allFns)
}

func getFunctions(ctx context.Context) Functions {
	fns, ok := ctx.Value(functionsKey{}).(Functions)
	if !ok {
		return maps.Clone(defaultFunctions)
	}
	return fns
}

func getFunction(ctx context.Context, name string) (Function, bool) {
	fns := getFunctions(ctx)
	fn, ok := fns[name]
	return fn, ok
}

// defaultFunctions contains FHIRPath specification functions as defined in the FHIRPath standard.
// For FHIR-specific extension functions, see FHIRFunctions.
var defaultFunctions = Functions{
	// Type functions
	"type": func(
		ctx context.Context,
		root Element, target Collection,
		inputOrdered bool,
		parameters []Expression,
		evaluate EvaluateFunc,
	) (result Collection, resultOrdered bool, err error) {
		if len(parameters) != 0 {
			return nil, false, fmt.Errorf("expected no parameters")
		}

		result = make(Collection, 0, len(target))
		for _, elem := range target {
			result = append(result, elem.TypeInfo())
		}

		return result, inputOrdered, nil
	},
	"is": func(
		ctx context.Context,
		root Element, target Collection,
		inputOrdered bool,
		parameters []Expression,
		evaluate EvaluateFunc,
	) (result Collection, resultOrdered bool, err error) {
		switch len(target) {
		case 0:
			return nil, true, nil
		case 1:
			// continue below
		default:
			return nil, false, fmt.Errorf("expected single input element")
		}
		if len(parameters) != 1 {
			return nil, false, fmt.Errorf("expected single type argument")
		}
		typeSpec := ParseTypeSpecifier(parameters[0].String())

		r, err := isType(ctx, target[0], typeSpec)
		if err != nil {
			return nil, false, err
		}
		return Collection{r}, true, nil
	},
	"as": func(
		ctx context.Context,
		root Element, target Collection,
		inputOrdered bool,
		parameters []Expression,
		evaluate EvaluateFunc,
	) (result Collection, resultOrdered bool, err error) {
		switch len(target) {
		case 0:
			return nil, true, nil
		case 1:
			// continue below
		default:
			return nil, false, fmt.Errorf("expected single input element")
		}
		if len(parameters) != 1 {
			return nil, false, fmt.Errorf("expected single type specifier parameter")
		}
		typeSpec := ParseTypeSpecifier(parameters[0].String())

		c, err := asType(ctx, target[0], typeSpec)
		if err != nil {
			return nil, false, err
		}
		return c, true, nil
	},
	"ofType": func(
		ctx context.Context,
		root Element, target Collection,
		inputOrdered bool,
		parameters []Expression,
		evaluate EvaluateFunc,
	) (result Collection, resultOrdered bool, err error) {
		if len(parameters) != 1 {
			return nil, false, fmt.Errorf("expected single type specifier parameter")
		}

		// If the input collection is empty, the result is empty
		if len(target) == 0 {
			return nil, true, nil
		}

		typeSpec := ParseTypeSpecifier(parameters[0].String())

		for _, elem := range target {
			isOfType, err := isType(ctx, elem, typeSpec)
			if err != nil {
				return nil, false, err
			}

			// Check if isOfType is a Boolean with value true
			if boolVal, ok := isOfType.(Boolean); ok && bool(boolVal) {
				result = append(result, elem)
			}
		}

		return result, inputOrdered, nil
	},

	// Boolean functions
	"not": func(
		ctx context.Context,
		root Element, target Collection,
		inputOrdered bool,
		parameters []Expression,
		evaluate EvaluateFunc,
	) (result Collection, resultOrdered bool, err error) {
		if len(parameters) != 0 {
			return nil, false, fmt.Errorf("expected no parameter")
		}
		b, ok, err := Singleton[Boolean](target)
		if err != nil {
			return nil, false, err
		}
		if !ok {
			return nil, true, nil
		}
		return Collection{!b}, true, nil
	},
	"empty": func(
		ctx context.Context,
		root Element, target Collection,
		inputOrdered bool,
		parameters []Expression,
		evaluate EvaluateFunc,
	) (result Collection, resultOrdered bool, err error) {
		if len(parameters) != 0 {
			return nil, false, fmt.Errorf("expected no parameters")
		}
		return Collection{Boolean(len(target) == 0)}, true, nil
	},
	"exists": func(
		ctx context.Context,
		root Element, target Collection,
		inputOrdered bool,
		parameters []Expression,
		evaluate EvaluateFunc,
	) (result Collection, resultOrdered bool, err error) {
		if len(parameters) > 1 {
			return nil, false, fmt.Errorf("expected at most one criteria parameter")
		}

		if len(parameters) == 0 {
			return Collection{Boolean(len(target) > 0)}, true, nil
		}

		// With criteria, equivalent to where(criteria).exists()
		for i, elem := range target {
			criteria, _, err := evaluate(ctx, Collection{elem}, parameters[0], &FunctionScope{index: i})
			if err != nil {
				return nil, false, err
			}

			b, ok, err := Singleton[Boolean](criteria)
			if err != nil {
				return nil, false, err
			}
			if ok && bool(b) {
				// Found at least one element that matches the criteria
				return Collection{Boolean(true)}, true, nil
			}
		}

		return Collection{Boolean(false)}, true, nil
	},
	"all": func(
		ctx context.Context,
		root Element, target Collection,
		inputOrdered bool,
		parameters []Expression,
		evaluate EvaluateFunc,
	) (result Collection, resultOrdered bool, err error) {
		if len(parameters) != 1 {
			return nil, false, fmt.Errorf("expected single criteria parameter")
		}

		// If the input collection is empty, the result is true
		if len(target) == 0 {
			return Collection{Boolean(true)}, true, nil
		}

		for i, elem := range target {
			criteria, _, err := evaluate(ctx, Collection{elem}, parameters[0], &FunctionScope{index: i})
			if err != nil {
				return nil, false, err
			}

			b, ok, err := Singleton[Boolean](criteria)
			if err != nil {
				return nil, false, err
			}
			if !ok || !bool(b) {
				// Found at least one element that doesn't match the criteria
				return Collection{Boolean(false)}, true, nil
			}
		}

		return Collection{Boolean(true)}, true, nil
	},
	"allTrue": func(
		ctx context.Context,
		root Element, target Collection,
		inputOrdered bool,
		parameters []Expression,
		evaluate EvaluateFunc,
	) (result Collection, resultOrdered bool, err error) {
		if len(parameters) != 0 {
			return nil, false, fmt.Errorf("expected no parameters")
		}

		// If the input collection is empty, the result is true
		if len(target) == 0 {
			return Collection{Boolean(true)}, true, nil
		}

		for _, elem := range target {
			b, ok, err := elem.ToBoolean(false)
			if err != nil {
				return nil, false, err
			}
			if !ok || !bool(b) {
				return Collection{Boolean(false)}, true, nil
			}
		}
		return Collection{Boolean(true)}, true, nil
	},
	"anyTrue": func(
		ctx context.Context,
		root Element, target Collection,
		inputOrdered bool,
		parameters []Expression,
		evaluate EvaluateFunc,
	) (result Collection, resultOrdered bool, err error) {
		if len(parameters) != 0 {
			return nil, false, fmt.Errorf("expected no parameters")
		}

		// If the input collection is empty, the result is false
		if len(target) == 0 {
			return Collection{Boolean(false)}, true, nil
		}

		for _, elem := range target {
			b, ok, err := elem.ToBoolean(false)
			if err != nil {
				return nil, false, err
			}
			if ok && bool(b) {
				return Collection{Boolean(true)}, true, nil
			}
		}
		return Collection{Boolean(false)}, true, nil
	},
	"allFalse": func(
		ctx context.Context,
		root Element, target Collection,
		inputOrdered bool,
		parameters []Expression,
		evaluate EvaluateFunc,
	) (result Collection, resultOrdered bool, err error) {
		if len(parameters) != 0 {
			return nil, false, fmt.Errorf("expected no parameters")
		}

		// If the input collection is empty, the result is true
		if len(target) == 0 {
			return Collection{Boolean(true)}, true, nil
		}

		for _, elem := range target {
			b, ok, err := elem.ToBoolean(false)
			if err != nil {
				return nil, false, err
			}
			if !ok || bool(b) {
				return Collection{Boolean(false)}, true, nil
			}
		}
		return Collection{Boolean(true)}, true, nil
	},
	"anyFalse": func(
		ctx context.Context,
		root Element, target Collection,
		inputOrdered bool,
		parameters []Expression,
		evaluate EvaluateFunc,
	) (result Collection, resultOrdered bool, err error) {
		if len(parameters) != 0 {
			return nil, false, fmt.Errorf("expected no parameters")
		}

		// If the input collection is empty, the result is false
		if len(target) == 0 {
			return Collection{Boolean(false)}, true, nil
		}

		for _, elem := range target {
			b, ok, err := elem.ToBoolean(false)
			if err != nil {
				return nil, false, err
			}
			if ok && !bool(b) {
				return Collection{Boolean(true)}, true, nil
			}
		}
		return Collection{Boolean(false)}, true, nil
	},

	// Collection functions
	"subsetOf": func(
		ctx context.Context,
		root Element, target Collection,
		inputOrdered bool,
		parameters []Expression,
		evaluate EvaluateFunc,
	) (result Collection, resultOrdered bool, err error) {
		if len(parameters) != 1 {
			return nil, false, fmt.Errorf("expected single collection parameter")
		}

		// If the input collection is empty, the result is true
		if len(target) == 0 {
			return Collection{Boolean(true)}, true, nil
		}

		other, _, err := evaluate(ctx, nil, parameters[0], nil)
		if err != nil {
			return nil, false, err
		}

		// If the other collection is empty, the result is false
		if len(other) == 0 {
			return Collection{Boolean(false)}, true, nil
		}

		for _, elem := range target {
			found := false
			for _, otherElem := range other {
				eq, ok := elem.Equal(otherElem)
				if ok && eq {
					found = true
					break
				}
			}
			if !found {
				return Collection{Boolean(false)}, true, nil
			}
		}
		return Collection{Boolean(true)}, true, nil
	},
	"supersetOf": func(
		ctx context.Context,
		root Element, target Collection,
		inputOrdered bool,
		parameters []Expression,
		evaluate EvaluateFunc,
	) (result Collection, resultOrdered bool, err error) {
		if len(parameters) != 1 {
			return nil, false, fmt.Errorf("expected single collection parameter")
		}

		other, _, err := evaluate(ctx, nil, parameters[0], nil)
		if err != nil {
			return nil, false, err
		}

		// If the other collection is empty, the result is true
		if len(other) == 0 {
			return Collection{Boolean(true)}, true, nil
		}

		// If the input collection is empty, the result is false
		if len(target) == 0 {
			return Collection{Boolean(false)}, true, nil
		}

		for _, otherElem := range other {
			found := false
			for _, elem := range target {
				eq, ok := otherElem.Equal(elem)
				if ok && eq {
					found = true
					break
				}
			}
			if !found {
				return Collection{Boolean(false)}, true, nil
			}
		}
		return Collection{Boolean(true)}, true, nil
	},
	"count": func(
		ctx context.Context,
		root Element, target Collection,
		inputOrdered bool,
		parameters []Expression,
		evaluate EvaluateFunc,
	) (result Collection, resultOrdered bool, err error) {
		if len(parameters) != 0 {
			return nil, false, fmt.Errorf("expected no parameters")
		}
		return Collection{Integer(len(target))}, true, nil
	},
	"distinct": func(
		ctx context.Context,
		root Element, target Collection,
		inputOrdered bool,
		parameters []Expression,
		evaluate EvaluateFunc,
	) (result Collection, resultOrdered bool, err error) {
		if len(parameters) != 0 {
			return nil, false, fmt.Errorf("expected no parameters")
		}

		// If the input collection is empty, the result is empty
		if len(target) == 0 {
			return nil, true, nil
		}

		for _, elem := range target {
			found := false
			for _, resultElem := range result {
				eq, ok := elem.Equal(resultElem)
				if ok && eq {
					found = true
					break
				}
			}
			if !found {
				result = append(result, elem)
			}
		}
		return result, false, nil
	},
	"isDistinct": func(
		ctx context.Context,
		root Element, target Collection,
		inputOrdered bool,
		parameters []Expression,
		evaluate EvaluateFunc,
	) (result Collection, resultOrdered bool, err error) {
		if len(parameters) != 0 {
			return nil, false, fmt.Errorf("expected no parameters")
		}

		// If the input collection is empty, the result is true
		if len(target) == 0 {
			return Collection{Boolean(true)}, true, nil
		}

		// Check if all elements are distinct
		for i := 0; i < len(target); i++ {
			for j := i + 1; j < len(target); j++ {
				eq, ok := target[i].Equal(target[j])
				if ok && eq {
					return Collection{Boolean(false)}, true, nil
				}
			}
		}
		return Collection{Boolean(true)}, true, nil
	},
	"where": func(
		ctx context.Context,
		root Element, target Collection,
		inputOrdered bool,
		parameters []Expression,
		evaluate EvaluateFunc,
	) (result Collection, resultOrdered bool, err error) {
		if len(parameters) != 1 {
			return nil, false, fmt.Errorf("expected single criteria parameter")
		}

		// If the input collection is empty, the result is empty
		if len(target) == 0 {
			return nil, true, nil
		}

		for i, elem := range target {
			criteria, _, err := evaluate(ctx, Collection{elem}, parameters[0], &FunctionScope{index: i})
			if err != nil {
				return nil, false, err
			}

			b, ok, err := Singleton[Boolean](criteria)
			if err != nil {
				return nil, false, err
			}
			if ok && bool(b) {
				// Element matches the criteria, add it to the result
				result = append(result, elem)
			}
		}

		return result, true, nil
	},
	"select": func(
		ctx context.Context,
		root Element, target Collection,
		inputOrdered bool,
		parameters []Expression,
		evaluate EvaluateFunc,
	) (result Collection, resultOrdered bool, err error) {
		if len(parameters) != 1 {
			return nil, false, fmt.Errorf("expected single projection parameter")
		}

		// If the input collection is empty, the result is empty
		if len(target) == 0 {
			return nil, true, nil
		}

		resultOrdered = inputOrdered
		for i, elem := range target {
			projection, ordered, err := evaluate(ctx, Collection{elem}, parameters[0], &FunctionScope{index: i})
			if err != nil {
				return nil, false, err
			}

			// Add all items from the projection to the result (flatten)
			result = append(result, projection...)

			if !ordered {
				resultOrdered = false
			}
		}

		return result, resultOrdered, nil
	},
	// sort implements FHIRPath v3.0.0 section 4.1.26 sort()
	"sort": func(
		ctx context.Context,
		root Element, target Collection,
		inputOrdered bool,
		parameters []Expression,
		evaluate EvaluateFunc,
	) (result Collection, resultOrdered bool, err error) {
		if len(target) == 0 {
			return nil, true, nil
		}

		type sortKeyValue struct {
			empty bool
			value Element
		}
		type sortItem struct {
			elem Element
			keys []sortKeyValue
		}

		items := make([]sortItem, len(target))
		for i, elem := range target {
			items[i].elem = elem
			if len(parameters) == 0 {
				continue
			}
			items[i].keys = make([]sortKeyValue, len(parameters))
			for j, param := range parameters {
				keyResult, _, err := evaluate(ctx, Collection{elem}, param, &FunctionScope{index: i})
				if err != nil {
					return nil, false, err
				}

				switch len(keyResult) {
				case 0:
					items[i].keys[j] = sortKeyValue{empty: true}
				case 1:
					items[i].keys[j] = sortKeyValue{value: keyResult[0]}
				default:
					return nil, false, fmt.Errorf(
						"sort key %d evaluated to %d items (expected 0 or 1)",
						j+1, len(keyResult),
					)
				}
			}
		}

		var sortErr error
		slices.SortStableFunc(items, func(a, b sortItem) int {
			if sortErr != nil {
				return 0
			}

			if len(parameters) == 0 {
				cmp, err := compareElementsForSort(a.elem, b.elem)
				if err != nil {
					sortErr = err
					return 0
				}
				return cmp
			}

			for idx, param := range parameters {
				dir := param.sortDirection
				if dir == sortDirectionNone {
					dir = sortDirectionAsc
				}

				av := a.keys[idx]
				bv := b.keys[idx]

				if av.empty && bv.empty {
					continue
				}
				if av.empty {
					return -1
				}
				if bv.empty {
					return 1
				}

				cmp, err := compareElementsForSort(av.value, bv.value)
				if err != nil {
					sortErr = err
					return 0
				}
				if cmp != 0 {
					if dir == sortDirectionDesc {
						cmp = -cmp
					}
					return cmp
				}
			}

			return 0
		})
		if sortErr != nil {
			return nil, false, sortErr
		}

		result = make(Collection, len(items))
		for i, item := range items {
			result[i] = item.elem
		}
		return result, true, nil
	},
	"repeat": func(
		ctx context.Context,
		root Element, target Collection,
		inputOrdered bool,
		parameters []Expression,
		evaluate EvaluateFunc,
	) (result Collection, resultOrdered bool, err error) {
		if len(parameters) != 1 {
			return nil, false, fmt.Errorf("expected single projection parameter")
		}

		// If the input collection is empty, the result is empty
		if len(target) == 0 {
			return nil, true, nil
		}

		var current = target
		var newItems Collection

		// Keep repeating the projection until no new items are found
		for {
			newItems = nil
			for i, elem := range current {
				projection, _, err := evaluate(ctx, Collection{elem}, parameters[0], &FunctionScope{index: i})
				if err != nil {
					return nil, false, err
				}

				for _, item := range projection {
					add := true
					// Check against already accumulated results
					for _, seen := range result {
						eq, ok := seen.Equal(item)
						if ok && eq {
							add = false
							break
						}
					}
					// Also check against items found in this iteration
					if add {
						for _, seen := range newItems {
							eq, ok := seen.Equal(item)
							if ok && eq {
								add = false
								break
							}
						}
					}
					if add {
						newItems = append(newItems, item)
					}
				}
			}

			// If no new items were found, we're done
			if len(newItems) == 0 {
				break
			}

			// Add new items to the result and set them as the current items for the next iteration
			result = append(result, newItems...)
			current = newItems
		}

		return result, false, nil
	},
	// repeatAll implements FHIRPath v3.0.0 section 4.1.26 repeatAll()
	"repeatAll": func(
		ctx context.Context,
		root Element, target Collection,
		inputOrdered bool,
		parameters []Expression,
		evaluate EvaluateFunc,
	) (result Collection, resultOrdered bool, err error) {
		if len(parameters) != 1 {
			return nil, false, fmt.Errorf("expected single projection parameter")
		}
		if len(target) == 0 {
			return nil, true, nil
		}

		queue := slices.Clone(target)

		for len(queue) > 0 {
			var next Collection
			for i, elem := range queue {
				projection, _, err := evaluate(ctx, Collection{elem}, parameters[0], &FunctionScope{index: i})
				if err != nil {
					return nil, false, err
				}
				if len(projection) == 0 {
					continue
				}
				result = append(result, projection...)
				next = append(next, projection...)
			}
			queue = next
		}

		return result, false, nil
	},
	"single": func(
		ctx context.Context,
		root Element, target Collection,
		inputOrdered bool,
		parameters []Expression,
		evaluate EvaluateFunc,
	) (result Collection, resultOrdered bool, err error) {
		if len(parameters) != 0 {
			return nil, false, fmt.Errorf("expected no parameters")
		}

		// If the input collection is empty, the result is empty
		if len(target) == 0 {
			return nil, true, nil
		}

		// If there are multiple items, signal an error
		if len(target) > 1 {
			return nil, false, fmt.Errorf("expected single item but got %d items", len(target))
		}

		// Return the single item
		return Collection{target[0]}, true, nil
	},
	"first": func(
		ctx context.Context,
		root Element, target Collection,
		inputOrdered bool,
		parameters []Expression,
		evaluate EvaluateFunc,
	) (result Collection, resultOrdered bool, err error) {
		if len(parameters) != 0 {
			return nil, false, fmt.Errorf("expected no parameters")
		}

		// If the input collection is empty, the result is empty
		if len(target) == 0 {
			return nil, true, nil
		}

		if !inputOrdered {
			return nil, false, fmt.Errorf("expected ordered input")
		}

		// Return the first item
		return Collection{target[0]}, true, nil
	},
	"last": func(
		ctx context.Context,
		root Element, target Collection,
		inputOrdered bool,
		parameters []Expression,
		evaluate EvaluateFunc,
	) (result Collection, resultOrdered bool, err error) {
		if len(parameters) != 0 {
			return nil, false, fmt.Errorf("expected no parameters")
		}

		// If the input collection is empty, the result is empty
		if len(target) == 0 {
			return nil, true, nil
		}

		if !inputOrdered {
			return nil, false, fmt.Errorf("expected ordered input")
		}

		// Return the last item
		return Collection{target[len(target)-1]}, true, nil
	},
	"tail": func(
		ctx context.Context,
		root Element, target Collection,
		inputOrdered bool,
		parameters []Expression,
		evaluate EvaluateFunc,
	) (result Collection, resultOrdered bool, err error) {
		if len(parameters) != 0 {
			return nil, false, fmt.Errorf("expected no parameters")
		}

		// If the input collection has no items or only one item, the result is empty
		if len(target) <= 1 {
			return nil, true, nil
		}

		if !inputOrdered {
			return nil, false, fmt.Errorf("expected ordered input")
		}

		// Return all but the first item
		return target[1:], inputOrdered, nil
	},
	"skip": func(
		ctx context.Context,
		root Element, target Collection,
		inputOrdered bool,
		parameters []Expression,
		evaluate EvaluateFunc,
	) (result Collection, resultOrdered bool, err error) {
		if len(parameters) != 1 {
			return nil, false, fmt.Errorf("expected single num parameter")
		}

		// If the input collection is empty, the result is empty
		if len(target) == 0 {
			return nil, true, nil
		}

		if !inputOrdered {
			return nil, false, fmt.Errorf("expected ordered input")
		}

		// Evaluate the num parameter
		numCollection, _, err := evaluate(ctx, nil, parameters[0], nil)
		if err != nil {
			return nil, false, err
		}

		// Convert to integer
		num, ok, err := Singleton[Integer](numCollection)
		if err != nil {
			return nil, false, err
		}
		if !ok {
			return nil, false, fmt.Errorf("expected integer parameter")
		}

		// If num is less than or equal to zero, return the input collection
		if num <= 0 {
			return target, inputOrdered, nil
		}

		// If num is greater than or equal to the length of the collection, return empty
		if int(num) >= len(target) {
			return nil, true, nil
		}

		// Return all but the first num items
		return target[num:], inputOrdered, nil
	},
	"take": func(
		ctx context.Context,
		root Element, target Collection,
		inputOrdered bool,
		parameters []Expression,
		evaluate EvaluateFunc,
	) (result Collection, resultOrdered bool, err error) {
		if len(parameters) != 1 {
			return nil, false, fmt.Errorf("expected single num parameter")
		}

		// If the input collection is empty, the result is empty
		if len(target) == 0 {
			return nil, true, nil
		}

		if !inputOrdered {
			return nil, false, fmt.Errorf("expected ordered input")
		}

		// Evaluate the num parameter
		numCollection, _, err := evaluate(ctx, nil, parameters[0], nil)
		if err != nil {
			return nil, false, err
		}

		// Convert to integer
		num, ok, err := Singleton[Integer](numCollection)
		if err != nil {
			return nil, false, err
		}
		if !ok {
			return nil, false, fmt.Errorf("expected integer parameter")
		}

		// If num is less than or equal to zero, return empty
		if num <= 0 {
			return nil, true, nil
		}

		// If num is greater than the length of the collection, return the whole collection
		if int(num) >= len(target) {
			return target, inputOrdered, nil
		}

		// Return the first num items
		return target[:num], inputOrdered, nil
	},
	"intersect": func(
		ctx context.Context,
		root Element, target Collection,
		inputOrdered bool,
		parameters []Expression,
		evaluate EvaluateFunc,
	) (result Collection, resultOrdered bool, err error) {
		if len(parameters) != 1 {
			return nil, false, fmt.Errorf("expected single collection parameter")
		}

		// If the input collection is empty, the result is empty
		if len(target) == 0 {
			return nil, true, nil
		}

		// Evaluate the other collection parameter
		other, _, err := evaluate(ctx, nil, parameters[0], nil)
		if err != nil {
			return nil, false, err
		}

		// If the other collection is empty, the result is empty
		if len(other) == 0 {
			return nil, true, nil
		}

		// Find the intersection of the two collections
		for _, elem := range target {
			// Check if the element is in the other collection
			for _, otherElem := range other {
				eq, ok := elem.Equal(otherElem)
				if ok && eq {
					// Check if the element is already in the result (eliminate duplicates)
					found := false
					for _, resultElem := range result {
						eq, ok := elem.Equal(resultElem)
						if ok && eq {
							found = true
							break
						}
					}
					if !found {
						result = append(result, elem)
					}
					break
				}
			}
		}

		return result, false, nil
	},
	"exclude": func(
		ctx context.Context,
		root Element, target Collection,
		inputOrdered bool,
		parameters []Expression,
		evaluate EvaluateFunc,
	) (result Collection, resultOrdered bool, err error) {
		if len(parameters) != 1 {
			return nil, false, fmt.Errorf("expected single collection parameter")
		}

		// If the input collection is empty, the result is empty
		if len(target) == 0 {
			return nil, true, nil
		}

		// Evaluate the other collection parameter
		other, _, err := evaluate(ctx, nil, parameters[0], nil)
		if err != nil {
			return nil, false, err
		}

		// If the other collection is empty, the result is the input collection
		if len(other) == 0 {
			return target, inputOrdered, nil
		}

		// Find the elements in the input collection that are not in the other collection
		for _, elem := range target {
			// Check if the element is in the other collection
			found := false
			for _, otherElem := range other {
				eq, ok := elem.Equal(otherElem)
				if ok && eq {
					found = true
					break
				}
			}
			if !found {
				// Element is not in the other collection, add it to the result
				result = append(result, elem)
			}
		}

		return result, inputOrdered, nil
	},
	"union": func(
		ctx context.Context,
		root Element, target Collection,
		inputOrdered bool,
		parameters []Expression,
		evaluate EvaluateFunc,
	) (result Collection, resultOrdered bool, err error) {
		if len(parameters) != 1 {
			return nil, false, fmt.Errorf("expected single collection parameter")
		}

		// Evaluate the other collection parameter
		other, _, err := evaluate(ctx, nil, parameters[0], nil)
		if err != nil {
			return nil, false, err
		}

		// Use the Union method to merge the collections
		return target.Union(other), false, nil
	},
	"combine": func(
		ctx context.Context,
		root Element, target Collection,
		inputOrdered bool,
		parameters []Expression,
		evaluate EvaluateFunc,
	) (result Collection, resultOrdered bool, err error) {
		if len(parameters) != 1 {
			return nil, false, fmt.Errorf("expected single collection parameter")
		}

		// Evaluate the other collection parameter
		other, _, err := evaluate(ctx, nil, parameters[0], nil)
		if err != nil {
			return nil, false, err
		}

		// Use the Combine method to merge the collections
		return target.Combine(other), false, nil
	},
	// coalesce implements FHIRPath v3.0.0 section 4.1.26 coalesce()
	"coalesce": func(
		ctx context.Context,
		root Element, target Collection,
		inputOrdered bool,
		parameters []Expression,
		evaluate EvaluateFunc,
	) (result Collection, resultOrdered bool, err error) {
		if len(parameters) == 0 {
			return nil, false, fmt.Errorf("expected at least one collection parameter")
		}

		for _, param := range parameters {
			value, ordered, err := evaluate(ctx, nil, param, nil)
			if err != nil {
				return nil, false, err
			}
			if len(value) > 0 {
				return value, ordered, nil
			}
		}

		return nil, true, nil
	},
	// String functions
	"indexOf": func(
		ctx context.Context,
		root Element, target Collection,
		inputOrdered bool,
		parameters []Expression,
		evaluate EvaluateFunc,
	) (result Collection, resultOrdered bool, err error) {
		if len(parameters) != 1 {
			return nil, false, fmt.Errorf("expected single substring parameter")
		}

		// Convert input to string
		s, ok, err := Singleton[String](target)
		if err != nil {
			return nil, false, err
		}
		if !ok {
			return nil, true, nil
		}

		// Evaluate the substring parameter
		substringCollection, _, err := evaluate(ctx, nil, parameters[0], nil)
		if err != nil {
			return nil, false, err
		}

		// Convert substring to string
		substring, ok, err := Singleton[String](substringCollection)
		if err != nil {
			return nil, false, err
		}
		if !ok {
			// If substring is empty/null, return empty collection
			return nil, true, nil
		}

		// If substring is an empty string (''), the function returns 0
		if substring == "" {
			return Collection{Integer(0)}, true, nil
		}

		// Return the index of the substring in the string
		index := strings.Index(string(s), string(substring))
		return Collection{Integer(index)}, true, nil
	},
	"lastIndexOf": func(
		ctx context.Context,
		root Element, target Collection,
		inputOrdered bool,
		parameters []Expression,
		evaluate EvaluateFunc,
	) (result Collection, resultOrdered bool, err error) {
		if len(parameters) != 1 {
			return nil, false, fmt.Errorf("expected single substring parameter")
		}

		// If the input collection is empty, the result is empty
		if len(target) == 0 {
			return nil, true, nil
		}

		// If the input collection contains multiple items, signal an error
		if len(target) > 1 {
			return nil, false, fmt.Errorf("expected single input element")
		}

		// Convert input to string
		s, ok, err := Singleton[String](target)
		if err != nil {
			return nil, false, err
		}
		if !ok {
			return nil, true, nil
		}

		// Evaluate the substring parameter
		substringCollection, _, err := evaluate(ctx, nil, parameters[0], nil)
		if err != nil {
			return nil, false, err
		}

		// Convert substring to string
		substring, ok, err := Singleton[String](substringCollection)
		if err != nil {
			return nil, false, err
		}
		if !ok {
			// If substring is empty/null, return empty collection
			return nil, true, nil
		}

		// If substring is an empty string (''), the function returns the length of the input string
		if substring == "" {
			return Collection{Integer(len([]rune(s)))}, true, nil
		}

		// Return the index of the last occurrence of the substring in the string
		index := strings.LastIndex(string(s), string(substring))
		return Collection{Integer(index)}, true, nil
	},
	"substring": func(
		ctx context.Context,
		root Element, target Collection,
		inputOrdered bool,
		parameters []Expression,
		evaluate EvaluateFunc,
	) (result Collection, resultOrdered bool, err error) {
		if len(parameters) < 1 || len(parameters) > 2 {
			return nil, false, fmt.Errorf("expected one or two parameters (start, [length])")
		}

		// Convert input to string
		s, ok, err := Singleton[String](target)
		if err != nil {
			return nil, false, err
		}
		if !ok {
			return nil, true, nil
		}
		runes := []rune(string(s))
		runeCount := len(runes)

		// Determine parameter evaluation context
		var paramTarget Collection
		var paramScope *FunctionScope
		parentScope, ok := getFunctionScope(ctx)
		if ok {
			if len(target) > 0 && target[0] != nil {
				paramTarget = Collection{target[0]}
				paramScope = &FunctionScope{index: parentScope.index, total: parentScope.total}
			}
		} else if root != nil {
			paramTarget = Collection{root}
		}

		// Evaluate the start parameter (FHIRPath substring section states empty args propagate as empty results)
		startCollection, _, err := evaluate(ctx, paramTarget, parameters[0], paramScope)
		if err != nil {
			return nil, false, err
		}
		if len(startCollection) == 0 {
			return nil, true, nil
		}

		// Convert start to integer
		start, ok, err := Singleton[Integer](startCollection)
		if err != nil {
			return nil, false, err
		}
		if !ok {
			return nil, false, fmt.Errorf("expected integer start parameter")
		}

		startIdx := int(start)
		if startIdx < 0 || startIdx >= runeCount {
			return nil, true, nil
		}

		// If length parameter is provided
		if len(parameters) == 2 {
			// Evaluate the length parameter (FHIRPath substring section: empty length behaves as if omitted)
			lengthCollection, _, err := evaluate(ctx, paramTarget, parameters[1], paramScope)
			if err != nil {
				return nil, false, err
			}
			if len(lengthCollection) == 0 {
				return Collection{String(string(runes[startIdx:]))}, true, nil
			}

			// Convert length to integer
			length, ok, err := Singleton[Integer](lengthCollection)
			if err != nil {
				return nil, false, err
			}
			if !ok {
				return nil, false, fmt.Errorf("expected integer length parameter")
			}

			if length <= 0 {
				return Collection{String("")}, true, nil
			}

			end := startIdx + int(length)
			if end > runeCount {
				end = runeCount
			}

			return Collection{String(string(runes[startIdx:end]))}, true, nil
		}

		// If length parameter is not provided, return the rest of the string
		return Collection{String(string(runes[startIdx:]))}, true, nil
	},
	"startsWith": func(
		ctx context.Context,
		root Element, target Collection,
		inputOrdered bool,
		parameters []Expression,
		evaluate EvaluateFunc,
	) (result Collection, resultOrdered bool, err error) {
		if len(parameters) != 1 {
			return nil, false, fmt.Errorf("expected single prefix parameter")
		}

		// Convert input to string
		s, ok, err := Singleton[String](target)
		if err != nil {
			return nil, false, err
		}
		if !ok {
			return nil, true, nil
		}

		// Determine parameter evaluation context
		var paramTarget Collection
		var paramScope *FunctionScope
		parentScope, ok := getFunctionScope(ctx)
		if ok {
			if len(target) > 0 && target[0] != nil {
				paramTarget = Collection{target[0]}
				paramScope = &FunctionScope{index: parentScope.index, total: parentScope.total}
			}
		} else if root != nil {
			paramTarget = Collection{root}
		}

		// Evaluate the prefix parameter
		prefixCollection, _, err := evaluate(ctx, paramTarget, parameters[0], paramScope)
		if err != nil {
			return nil, false, err
		}
		if len(prefixCollection) == 0 {
			return nil, true, nil
		}

		// Convert prefix to string
		prefix, ok, err := Singleton[String](prefixCollection)
		if err != nil {
			return nil, false, err
		}
		if !ok {
			return nil, false, fmt.Errorf("expected string prefix parameter")
		}

		// If prefix is an empty string (''), the result is true
		if prefix == "" {
			return Collection{Boolean(true)}, true, nil
		}

		// Check if the string starts with the prefix
		startsWith := strings.HasPrefix(string(s), string(prefix))
		return Collection{Boolean(startsWith)}, true, nil
	},
	"endsWith": func(
		ctx context.Context,
		root Element, target Collection,
		inputOrdered bool,
		parameters []Expression,
		evaluate EvaluateFunc,
	) (result Collection, resultOrdered bool, err error) {
		if len(parameters) != 1 {
			return nil, false, fmt.Errorf("expected single suffix parameter")
		}

		// Convert input to string
		s, ok, err := Singleton[String](target)
		if err != nil {
			return nil, false, err
		}
		if !ok {
			return nil, true, nil
		}

		// Determine parameter evaluation context
		var paramTarget Collection
		var paramScope *FunctionScope
		parentScope, ok := getFunctionScope(ctx)
		if ok {
			if len(target) > 0 && target[0] != nil {
				paramTarget = Collection{target[0]}
				paramScope = &FunctionScope{index: parentScope.index, total: parentScope.total}
			}
		} else if root != nil {
			paramTarget = Collection{root}
		}

		// Evaluate the suffix parameter
		suffixCollection, _, err := evaluate(ctx, paramTarget, parameters[0], paramScope)
		if err != nil {
			return nil, false, err
		}
		if len(suffixCollection) == 0 {
			return nil, true, nil
		}

		// Convert suffix to string
		suffix, ok, err := Singleton[String](suffixCollection)
		if err != nil {
			return nil, false, err
		}
		if !ok {
			return nil, false, fmt.Errorf("expected string suffix parameter")
		}

		// If suffix is an empty string (''), the result is true
		if suffix == "" {
			return Collection{Boolean(true)}, true, nil
		}

		// Check if the string ends with the suffix
		endsWith := strings.HasSuffix(string(s), string(suffix))
		return Collection{Boolean(endsWith)}, true, nil
	},
	"contains": func(
		ctx context.Context,
		root Element, target Collection,
		inputOrdered bool,
		parameters []Expression,
		evaluate EvaluateFunc,
	) (result Collection, resultOrdered bool, err error) {
		if len(parameters) != 1 {
			return nil, false, fmt.Errorf("expected single substring parameter")
		}

		// Convert input to string
		s, ok, err := Singleton[String](target)
		if err != nil {
			return nil, false, err
		}
		if !ok {
			return nil, true, nil
		}

		// Determine parameter evaluation context
		var paramTarget Collection
		var paramScope *FunctionScope
		parentScope, ok := getFunctionScope(ctx)
		if ok {
			if len(target) > 0 && target[0] != nil {
				paramTarget = Collection{target[0]}
				paramScope = &FunctionScope{index: parentScope.index, total: parentScope.total}
			}
		} else if root != nil {
			paramTarget = Collection{root}
		}

		// Evaluate the substring parameter
		substringCollection, _, err := evaluate(ctx, paramTarget, parameters[0], paramScope)
		if err != nil {
			return nil, false, err
		}
		if len(substringCollection) == 0 {
			return nil, true, nil
		}

		// Convert substring to string
		substring, ok, err := Singleton[String](substringCollection)
		if err != nil {
			return nil, false, err
		}
		if !ok {
			return nil, false, fmt.Errorf("expected string substring parameter")
		}

		// If substring is an empty string (''), the result is true
		if substring == "" {
			return Collection{Boolean(true)}, true, nil
		}

		// Check if the string contains the substring
		contains := strings.Contains(string(s), string(substring))
		return Collection{Boolean(contains)}, true, nil
	},
	"upper": func(
		ctx context.Context,
		root Element, target Collection,
		inputOrdered bool,
		parameters []Expression,
		evaluate EvaluateFunc,
	) (result Collection, resultOrdered bool, err error) {
		if len(parameters) != 0 {
			return nil, false, fmt.Errorf("expected no parameters")
		}

		// Convert input to string
		s, ok, err := Singleton[String](target)
		if err != nil {
			return nil, false, err
		}
		if !ok {
			return nil, true, nil
		}

		// Convert the string to upper case
		return Collection{String(strings.ToUpper(string(s)))}, true, nil
	},
	"lower": func(
		ctx context.Context,
		root Element, target Collection,
		inputOrdered bool,
		parameters []Expression,
		evaluate EvaluateFunc,
	) (result Collection, resultOrdered bool, err error) {
		if len(parameters) != 0 {
			return nil, false, fmt.Errorf("expected no parameters")
		}

		// Convert input to string
		s, ok, err := Singleton[String](target)
		if err != nil {
			return nil, false, err
		}
		if !ok {
			return nil, true, nil
		}

		// Convert the string to lower case
		return Collection{String(strings.ToLower(string(s)))}, true, nil
	},
	"replace": func(
		ctx context.Context,
		root Element, target Collection,
		inputOrdered bool,
		parameters []Expression,
		evaluate EvaluateFunc,
	) (result Collection, resultOrdered bool, err error) {
		if len(parameters) != 2 {
			return nil, false, fmt.Errorf("expected two parameters (pattern, substitution)")
		}

		// Convert input to string
		s, ok, err := Singleton[String](target)
		if err != nil {
			return nil, false, err
		}
		if !ok {
			return nil, true, nil
		}

		// Evaluate pattern and substitution parameters against the input string
		var evalTarget Collection
		if len(target) > 0 {
			evalTarget = Collection{target[0]}
		}

		// Evaluate the pattern parameter
		patternCollection, _, err := evaluate(ctx, evalTarget, parameters[0], nil)
		if err != nil {
			return nil, false, err
		}

		// Convert pattern to string
		pattern, ok, err := Singleton[String](patternCollection)
		if err != nil {
			return nil, false, err
		}
		if !ok {
			return nil, true, nil
		}

		// Evaluate the substitution parameter
		substitutionCollection, _, err := evaluate(ctx, evalTarget, parameters[1], nil)
		if err != nil {
			return nil, false, err
		}

		// Convert substitution to string
		substitution, ok, err := Singleton[String](substitutionCollection)
		if err != nil {
			return nil, false, err
		}
		if !ok {
			return nil, true, nil
		}

		// If pattern is an empty string (''), every character in the input string is surrounded by the substitution
		if pattern == "" {
			var result strings.Builder
			result.WriteString(string(substitution))
			for _, c := range s {
				result.WriteRune(c)
				result.WriteString(string(substitution))
			}
			return Collection{String(result.String())}, true, nil
		}

		// Replace all instances of pattern with substitution
		return Collection{String(strings.ReplaceAll(string(s), string(pattern), string(substitution)))}, true, nil
	},
	"matches": func(
		ctx context.Context,
		root Element, target Collection,
		inputOrdered bool,
		parameters []Expression,
		evaluate EvaluateFunc,
	) (result Collection, resultOrdered bool, err error) {
		if len(parameters) < 1 || len(parameters) > 2 {
			return nil, false, fmt.Errorf("expected regex parameter and optional flags parameter")
		}

		// Convert input to string
		s, ok, err := Singleton[String](target)
		if err != nil {
			return nil, false, err
		}
		if !ok {
			return nil, true, nil
		}

		// Evaluate the regex parameter
		regexCollection, _, err := evaluate(ctx, nil, parameters[0], nil)
		if err != nil {
			return nil, false, err
		}

		// If regex is empty collection, return empty
		if len(regexCollection) == 0 {
			return nil, true, nil
		}

		// Convert regex to string
		regexStr, ok, err := Singleton[String](regexCollection)
		if err != nil {
			return nil, false, err
		}
		if !ok {
			return nil, true, nil
		}

		// Evaluate optional flags parameter
		var flags string
		if len(parameters) == 2 {
			flagsCollection, _, err := evaluate(ctx, nil, parameters[1], nil)
			if err != nil {
				return nil, false, err
			}

			flagsStr, ok, err := Singleton[String](flagsCollection)
			if err != nil {
				return nil, false, err
			}
			if !ok {
				return nil, false, fmt.Errorf("expected string flags parameter")
			}
			flags = string(flagsStr)
		}

		// Apply flags to the regular expression
		// Per FHIRPath spec, regex operates in single-line mode by default (. matches newlines)
		pattern := "(?s)" + string(regexStr)
		for _, flag := range flags {
			switch flag {
			case 'i':
				// Case-insensitive: wrap pattern with (?i)
				pattern = "(?i)" + pattern
			case 'm':
				// Multiline mode: wrap pattern with (?m)
				pattern = "(?m)" + pattern
			default:
				return nil, false, fmt.Errorf("unsupported regex flag: %c", flag)
			}
		}

		// Compile the regular expression
		regex, err := regexp.Compile(pattern)
		if err != nil {
			return nil, false, fmt.Errorf("invalid regular expression: %v", err)
		}

		// Check if the string matches the regular expression
		return Collection{Boolean(regex.MatchString(string(s)))}, true, nil
	},
	"replaceMatches": func(
		ctx context.Context,
		root Element, target Collection,
		inputOrdered bool,
		parameters []Expression,
		evaluate EvaluateFunc,
	) (result Collection, resultOrdered bool, err error) {
		if len(parameters) < 2 || len(parameters) > 3 {
			return nil, false, fmt.Errorf("expected regex, substitution, and optional flags parameters")
		}

		// Convert input to string
		s, ok, err := Singleton[String](target)
		if err != nil {
			return nil, false, err
		}
		if !ok {
			return nil, true, nil
		}

		// Evaluate the regex parameter
		regexCollection, _, err := evaluate(ctx, nil, parameters[0], nil)
		if err != nil {
			return nil, false, err
		}

		// If regex is empty collection, return empty
		if len(regexCollection) == 0 {
			return nil, true, nil
		}

		// Convert regex to string
		regexStr, ok, err := Singleton[String](regexCollection)
		if err != nil {
			return nil, false, err
		}
		if !ok {
			return nil, true, nil
		}

		// If regex is an empty string, return input unchanged per spec
		if regexStr == "" {
			return Collection{s}, true, nil
		}

		// Evaluate the substitution parameter
		substitutionCollection, _, err := evaluate(ctx, nil, parameters[1], nil)
		if err != nil {
			return nil, false, err
		}

		// If substitution is empty collection, return empty
		if len(substitutionCollection) == 0 {
			return nil, true, nil
		}

		// Convert substitution to string
		substitution, ok, err := Singleton[String](substitutionCollection)
		if err != nil {
			return nil, false, err
		}
		if !ok {
			return nil, true, nil
		}

		// Evaluate optional flags parameter
		var flags string
		if len(parameters) == 3 {
			flagsCollection, _, err := evaluate(ctx, nil, parameters[2], nil)
			if err != nil {
				return nil, false, err
			}

			flagsStr, ok, err := Singleton[String](flagsCollection)
			if err != nil {
				return nil, false, err
			}
			if !ok {
				return nil, false, fmt.Errorf("expected string flags parameter")
			}
			flags = string(flagsStr)
		}

		// Apply flags to the regular expression
		// Per FHIRPath spec, regex operates in single-line mode by default (. matches newlines)
		pattern := "(?s)" + string(regexStr)
		for _, flag := range flags {
			switch flag {
			case 'i':
				// Case-insensitive: wrap pattern with (?i)
				pattern = "(?i)" + pattern
			case 'm':
				// Multiline mode: wrap pattern with (?m)
				pattern = "(?m)" + pattern
			default:
				return nil, false, fmt.Errorf("unsupported regex flag: %c", flag)
			}
		}

		// Compile the regular expression
		regex, err := regexp.Compile(pattern)
		if err != nil {
			return nil, false, fmt.Errorf("invalid regular expression: %v", err)
		}

		// Replace all matches of the regular expression with the substitution
		return Collection{String(regex.ReplaceAllString(string(s), string(substitution)))}, true, nil
	},
	"length": func(
		ctx context.Context,
		root Element, target Collection,
		inputOrdered bool,
		parameters []Expression,
		evaluate EvaluateFunc,
	) (result Collection, resultOrdered bool, err error) {
		if len(parameters) != 0 {
			return nil, false, fmt.Errorf("expected no parameters")
		}

		// Convert input to string
		s, ok, err := Singleton[String](target)
		if err != nil {
			return nil, false, err
		}
		if !ok {
			return nil, true, nil
		}

		// Return the length of the string
		return Collection{Integer(len(s))}, true, nil
	},
	"toChars": func(
		ctx context.Context,
		root Element, target Collection,
		inputOrdered bool,
		parameters []Expression,
		evaluate EvaluateFunc,
	) (result Collection, resultOrdered bool, err error) {
		if len(parameters) != 0 {
			return nil, false, fmt.Errorf("expected no parameters")
		}

		// Convert input to string
		s, ok, err := Singleton[String](target)
		if err != nil {
			return nil, false, err
		}
		if !ok {
			return nil, true, nil
		}

		// Convert the string to a collection of characters
		chars := make(Collection, len(s))
		for i, c := range string(s) {
			chars[i] = String(c)
		}
		return chars, true, nil
	},
	"matchesFull": func(
		ctx context.Context,
		root Element, target Collection,
		inputOrdered bool,
		parameters []Expression,
		evaluate EvaluateFunc,
	) (result Collection, resultOrdered bool, err error) {
		if len(parameters) != 1 {
			return nil, false, fmt.Errorf("expected single regex parameter")
		}

		// Convert input to string
		s, ok, err := Singleton[String](target)
		if err != nil {
			return nil, false, err
		}
		if !ok {
			return nil, true, nil
		}

		// Evaluate the regex parameter
		regexCollection, _, err := evaluate(ctx, nil, parameters[0], nil)
		if err != nil {
			return nil, false, err
		}

		// Convert regex to string
		regexStr, ok, err := Singleton[String](regexCollection)
		if err != nil {
			return nil, false, err
		}
		if !ok {
			return nil, false, fmt.Errorf("expected string regex parameter")
		}

		// Compile the regular expression
		regex, err := regexp.Compile("^" + string(regexStr) + "$")
		if err != nil {
			return nil, false, fmt.Errorf("invalid regular expression: %v", err)
		}

		// Check if the string matches the regular expression exactly
		return Collection{Boolean(regex.MatchString(string(s)))}, true, nil
	},
	"trim": func(
		ctx context.Context,
		root Element, target Collection,
		inputOrdered bool,
		parameters []Expression,
		evaluate EvaluateFunc,
	) (result Collection, resultOrdered bool, err error) {
		if len(parameters) != 0 {
			return nil, false, fmt.Errorf("expected no parameters")
		}

		// Convert input to string
		s, ok, err := Singleton[String](target)
		if err != nil {
			return nil, false, err
		}
		if !ok {
			return nil, true, nil
		}

		// Trim whitespace from both ends
		return Collection{String(strings.TrimSpace(string(s)))}, true, nil
	},
	"split": func(
		ctx context.Context,
		root Element, target Collection,
		inputOrdered bool,
		parameters []Expression,
		evaluate EvaluateFunc,
	) (result Collection, resultOrdered bool, err error) {
		if len(parameters) != 1 {
			return nil, false, fmt.Errorf("expected single separator parameter")
		}

		// Convert input to string
		s, ok, err := Singleton[String](target)
		if err != nil {
			return nil, false, err
		}
		if !ok {
			return nil, true, nil
		}

		// Evaluate the separator parameter
		separatorCollection, _, err := evaluate(ctx, nil, parameters[0], nil)
		if err != nil {
			return nil, false, err
		}

		// Convert separator to string
		separator, ok, err := Singleton[String](separatorCollection)
		if err != nil {
			return nil, false, err
		}
		if !ok {
			return nil, false, fmt.Errorf("expected string separator parameter")
		}

		// Split the string by the separator
		parts := strings.Split(string(s), string(separator))
		result = make(Collection, len(parts))
		for i, part := range parts {
			result[i] = String(part)
		}
		return result, true, nil
	},
	"join": func(
		ctx context.Context,
		root Element, target Collection,
		inputOrdered bool,
		parameters []Expression,
		evaluate EvaluateFunc,
	) (result Collection, resultOrdered bool, err error) {
		if len(parameters) > 1 {
			return nil, false, fmt.Errorf("expected at most one separator parameter")
		}

		// If the input collection is empty, the result is empty
		if len(target) == 0 {
			return nil, true, nil
		}

		// Default separator is empty string
		separator := String("")
		if len(parameters) == 1 {
			// Evaluate the separator parameter
			separatorCollection, _, err := evaluate(ctx, nil, parameters[0], nil)
			if err != nil {
				return nil, false, err
			}

			// Convert separator to string
			sep, ok, err := Singleton[String](separatorCollection)
			if err != nil {
				return nil, false, err
			}
			if !ok {
				return nil, false, fmt.Errorf("expected string separator parameter")
			}
			separator = sep
		}

		// Convert all elements to strings
		parts := make([]string, 0, len(target))
		for _, elem := range target {
			s, ok, err := elementTo[String](elem, true)
			if err != nil || !ok {
				// Skip elements that can't be converted to strings
				continue
			}
			parts = append(parts, string(s))
		}

		// If no elements could be converted to strings, return empty
		if len(parts) == 0 {
			return nil, true, nil
		}

		// Join the strings with the separator
		return Collection{String(strings.Join(parts, string(separator)))}, true, nil
	},
	"encode": func(
		ctx context.Context,
		root Element, target Collection,
		inputOrdered bool,
		parameters []Expression,
		evaluate EvaluateFunc,
	) (result Collection, resultOrdered bool, err error) {
		if len(parameters) != 1 {
			return nil, false, fmt.Errorf("expected single format parameter")
		}

		// If the input collection is empty, the result is empty
		if len(target) == 0 {
			return nil, true, nil
		}

		// Convert input to string
		s, ok, err := Singleton[String](target)
		if err != nil {
			return nil, false, err
		}
		if !ok {
			return nil, true, nil
		}

		// Evaluate the format parameter
		formatCollection, _, err := evaluate(ctx, nil, parameters[0], nil)
		if err != nil {
			return nil, false, err
		}

		// Convert format to string
		format, ok, err := Singleton[String](formatCollection)
		if err != nil {
			return nil, false, err
		}
		if !ok {
			return nil, false, fmt.Errorf("expected string format parameter")
		}

		// Encode according to format
		switch string(format) {
		case "hex":
			// Convert to hex using encoding/hex
			hex := hex.EncodeToString([]byte(s))
			return Collection{String(hex)}, true, nil
		case "base64":
			// Convert to base64
			encoded := base64.StdEncoding.EncodeToString([]byte(s))
			return Collection{String(encoded)}, true, nil
		case "urlbase64":
			// Convert to URL-safe base64
			encoded := base64.URLEncoding.EncodeToString([]byte(s))
			return Collection{String(encoded)}, true, nil
		default:
			return nil, false, fmt.Errorf("unsupported encoding format: %s", format)
		}
	},
	"decode": func(
		ctx context.Context,
		root Element, target Collection,
		inputOrdered bool,
		parameters []Expression,
		evaluate EvaluateFunc,
	) (result Collection, resultOrdered bool, err error) {
		if len(parameters) != 1 {
			return nil, false, fmt.Errorf("expected single format parameter")
		}

		// If the input collection is empty, the result is empty
		if len(target) == 0 {
			return nil, true, nil
		}

		// Convert input to string
		s, ok, err := Singleton[String](target)
		if err != nil {
			return nil, false, err
		}
		if !ok {
			return nil, true, nil
		}

		// Evaluate the format parameter
		formatCollection, _, err := evaluate(ctx, nil, parameters[0], nil)
		if err != nil {
			return nil, false, err
		}

		// Convert format to string
		format, ok, err := Singleton[String](formatCollection)
		if err != nil {
			return nil, false, err
		}
		if !ok {
			return nil, false, fmt.Errorf("expected string format parameter")
		}

		// Decode according to format
		switch string(format) {
		case "hex":
			// Convert from hex
			decoded, err := hex.DecodeString(string(s))
			if err != nil {
				return nil, false, fmt.Errorf("invalid hex string: %v", err)
			}
			return Collection{String(decoded)}, true, nil
		case "base64":
			// Convert from base64
			decoded, err := base64.StdEncoding.DecodeString(string(s))
			if err != nil {
				return nil, false, fmt.Errorf("invalid base64 string: %v", err)
			}
			return Collection{String(decoded)}, true, nil
		case "urlbase64":
			// Convert from URL-safe base64
			decoded, err := base64.URLEncoding.DecodeString(string(s))
			if err != nil {
				return nil, false, fmt.Errorf("invalid URL-safe base64 string: %v", err)
			}
			return Collection{String(decoded)}, true, nil
		default:
			return nil, false, fmt.Errorf("unsupported encoding format: %s", format)
		}
	},
	"escape": func(
		ctx context.Context,
		root Element, target Collection,
		inputOrdered bool,
		parameters []Expression,
		evaluate EvaluateFunc,
	) (result Collection, resultOrdered bool, err error) {
		if len(parameters) != 1 {
			return nil, false, fmt.Errorf("expected single target parameter")
		}

		// If the input collection is empty, the result is empty
		if len(target) == 0 {
			return nil, true, nil
		}

		// Convert input to string
		s, ok, err := Singleton[String](target)
		if err != nil {
			return nil, false, err
		}
		if !ok {
			return nil, true, nil
		}

		// Evaluate the target parameter
		targetCollection, _, err := evaluate(ctx, nil, parameters[0], nil)
		if err != nil {
			return nil, false, err
		}

		// Convert target to string
		targetStr, ok, err := Singleton[String](targetCollection)
		if err != nil {
			return nil, false, err
		}
		if !ok {
			return nil, false, fmt.Errorf("expected string target parameter")
		}

		// Escape according to target
		switch string(targetStr) {
		case "html":
			// Per FHIRPath spec: escape <, &, " and ideally anything with character encoding above 127
			var result strings.Builder
			result.Grow(len(s))
			for _, r := range string(s) {
				switch r {
				case '<':
					result.WriteString("&lt;")
				case '>':
					result.WriteString("&gt;")
				case '&':
					result.WriteString("&amp;")
				case '"':
					result.WriteString("&quot;")
				case '\'':
					result.WriteString("&#39;")
				default:
					// Escape high Unicode characters (above 127)
					if r > 127 {
						result.WriteString(fmt.Sprintf("&#%d;", r))
					} else {
						result.WriteRune(r)
					}
				}
			}
			return Collection{String(result.String())}, true, nil
		case "json":
			// Escape JSON special characters per FHIRPath spec
			// We need to escape quotes and backslashes, but NOT < > & (unlike Go's default json.Marshal)
			var result strings.Builder
			for _, r := range string(s) {
				switch r {
				case '"':
					result.WriteString(`\"`)
				case '\\':
					result.WriteString(`\\`)
				case '\n':
					result.WriteString(`\n`)
				case '\r':
					result.WriteString(`\r`)
				case '\t':
					result.WriteString(`\t`)
				case '\b':
					result.WriteString(`\b`)
				case '\f':
					result.WriteString(`\f`)
				default:
					result.WriteRune(r)
				}
			}
			return Collection{String(result.String())}, true, nil
		default:
			return nil, false, fmt.Errorf("unsupported escape target: %s", targetStr)
		}
	},
	"unescape": func(
		ctx context.Context,
		root Element, target Collection,
		inputOrdered bool,
		parameters []Expression,
		evaluate EvaluateFunc,
	) (result Collection, resultOrdered bool, err error) {
		if len(parameters) != 1 {
			return nil, false, fmt.Errorf("expected single target parameter")
		}

		// If the input collection is empty, the result is empty
		if len(target) == 0 {
			return nil, true, nil
		}

		// Convert input to string
		s, ok, err := Singleton[String](target)
		if err != nil {
			return nil, false, err
		}
		if !ok {
			return nil, true, nil
		}

		// Evaluate the target parameter
		targetCollection, _, err := evaluate(ctx, nil, parameters[0], nil)
		if err != nil {
			return nil, false, err
		}

		// Convert target to string
		targetStr, ok, err := Singleton[String](targetCollection)
		if err != nil {
			return nil, false, err
		}
		if !ok {
			return nil, false, fmt.Errorf("expected string target parameter")
		}

		// Unescape according to target
		switch string(targetStr) {
		case "html":
			// Unescape HTML entities
			unescaped := html.UnescapeString(string(s))
			return Collection{String(unescaped)}, true, nil
		case "json":
			// Unescape JSON string
			// The input string may contain JSON escape sequences like \", \\, \n, \t, \r, \b, \f, \/
			// We need to interpret these escape sequences manually
			var result strings.Builder
			input := string(s)
			for i := 0; i < len(input); i++ {
				if input[i] == '\\' && i+1 < len(input) {
					// Process escape sequence
					switch input[i+1] {
					case '"':
						result.WriteByte('"')
						i++ // Skip next char
					case '\\':
						result.WriteByte('\\')
						i++
					case '/':
						result.WriteByte('/')
						i++
					case 'b':
						result.WriteByte('\b')
						i++
					case 'f':
						result.WriteByte('\f')
						i++
					case 'n':
						result.WriteByte('\n')
						i++
					case 'r':
						result.WriteByte('\r')
						i++
					case 't':
						result.WriteByte('\t')
						i++
					case 'u':
						// Unicode escape \uXXXX
						if i+5 < len(input) {
							// Parse hex digits
							hexStr := input[i+2 : i+6]
							var codePoint int
							_, err := fmt.Sscanf(hexStr, "%x", &codePoint)
							if err == nil {
								result.WriteRune(rune(codePoint))
								i += 5 // Skip u and 4 hex digits
							} else {
								// Invalid unicode escape, keep as-is
								result.WriteByte(input[i])
							}
						} else {
							// Not enough characters for unicode escape
							result.WriteByte(input[i])
						}
					default:
						// Unknown escape sequence, keep the backslash
						result.WriteByte(input[i])
					}
				} else {
					result.WriteByte(input[i])
				}
			}
			return Collection{String(result.String())}, true, nil
		default:
			return nil, false, fmt.Errorf("unsupported unescape target: %s", targetStr)
		}
	},
	"lowBoundary": func(
		ctx context.Context,
		root Element, target Collection,
		inputOrdered bool,
		parameters []Expression,
		evaluate EvaluateFunc,
	) (result Collection, resultOrdered bool, err error) {
		if len(target) == 0 {
			return nil, true, nil
		}
		if len(target) > 1 {
			return nil, false, fmt.Errorf("expected single input element")
		}
		if len(parameters) > 1 {
			return nil, false, fmt.Errorf("expected at most one precision parameter")
		}

		var (
			precisionOverride int
			precisionProvided bool
		)
		if len(parameters) == 1 {
			precisionCollection, _, err := evaluate(ctx, nil, parameters[0], nil)
			if err != nil {
				return nil, false, err
			}
			prec, ok, err := Singleton[Integer](precisionCollection)
			if err != nil {
				return nil, false, err
			}
			if !ok {
				return nil, false, fmt.Errorf("expected integer precision parameter")
			}
			precisionOverride = int(prec)
			precisionProvided = true
		}

		if value, ok, err := Singleton[Decimal](target); err == nil && ok {
			var outputPrecision *int
			if precisionProvided {
				p := precisionOverride
				if p < 0 || p > 31 {
					return nil, true, nil
				}
				outputPrecision = &p
			}
			boundary, err := value.LowBoundary(ctx, outputPrecision)
			if err != nil {
				return nil, false, err
			}
			return Collection{boundary}, true, nil
		}

		if qty, ok, err := Singleton[Quantity](target); err == nil && ok {
			var outputPrecision *int
			if precisionProvided {
				p := precisionOverride
				if p < 0 || p > 31 {
					return nil, true, nil
				}
				outputPrecision = &p
			}
			boundary, err := qty.Value.LowBoundary(ctx, outputPrecision)
			if err != nil {
				return nil, false, err
			}
			resultQuantity := qty
			resultQuantity.Value = boundary
			return Collection{resultQuantity}, true, nil
		}

		if value, ok, err := Singleton[Date](target); err == nil && ok {
			var digits *int
			if precisionProvided {
				p := precisionOverride
				digits = &p
			}
			resultDate, ok := value.LowBoundary(digits)
			if !ok {
				return nil, true, nil
			}
			return Collection{resultDate}, true, nil
		}

		if value, ok, err := Singleton[DateTime](target); err == nil && ok {
			var digits *int
			if precisionProvided {
				p := precisionOverride
				digits = &p
			}
			resultDateTime, ok := value.LowBoundary(digits)
			if !ok {
				return nil, true, nil
			}
			return Collection{resultDateTime}, true, nil
		}

		if value, ok, err := Singleton[Time](target); err == nil && ok {
			var digits *int
			if precisionProvided {
				p := precisionOverride
				digits = &p
			}
			resultTime, ok := value.LowBoundary(digits)
			if !ok {
				return nil, true, nil
			}
			return Collection{resultTime}, true, nil
		}

		return nil, false, fmt.Errorf("expected Decimal, Quantity, Date, DateTime, or Time but got %T", target[0])
	},
	"highBoundary": func(
		ctx context.Context,
		root Element, target Collection,
		inputOrdered bool,
		parameters []Expression,
		evaluate EvaluateFunc,
	) (result Collection, resultOrdered bool, err error) {
		if len(target) == 0 {
			return nil, true, nil
		}
		if len(target) > 1 {
			return nil, false, fmt.Errorf("expected single input element")
		}
		if len(parameters) > 1 {
			return nil, false, fmt.Errorf("expected at most one precision parameter")
		}

		var (
			precisionOverride int
			precisionProvided bool
		)
		if len(parameters) == 1 {
			precisionCollection, _, err := evaluate(ctx, nil, parameters[0], nil)
			if err != nil {
				return nil, false, err
			}
			prec, ok, err := Singleton[Integer](precisionCollection)
			if err != nil {
				return nil, false, err
			}
			if !ok {
				return nil, false, fmt.Errorf("expected integer precision parameter")
			}
			precisionOverride = int(prec)
			precisionProvided = true
		}

		if value, ok, err := Singleton[Decimal](target); err == nil && ok {
			var outputPrecision *int
			if precisionProvided {
				p := precisionOverride
				if p < 0 || p > 31 {
					return nil, true, nil
				}
				outputPrecision = &p
			}
			boundary, err := value.HighBoundary(ctx, outputPrecision)
			if err != nil {
				return nil, false, err
			}
			return Collection{boundary}, true, nil
		}

		if qty, ok, err := Singleton[Quantity](target); err == nil && ok {
			var outputPrecision *int
			if precisionProvided {
				p := precisionOverride
				if p < 0 || p > 31 {
					return nil, true, nil
				}
				outputPrecision = &p
			}
			boundary, err := qty.Value.HighBoundary(ctx, outputPrecision)
			if err != nil {
				return nil, false, err
			}
			resultQuantity := qty
			resultQuantity.Value = boundary
			return Collection{resultQuantity}, true, nil
		}

		if value, ok, err := Singleton[Date](target); err == nil && ok {
			var digits *int
			if precisionProvided {
				p := precisionOverride
				digits = &p
			}
			resultDate, ok := value.HighBoundary(digits)
			if !ok {
				return nil, true, nil
			}
			return Collection{resultDate}, true, nil
		}

		if value, ok, err := Singleton[DateTime](target); err == nil && ok {
			var digits *int
			if precisionProvided {
				p := precisionOverride
				digits = &p
			}
			resultDateTime, ok := value.HighBoundary(digits)
			if !ok {
				return nil, true, nil
			}
			return Collection{resultDateTime}, true, nil
		}

		if value, ok, err := Singleton[Time](target); err == nil && ok {
			var digits *int
			if precisionProvided {
				p := precisionOverride
				digits = &p
			}
			resultTime, ok := value.HighBoundary(digits)
			if !ok {
				return nil, true, nil
			}
			return Collection{resultTime}, true, nil
		}

		return nil, false, fmt.Errorf("expected Decimal, Quantity, Date, DateTime, or Time but got %T", target[0])
	},
	"precision": func(
		ctx context.Context,
		root Element, target Collection,
		inputOrdered bool,
		parameters []Expression,
		evaluate EvaluateFunc,
	) (result Collection, resultOrdered bool, err error) {
		if len(parameters) != 0 {
			return nil, false, fmt.Errorf("expected no parameters")
		}

		// If the input collection is empty, the result is empty
		if len(target) == 0 {
			return nil, true, nil
		}

		// If the input collection contains multiple items, signal an error
		if len(target) > 1 {
			return nil, false, fmt.Errorf("expected single input element")
		}

		// Handle Decimal type
		if value, ok, err := Singleton[Decimal](target); err == nil && ok {
			// Use the Decimal.Precision() method which returns decimal places based on exponent
			precision := value.Precision()
			return Collection{Integer(precision)}, true, nil
		}

		if value, ok, err := Singleton[Date](target); err == nil && ok {
			digits := value.PrecisionDigits()
			return Collection{Integer(digits)}, true, nil
		}

		if value, ok, err := Singleton[DateTime](target); err == nil && ok {
			digits := value.PrecisionDigits()
			return Collection{Integer(digits)}, true, nil
		}

		if value, ok, err := Singleton[Time](target); err == nil && ok {
			digits := value.PrecisionDigits()
			return Collection{Integer(digits)}, true, nil
		}

		return nil, false, fmt.Errorf("expected Decimal, Date, DateTime, or Time but got %T", target[0])
	},

	"duration": func(
		ctx context.Context,
		root Element, target Collection,
		inputOrdered bool,
		parameters []Expression,
		evaluate EvaluateFunc,
	) (result Collection, resultOrdered bool, err error) {
		if len(parameters) != 2 {
			return nil, false, fmt.Errorf("expected 2 parameters (value, precision)")
		}

		// If the input collection is empty, the result is empty
		if len(target) == 0 {
			return nil, true, nil
		}

		// If the input collection contains multiple items, signal an error
		if len(target) > 1 {
			return nil, false, fmt.Errorf("expected single input element")
		}

		// Evaluate the value parameter
		valueResult, _, err := evaluate(ctx, nil, parameters[0], nil)
		if err != nil {
			return nil, false, err
		}
		if len(valueResult) == 0 {
			return nil, true, nil
		}
		if len(valueResult) > 1 {
			return nil, false, fmt.Errorf("value parameter must return single element")
		}

		// Evaluate the precision parameter
		precisionResult, _, err := evaluate(ctx, nil, parameters[1], nil)
		if err != nil {
			return nil, false, err
		}
		if len(precisionResult) == 0 {
			return nil, true, nil
		}
		precisionStr, ok, err := precisionResult[0].ToString(false)
		if err != nil || !ok {
			return nil, false, fmt.Errorf("precision parameter must be a string")
		}

		precision := normalizeTimeUnit(string(precisionStr))

		// Handle Date types
		if startDate, ok, _ := Singleton[Date](target); ok {
			endDate, ok, _ := Singleton[Date](valueResult)
			if !ok {
				return nil, false, fmt.Errorf("duration requires matching types")
			}
			return calculateDateDuration(startDate, endDate, precision)
		}

		// Handle DateTime types
		if startDT, ok, _ := Singleton[DateTime](target); ok {
			endDT, ok, _ := Singleton[DateTime](valueResult)
			if !ok {
				return nil, false, fmt.Errorf("duration requires matching types")
			}
			return calculateDateTimeDuration(startDT, endDT, precision)
		}

		// Handle Time types
		if startTime, ok, _ := Singleton[Time](target); ok {
			endTime, ok, _ := Singleton[Time](valueResult)
			if !ok {
				return nil, false, fmt.Errorf("duration requires matching types")
			}
			return calculateTimeDuration(startTime, endTime, precision)
		}

		return nil, false, fmt.Errorf("duration requires Date, DateTime, or Time input")
	},

	"difference": func(
		ctx context.Context,
		root Element, target Collection,
		inputOrdered bool,
		parameters []Expression,
		evaluate EvaluateFunc,
	) (result Collection, resultOrdered bool, err error) {
		if len(parameters) != 2 {
			return nil, false, fmt.Errorf("expected 2 parameters (value, precision)")
		}

		// If the input collection is empty, the result is empty
		if len(target) == 0 {
			return nil, true, nil
		}

		// If the input collection contains multiple items, signal an error
		if len(target) > 1 {
			return nil, false, fmt.Errorf("expected single input element")
		}

		// Evaluate the value parameter
		valueResult, _, err := evaluate(ctx, nil, parameters[0], nil)
		if err != nil {
			return nil, false, err
		}
		if len(valueResult) == 0 {
			return nil, true, nil
		}
		if len(valueResult) > 1 {
			return nil, false, fmt.Errorf("value parameter must return single element")
		}

		// Evaluate the precision parameter
		precisionResult, _, err := evaluate(ctx, nil, parameters[1], nil)
		if err != nil {
			return nil, false, err
		}
		if len(precisionResult) == 0 {
			return nil, true, nil
		}
		precisionStr, ok, err := precisionResult[0].ToString(false)
		if err != nil || !ok {
			return nil, false, fmt.Errorf("precision parameter must be a string")
		}

		precision := normalizeTimeUnit(string(precisionStr))

		// Handle Date types
		if startDate, ok, _ := Singleton[Date](target); ok {
			endDate, ok, _ := Singleton[Date](valueResult)
			if !ok {
				return nil, false, fmt.Errorf("difference requires matching types")
			}
			return calculateDateDifference(startDate, endDate, precision)
		}

		// Handle DateTime types
		if startDT, ok, _ := Singleton[DateTime](target); ok {
			endDT, ok, _ := Singleton[DateTime](valueResult)
			if !ok {
				return nil, false, fmt.Errorf("difference requires matching types")
			}
			return calculateDateTimeDifference(startDT, endDT, precision)
		}

		// Handle Time types
		if startTime, ok, _ := Singleton[Time](target); ok {
			endTime, ok, _ := Singleton[Time](valueResult)
			if !ok {
				return nil, false, fmt.Errorf("difference requires matching types")
			}
			return calculateTimeDifference(startTime, endTime, precision)
		}

		return nil, false, fmt.Errorf("difference requires Date, DateTime, or Time input")
	},

	"defineVariable": func(
		ctx context.Context,
		root Element, target Collection,
		inputOrdered bool,
		parameters []Expression,
		evaluate EvaluateFunc,
	) (result Collection, resultOrdered bool, err error) {
		if len(parameters) != 1 && len(parameters) != 2 {
			return nil, false, fmt.Errorf("expected one or two parameters (name [, value])")
		}

		nameCollection, _, err := evaluate(ctx, nil, parameters[0], nil)
		if err != nil {
			return nil, false, err
		}

		// Convert name to string
		name, ok, err := Singleton[String](nameCollection)
		if err != nil {
			return nil, false, err
		}
		if !ok {
			return nil, false, fmt.Errorf("expected string name parameter")
		}

		// Protect system variables from being overwritten
		if _, isSystem := systemVariables[string(name)]; isSystem {
			return nil, false, fmt.Errorf("cannot redefine system variable '%s'", name)
		}

		// Check if variable already defined in current scope
		if frame, ok := envStackFrame(ctx); ok {
			if _, exists := frame[string(name)]; exists {
				return nil, false, fmt.Errorf("variable %%%s already defined", name)
			}
		}

		// Determine the value to store
		// Variables in FHIRPath store the entire evaluated result (which can be a collection)
		value := target
		if len(parameters) == 2 {
			// FHIRPath STU defineVariable: the value expression is evaluated once using the
			// current input collection as its starting point (Functions - Utility section).
			value, _, err = evaluate(ctx, target, parameters[1], nil)
			if err != nil {
				return nil, false, err
			}
		}

		// Store the collection as the variable value in the parent context
		ctx = WithEnv(ctx, string(name), value)

		// Return the input collection (does not change input)
		return target, inputOrdered, nil
	},

	// Math functions
	"abs": func(
		ctx context.Context,
		root Element, target Collection,
		inputOrdered bool,
		parameters []Expression,
		evaluate EvaluateFunc,
	) (result Collection, resultOrdered bool, err error) {
		if len(parameters) != 0 {
			return nil, false, fmt.Errorf("expected no parameters")
		}
		if len(target) == 0 {
			// FHIRPath v3.0.0 Math functions: empty input yields empty result.
			return nil, true, nil
		}
		if len(target) > 1 {
			return nil, false, fmt.Errorf("abs() expects a single input element")
		}

		i, ok, err := Singleton[Integer](target)
		if err == nil && ok {
			if i < 0 {
				return Collection{-i}, true, nil
			}
			return Collection{i}, true, nil
		}

		d, ok, err := Singleton[Decimal](target)
		if err == nil && ok {
			// Create a new Decimal with the absolute value
			var absValue apd.Decimal
			absValue.Abs(d.Value)
			return Collection{Decimal{Value: &absValue}}, true, nil
		}

		q, ok, err := Singleton[Quantity](target)
		if err == nil && ok {
			// Create a new Quantity with the absolute value of the value
			var absValue apd.Decimal
			absValue.Abs(q.Value.Value)
			return Collection{Quantity{Value: Decimal{Value: &absValue}, Unit: q.Unit}}, true, nil
		}

		return nil, false, fmt.Errorf("expected Integer, Decimal, or Quantity but got %T", target[0])
	},
	"ceiling": func(
		ctx context.Context,
		root Element, target Collection,
		inputOrdered bool,
		parameters []Expression,
		evaluate EvaluateFunc,
	) (result Collection, resultOrdered bool, err error) {
		if len(parameters) != 0 {
			return nil, false, fmt.Errorf("expected no parameters")
		}
		if len(target) == 0 {
			// FHIRPath v3.0.0 Math functions: empty input yields empty result.
			return nil, true, nil
		}
		if len(target) > 1 {
			return nil, false, fmt.Errorf("ceiling() expects a single input element")
		}

		i, ok, err := Singleton[Integer](target)
		if err == nil && ok {
			// Integer is already a whole number, so ceiling is the same
			return Collection{i}, true, nil
		}

		d, ok, err := Singleton[Decimal](target)
		if err == nil && ok {
			// Get the integer part
			var intPart apd.Decimal
			_, err = apdContext(ctx).Ceil(&intPart, d.Value)
			if err != nil {
				return nil, false, err
			}

			intVal, err := intPart.Int64()
			if err != nil {
				return nil, false, err
			}

			return Collection{Integer(intVal)}, true, nil
		}

		return nil, false, fmt.Errorf("expected Integer or Decimal but got %T", target[0])
	},
	"floor": func(
		ctx context.Context,
		root Element, target Collection,
		inputOrdered bool,
		parameters []Expression,
		evaluate EvaluateFunc,
	) (result Collection, resultOrdered bool, err error) {
		if len(parameters) != 0 {
			return nil, false, fmt.Errorf("expected no parameters")
		}
		if len(target) == 0 {
			// FHIRPath v3.0.0 Math functions: empty input yields empty result.
			return nil, true, nil
		}
		if len(target) > 1 {
			return nil, false, fmt.Errorf("floor() expects a single input element")
		}

		i, ok, err := Singleton[Integer](target)
		if err == nil && ok {
			// Integer is already a whole number, so floor is the same
			return Collection{i}, true, nil
		}

		d, ok, err := Singleton[Decimal](target)
		if err == nil && ok {
			// Get the integer part
			var intPart apd.Decimal
			_, err = apdContext(ctx).Floor(&intPart, d.Value)
			if err != nil {
				return nil, false, err
			}

			// Convert to Integer
			intVal, err := intPart.Int64()
			if err != nil {
				return nil, false, err
			}

			return Collection{Integer(intVal)}, true, nil
		}

		return nil, false, fmt.Errorf("expected Integer or Decimal but got %T", target[0])
	},
	"truncate": func(
		ctx context.Context,
		root Element, target Collection,
		inputOrdered bool,
		parameters []Expression,
		evaluate EvaluateFunc,
	) (result Collection, resultOrdered bool, err error) {
		if len(parameters) != 0 {
			return nil, false, fmt.Errorf("expected no parameters")
		}
		if len(target) == 0 {
			// FHIRPath v3.0.0 Math functions: empty input yields empty result.
			return nil, true, nil
		}
		if len(target) > 1 {
			return nil, false, fmt.Errorf("truncate() expects a single input element")
		}

		i, ok, err := Singleton[Integer](target)
		if err == nil && ok {
			// Integer is already a whole number, so truncate is the same
			return Collection{i}, true, nil
		}

		d, ok, err := Singleton[Decimal](target)
		if err == nil && ok {
			// Get the integer part
			var intPart apd.Decimal

			// Use Floor for positive numbers and Ceil for negative numbers
			if d.Value.Negative {
				_, err = apdContext(ctx).Ceil(&intPart, d.Value)
			} else {
				_, err = apdContext(ctx).Floor(&intPart, d.Value)
			}

			if err != nil {
				return nil, false, err
			}

			// Convert to Integer
			intVal, err := intPart.Int64()
			if err != nil {
				return nil, false, err
			}

			return Collection{Integer(intVal)}, true, nil
		}

		return nil, false, fmt.Errorf("expected Integer or Decimal but got %T", target[0])
	},
	"round": func(
		ctx context.Context,
		root Element, target Collection,
		inputOrdered bool,
		parameters []Expression,
		evaluate EvaluateFunc,
	) (result Collection, resultOrdered bool, err error) {
		if len(parameters) > 1 {
			return nil, false, fmt.Errorf("expected at most one precision parameter")
		}

		if len(target) == 0 {
			return nil, true, nil
		}
		if len(target) > 1 {
			return nil, false, fmt.Errorf("expected single input element")
		}

		decimalPlaces := int64(0)
		if len(parameters) == 1 {
			c, _, err := evaluate(ctx, nil, parameters[0], nil)
			if err != nil {
				return nil, false, err
			}

			decimalPlacesValue, ok, err := Singleton[Integer](c)
			if err != nil {
				return nil, false, err
			}
			if !ok {
				return nil, false, fmt.Errorf("expected integer precision parameter")
			}

			if decimalPlacesValue < 0 {
				return nil, false, fmt.Errorf("precision must be >= 0")
			}

			decimalPlaces = int64(decimalPlacesValue)
		}

		var dec *apd.Decimal
		// Convert Integer to Decimal if needed
		if i, ok, _ := Singleton[Integer](target); ok {
			dec = apd.New(int64(i), 0)
		} else if d, ok, _ := Singleton[Decimal](target); ok {
			dec = d.Value
		} else {
			return nil, false, fmt.Errorf("expected Integer or Decimal but got %T", target[0])
		}

		// Set precision for rounding
		apdCtx := apdContext(ctx).WithPrecision(uint32(dec.NumDigits() + decimalPlaces))
		var rounded apd.Decimal
		// Use Quantize to round to the specified number of decimal places
		_, err = apdCtx.Quantize(&rounded, dec, int32(-decimalPlaces))
		if err != nil {
			return nil, false, err
		}

		return Collection{Decimal{Value: &rounded}}, true, nil
	},
	"exp": func(
		ctx context.Context,
		root Element, target Collection,
		inputOrdered bool,
		parameters []Expression,
		evaluate EvaluateFunc,
	) (result Collection, resultOrdered bool, err error) {
		if len(parameters) != 0 {
			return nil, false, fmt.Errorf("expected no parameters")
		}
		if len(target) == 0 {
			// FHIRPath v3.0.0 Math functions: empty input yields empty result.
			return nil, true, nil
		}
		if len(target) > 1 {
			return nil, false, fmt.Errorf("exp() expects a single input element")
		}

		d, ok, err := Singleton[Decimal](target)
		if err == nil && ok {
			// Calculate e^x
			var result apd.Decimal
			_, err = apdContext(ctx).Exp(&result, d.Value)
			if err != nil {
				return nil, false, err
			}

			return Collection{Decimal{Value: &result}}, true, nil
		}

		return nil, false, fmt.Errorf("expected Integer or Decimal but got %T", target[0])
	},
	"ln": func(
		ctx context.Context,
		root Element, target Collection,
		inputOrdered bool,
		parameters []Expression,
		evaluate EvaluateFunc,
	) (result Collection, resultOrdered bool, err error) {
		if len(parameters) != 0 {
			return nil, false, fmt.Errorf("expected no parameters")
		}
		if len(target) == 0 {
			// FHIRPath v3.0.0 Math functions: empty input yields empty result.
			return nil, true, nil
		}
		if len(target) > 1 {
			return nil, false, fmt.Errorf("ln() expects a single input element")
		}

		d, ok, err := Singleton[Decimal](target)
		if err == nil && ok {
			// Calculate ln(x)
			var result apd.Decimal
			_, err = apdContext(ctx).Ln(&result, d.Value)
			if err != nil {
				return nil, false, err
			}

			return Collection{Decimal{Value: &result}}, true, nil
		}

		return nil, false, fmt.Errorf("expected Integer or Decimal but got %T", target[0])
	},
	"log": func(
		ctx context.Context,
		root Element, target Collection,
		inputOrdered bool,
		parameters []Expression,
		evaluate EvaluateFunc,
	) (result Collection, resultOrdered bool, err error) {
		if len(parameters) != 1 {
			return nil, false, fmt.Errorf("expected one base parameter")
		}

		if len(target) == 0 {
			// FHIRPath v3.0.0 Math functions: empty input yields empty result.
			return nil, true, nil
		}
		if len(target) > 1 {
			return nil, false, fmt.Errorf("log() expects a single input element")
		}

		baseCollection, _, err := evaluate(ctx, nil, parameters[0], nil)
		if err != nil {
			return nil, false, err
		}
		if len(baseCollection) == 0 {
			// FHIRPath v3.0.0 Math functions: empty arguments yield empty results.
			return nil, true, nil
		}

		baseDecimal, ok, err := Singleton[Decimal](baseCollection)
		if err != nil || !ok {
			// Try to convert Integer to Decimal
			baseInt, ok, err := Singleton[Integer](baseCollection)
			if err != nil || !ok {
				return nil, false, fmt.Errorf("expected Integer or Decimal base parameter but got %T", baseCollection[0])
			}
			baseDecimal = Decimal{Value: apd.New(int64(baseInt), 0)}
		}

		d, ok, err := Singleton[Decimal](target)
		if err == nil && ok {
			// Calculate log_base(x) = ln(x) / ln(base)
			var lnX, lnBase, result apd.Decimal
			_, err = apdContext(ctx).Ln(&lnX, d.Value)
			if err != nil {
				return nil, false, err
			}

			_, err = apdContext(ctx).Ln(&lnBase, baseDecimal.Value)
			if err != nil {
				return nil, false, err
			}

			_, err = apdContext(ctx).Quo(&result, &lnX, &lnBase)
			if err != nil {
				return nil, false, err
			}

			return Collection{Decimal{Value: &result}}, true, nil
		}

		return nil, false, fmt.Errorf("expected Integer or Decimal but got %T", target[0])
	},
	"power": func(
		ctx context.Context,
		root Element, target Collection,
		inputOrdered bool,
		parameters []Expression,
		evaluate EvaluateFunc,
	) (result Collection, resultOrdered bool, err error) {
		if len(parameters) != 1 {
			return nil, false, fmt.Errorf("expected one exponent parameter")
		}
		if len(target) == 0 {
			// FHIRPath v3.0.0 Math functions: empty input yields empty result.
			return nil, true, nil
		}
		if len(target) > 1 {
			return nil, false, fmt.Errorf("power() expects a single input element")
		}

		exponentCollection, _, err := evaluate(ctx, nil, parameters[0], nil)
		if err != nil {
			return nil, false, err
		}
		if len(exponentCollection) == 0 {
			// FHIRPath v3.0.0 Math functions: empty arguments yield empty results.
			return nil, true, nil
		}

		exponentInt, ok, err := Singleton[Integer](exponentCollection)
		if err == nil && ok {
			// Handle Integer base
			i, ok, err := Singleton[Integer](target)
			if err == nil && ok {
				// For Integer base and Integer exponent, return Integer if possible
				result := int64(math.Pow(float64(i), float64(exponentInt)))

				// Check if the result can be represented as an Integer
				if math.Pow(float64(i), float64(exponentInt)) == float64(result) {
					return Collection{Integer(result)}, true, nil
				}

				// Otherwise, return as Decimal
				resultDecimal := apd.New(0, 0)
				_, err := resultDecimal.SetFloat64(math.Pow(float64(i), float64(exponentInt)))
				if err != nil {
					return nil, false, err
				}
				return Collection{Decimal{Value: resultDecimal}}, true, nil
			}
		}

		// Get the exponent as a Decimal
		exponentDecimal, ok, err := Singleton[Decimal](exponentCollection)
		if err != nil || !ok {
			// Try to convert Integer to Decimal
			exponentInt, ok, err := Singleton[Integer](exponentCollection)
			if err != nil || !ok {
				return nil, false, fmt.Errorf("expected Integer or Decimal exponent parameter but got %T", exponentCollection[0])
			}
			exponentDecimal = Decimal{Value: apd.New(int64(exponentInt), 0)}
		}

		d, ok, err := Singleton[Decimal](target)
		if err == nil && ok {
			_, err := exponentDecimal.Value.Int64()
			// For negative base, the result is empty
			if d.Value.Negative {
				return nil, true, nil
			}

			// Calculate x^y
			var result apd.Decimal
			_, err = apdContext(ctx).Pow(&result, d.Value, exponentDecimal.Value)
			if err != nil {
				return nil, false, err
			}

			return Collection{Decimal{Value: &result}}, true, nil
		}

		return nil, false, fmt.Errorf("expected Integer or Decimal but got %T", target[0])
	},
	"sqrt": func(
		ctx context.Context,
		root Element, target Collection,
		inputOrdered bool,
		parameters []Expression,
		evaluate EvaluateFunc,
	) (result Collection, resultOrdered bool, err error) {
		if len(parameters) != 0 {
			return nil, false, fmt.Errorf("expected no parameters")
		}
		if len(target) == 0 {
			// FHIRPath v3.0.0 Math functions: empty input yields empty result.
			return nil, true, nil
		}
		if len(target) > 1 {
			return nil, false, fmt.Errorf("sqrt() expects a single input element")
		}

		d, ok, err := Singleton[Decimal](target)
		if err == nil && ok {
			if d.Value.Negative {
				return nil, true, nil
			}

			var result apd.Decimal
			_, err = apdContext(ctx).Sqrt(&result, d.Value)
			if err != nil {
				return nil, false, err
			}

			return Collection{Decimal{Value: &result}}, true, nil
		}

		return nil, false, fmt.Errorf("expected Integer or Decimal but got %T", target[0])
	},

	// Type conversion functions
	"toBoolean": func(
		ctx context.Context,
		root Element, target Collection,
		inputOrdered bool,
		parameters []Expression,
		evaluate EvaluateFunc,
	) (result Collection, resultOrdered bool, err error) {
		if len(parameters) != 0 {
			return nil, false, fmt.Errorf("expected no parameters")
		}

		// Convert to boolean
		if len(target) == 0 {
			return nil, true, nil
		} else if len(target) > 1 {
			return nil, false, fmt.Errorf("cannot convert to boolean: collection contains > 1 values")
		}

		b, ok, err := elementTo[Boolean](target[0], true)
		if err != nil || !ok {
			return nil, true, nil
		}

		return Collection{b}, true, nil
	},
	"convertsToBoolean": func(
		ctx context.Context,
		root Element, target Collection,
		inputOrdered bool,
		parameters []Expression,
		evaluate EvaluateFunc,
	) (result Collection, resultOrdered bool, err error) {
		if len(parameters) != 0 {
			return nil, false, fmt.Errorf("expected no parameters")
		}

		// Check if convertible to boolean
		if len(target) == 0 {
			return Collection{Boolean(false)}, true, nil
		} else if len(target) > 1 {
			return nil, false, fmt.Errorf("cannot convert to boolean: collection contains > 1 values")
		}

		_, ok, err := elementTo[Boolean](target[0], true)
		if err != nil || !ok {
			return Collection{Boolean(false)}, true, nil
		}

		return Collection{Boolean(true)}, true, nil
	},
	"toInteger": func(
		ctx context.Context,
		root Element, target Collection,
		inputOrdered bool,
		parameters []Expression,
		evaluate EvaluateFunc,
	) (result Collection, resultOrdered bool, err error) {
		if len(parameters) != 0 {
			return nil, false, fmt.Errorf("expected no parameters")
		}

		// Convert to integer
		if len(target) == 0 {
			return nil, true, nil
		} else if len(target) > 1 {
			return nil, false, fmt.Errorf("cannot convert to integer: collection contains > 1 values")
		}

		i, ok, err := elementTo[Integer](target[0], true)
		if err != nil || !ok {
			return nil, true, nil
		}

		return Collection{i}, true, nil
	},
	"convertsToInteger": func(
		ctx context.Context,
		root Element, target Collection,
		inputOrdered bool,
		parameters []Expression,
		evaluate EvaluateFunc,
	) (result Collection, resultOrdered bool, err error) {
		if len(parameters) != 0 {
			return nil, false, fmt.Errorf("expected no parameters")
		}

		// Check if convertible to integer
		if len(target) == 0 {
			return Collection{Boolean(false)}, true, nil
		} else if len(target) > 1 {
			return nil, false, fmt.Errorf("cannot convert to integer: collection contains > 1 values")
		}

		_, ok, err := elementTo[Integer](target[0], true)
		if err != nil || !ok {
			return Collection{Boolean(false)}, true, nil
		}

		return Collection{Boolean(true)}, true, nil
	},
	"toLong": func(
		ctx context.Context,
		root Element, target Collection,
		inputOrdered bool,
		parameters []Expression,
		evaluate EvaluateFunc,
	) (result Collection, resultOrdered bool, err error) {
		if len(parameters) != 0 {
			return nil, false, fmt.Errorf("expected no parameters")
		}

		if len(target) == 0 {
			return nil, true, nil
		} else if len(target) > 1 {
			return nil, false, fmt.Errorf("cannot convert to long: collection contains > 1 values")
		}

		l, ok, err := target[0].ToLong(true)
		if err != nil || !ok {
			return nil, true, nil
		}

		return Collection{l}, true, nil
	},
	"convertsToLong": func(
		ctx context.Context,
		root Element, target Collection,
		inputOrdered bool,
		parameters []Expression,
		evaluate EvaluateFunc,
	) (result Collection, resultOrdered bool, err error) {
		if len(parameters) != 0 {
			return nil, false, fmt.Errorf("expected no parameters")
		}

		if len(target) == 0 {
			return Collection{Boolean(false)}, true, nil
		} else if len(target) > 1 {
			return nil, false, fmt.Errorf("cannot convert to long: collection contains > 1 values")
		}

		_, ok, err := target[0].ToLong(true)
		if err != nil || !ok {
			return Collection{Boolean(false)}, true, nil
		}

		return Collection{Boolean(true)}, true, nil
	},
	"toDate": func(
		ctx context.Context,
		root Element, target Collection,
		inputOrdered bool,
		parameters []Expression,
		evaluate EvaluateFunc,
	) (result Collection, resultOrdered bool, err error) {
		if len(parameters) != 0 {
			return nil, false, fmt.Errorf("expected no parameters")
		}

		// Convert to date
		if len(target) == 0 {
			return nil, true, nil
		} else if len(target) > 1 {
			return nil, false, fmt.Errorf("cannot convert to date: collection contains > 1 values")
		}

		d, ok, err := elementTo[Date](target[0], true)
		if err != nil || !ok {
			return nil, true, nil
		}

		return Collection{d}, true, nil
	},
	"convertsToDate": func(
		ctx context.Context,
		root Element, target Collection,
		inputOrdered bool,
		parameters []Expression,
		evaluate EvaluateFunc,
	) (result Collection, resultOrdered bool, err error) {
		if len(parameters) != 0 {
			return nil, false, fmt.Errorf("expected no parameters")
		}

		// Check if convertible to date
		if len(target) == 0 {
			return Collection{Boolean(false)}, true, nil
		} else if len(target) > 1 {
			return nil, false, fmt.Errorf("cannot convert to date: collection contains > 1 values")
		}

		_, ok, err := elementTo[Date](target[0], true)
		if err != nil || !ok {
			return Collection{Boolean(false)}, true, nil
		}

		return Collection{Boolean(true)}, true, nil
	},
	"toDateTime": func(
		ctx context.Context,
		root Element, target Collection,
		inputOrdered bool,
		parameters []Expression,
		evaluate EvaluateFunc,
	) (result Collection, resultOrdered bool, err error) {
		if len(parameters) != 0 {
			return nil, false, fmt.Errorf("expected no parameters")
		}

		// Convert to datetime
		if len(target) == 0 {
			return nil, true, nil
		} else if len(target) > 1 {
			return nil, false, fmt.Errorf("cannot convert to datetime: collection contains > 1 values")
		}

		dt, ok, err := elementTo[DateTime](target[0], true)
		if err != nil || !ok {
			return nil, true, nil
		}

		return Collection{dt}, true, nil
	},
	"convertsToDateTime": func(
		ctx context.Context,
		root Element, target Collection,
		inputOrdered bool,
		parameters []Expression,
		evaluate EvaluateFunc,
	) (result Collection, resultOrdered bool, err error) {
		if len(parameters) != 0 {
			return nil, false, fmt.Errorf("expected no parameters")
		}

		// Check if convertible to datetime
		if len(target) == 0 {
			return Collection{Boolean(false)}, true, nil
		} else if len(target) > 1 {
			return nil, false, fmt.Errorf("cannot convert to datetime: collection contains > 1 values")
		}

		_, ok, err := elementTo[DateTime](target[0], true)
		if err != nil || !ok {
			return Collection{Boolean(false)}, true, nil
		}

		return Collection{Boolean(true)}, true, nil
	},
	"toTime": func(
		ctx context.Context,
		root Element, target Collection,
		inputOrdered bool,
		parameters []Expression,
		evaluate EvaluateFunc,
	) (result Collection, resultOrdered bool, err error) {
		if len(parameters) != 0 {
			return nil, false, fmt.Errorf("expected no parameters")
		}

		// Convert to time
		if len(target) == 0 {
			return nil, true, nil
		} else if len(target) > 1 {
			return nil, false, fmt.Errorf("cannot convert to time: collection contains > 1 values")
		}

		t, ok, err := elementTo[Time](target[0], true)
		if err != nil || !ok {
			return nil, true, nil
		}

		return Collection{t}, true, nil
	},
	"convertsToTime": func(
		ctx context.Context,
		root Element, target Collection,
		inputOrdered bool,
		parameters []Expression,
		evaluate EvaluateFunc,
	) (result Collection, resultOrdered bool, err error) {
		if len(parameters) != 0 {
			return nil, false, fmt.Errorf("expected no parameters")
		}

		// Check if convertible to time
		if len(target) == 0 {
			return Collection{Boolean(false)}, true, nil
		} else if len(target) > 1 {
			return nil, false, fmt.Errorf("cannot convert to time: collection contains > 1 values")
		}

		_, ok, err := elementTo[Time](target[0], true)
		if err != nil || !ok {
			return Collection{Boolean(false)}, true, nil
		}

		return Collection{Boolean(true)}, true, nil
	},
	"toDecimal": func(
		ctx context.Context,
		root Element, target Collection,
		inputOrdered bool,
		parameters []Expression,
		evaluate EvaluateFunc,
	) (result Collection, resultOrdered bool, err error) {
		if len(parameters) != 0 {
			return nil, false, fmt.Errorf("expected no parameters")
		}

		// Convert to decimal
		if len(target) == 0 {
			return nil, true, nil
		} else if len(target) > 1 {
			return nil, false, fmt.Errorf("cannot convert to decimal: collection contains > 1 values")
		}

		d, ok, err := elementTo[Decimal](target[0], true)
		if err != nil || !ok {
			return nil, true, nil
		}

		return Collection{d}, true, nil
	},
	"convertsToDecimal": func(
		ctx context.Context,
		root Element, target Collection,
		inputOrdered bool,
		parameters []Expression,
		evaluate EvaluateFunc,
	) (result Collection, resultOrdered bool, err error) {
		if len(parameters) != 0 {
			return nil, false, fmt.Errorf("expected no parameters")
		}

		// Check if convertible to decimal
		if len(target) == 0 {
			return Collection{Boolean(false)}, true, nil
		} else if len(target) > 1 {
			return nil, false, fmt.Errorf("cannot convert to decimal: collection contains > 1 values")
		}

		_, ok, err := elementTo[Decimal](target[0], true)
		if err != nil || !ok {
			return Collection{Boolean(false)}, true, nil
		}

		return Collection{Boolean(true)}, true, nil
	},
	"toQuantity": func(
		ctx context.Context,
		root Element, target Collection,
		inputOrdered bool,
		parameters []Expression,
		evaluate EvaluateFunc,
	) (result Collection, resultOrdered bool, err error) {
		// Check parameters: optional unit
		if len(parameters) > 1 {
			return nil, false, fmt.Errorf("expected at most one unit parameter")
		}

		// Convert to quantity
		if len(target) == 0 {
			return nil, true, nil
		} else if len(target) > 1 {
			return nil, false, fmt.Errorf("cannot convert to quantity: collection contains > 1 values")
		}

		q, ok, err := elementTo[Quantity](target[0], true)
		if err != nil || !ok {
			return nil, true, nil
		}

		// If unit parameter is provided, check if the quantity can be converted to the given unit
		if len(parameters) == 1 {
			// Evaluate the unit parameter
			unitCollection, _, err := evaluate(ctx, nil, parameters[0], nil)
			if err != nil {
				return nil, false, err
			}

			unitStr, ok, err := Singleton[String](unitCollection)
			if err != nil {
				return nil, false, err
			}
			if !ok {
				return nil, false, fmt.Errorf("expected string unit parameter")
			}

			q.Unit = unitStr
		}

		return Collection{q}, true, nil
	},
	"convertsToQuantity": func(
		ctx context.Context,
		root Element, target Collection,
		inputOrdered bool,
		parameters []Expression,
		evaluate EvaluateFunc,
	) (result Collection, resultOrdered bool, err error) {
		// Check parameters: optional unit
		if len(parameters) > 1 {
			return nil, false, fmt.Errorf("expected at most one unit parameter")
		}

		// Check if convertible to quantity
		if len(target) == 0 {
			return Collection{Boolean(false)}, true, nil
		} else if len(target) > 1 {
			return nil, false, fmt.Errorf("cannot convert to quantity: collection contains > 1 values")
		}

		_, ok, err := elementTo[Quantity](target[0], true)
		if err != nil || !ok {
			return Collection{Boolean(false)}, true, nil
		}

		// If unit parameter is provided, check if the quantity can be converted to the given unit
		if len(parameters) == 1 {
			// Evaluate the unit parameter
			unitCollection, _, err := evaluate(ctx, nil, parameters[0], nil)
			if err != nil {
				return nil, false, err
			}

			// Convert to string
			if len(unitCollection) == 0 {
				return nil, false, fmt.Errorf("expected string unit parameter")
			} else if len(unitCollection) > 1 {
				return nil, false, fmt.Errorf("expected single string unit parameter")
			}

			_, ok, err := Singleton[String](unitCollection)
			if err != nil {
				return nil, false, err
			}
			if !ok {
				return nil, false, fmt.Errorf("expected string unit parameter")
			}
		}

		return Collection{Boolean(true)}, true, nil
	},
	"toString": func(
		ctx context.Context,
		root Element, target Collection,
		inputOrdered bool,
		parameters []Expression,
		evaluate EvaluateFunc,
	) (result Collection, resultOrdered bool, err error) {
		if len(parameters) != 0 {
			return nil, false, fmt.Errorf("expected no parameters")
		}

		// Convert to string
		if len(target) == 0 {
			return nil, true, nil
		} else if len(target) > 1 {
			return nil, false, fmt.Errorf("cannot convert to string: collection contains > 1 values")
		}

		s, ok, err := elementTo[String](target[0], true)
		if err != nil || !ok {
			return nil, true, nil
		}

		return Collection{s}, true, nil
	},
	"convertsToString": func(
		ctx context.Context,
		root Element, target Collection,
		inputOrdered bool,
		parameters []Expression,
		evaluate EvaluateFunc,
	) (result Collection, resultOrdered bool, err error) {
		if len(parameters) != 0 {
			return nil, false, fmt.Errorf("expected no parameters")
		}

		// Check if convertible to string
		if len(target) == 0 {
			return Collection{Boolean(false)}, true, nil
		} else if len(target) > 1 {
			return nil, false, fmt.Errorf("cannot convert to string: collection contains > 1 values")
		}

		_, ok, err := elementTo[String](target[0], true)
		if err != nil || !ok {
			return Collection{Boolean(false)}, true, nil
		}

		return Collection{Boolean(true)}, true, nil
	},

	// Tree navigation functions
	"children": func(
		ctx context.Context,
		root Element, target Collection,
		inputOrdered bool,
		parameters []Expression,
		evaluate EvaluateFunc,
	) (result Collection, resultOrdered bool, err error) {
		if len(parameters) != 0 {
			return nil, false, fmt.Errorf("expected no parameters")
		}

		// If the input collection is empty, the result is empty
		if len(target) == 0 {
			return nil, true, nil
		}

		for _, elem := range target {
			// Get all immediate child nodes
			children := elem.Children()
			result = append(result, children...)
		}

		return result, false, nil
	},
	"descendants": func(
		ctx context.Context,
		root Element, target Collection,
		inputOrdered bool,
		parameters []Expression,
		evaluate EvaluateFunc,
	) (result Collection, resultOrdered bool, err error) {
		if len(parameters) != 0 {
			return nil, false, fmt.Errorf("expected no parameters")
		}

		// If the input collection is empty, the result is empty
		if len(target) == 0 {
			return nil, true, nil
		}

		// descendants() is a shorthand for repeat(children())
		// Manually implement the logic of repeat(children())

		var current = target
		var newItems Collection

		// Keep repeating the children() function until no new items are found
		for {
			newItems = nil

			// Get all children of the current elements
			for _, elem := range current {
				children := elem.Children()

				// Check for new items
				for _, child := range children {
					isNew := true
					for _, seen := range result {
						eq, ok := seen.Equal(child)
						if ok && eq {
							isNew = false
							break
						}
					}
					if isNew {
						newItems = append(newItems, child)
					}
				}
			}

			// If no new items were found, we're done
			if len(newItems) == 0 {
				break
			}

			// Add new items to the result and set them as the current items for the next iteration
			result = append(result, newItems...)
			current = newItems
		}

		return result, false, nil
	},

	// Utility functions
	"trace": func(
		ctx context.Context,
		root Element, target Collection,
		inputOrdered bool,
		parameters []Expression,
		evaluate EvaluateFunc,
	) (result Collection, resultOrdered bool, err error) {
		if len(parameters) == 0 || len(parameters) > 2 {
			return nil, false, fmt.Errorf("expected one or two parameters")
		}

		logger, err := tracer(ctx)
		if err != nil {
			return nil, false, err
		}

		nameParam, _, err := evaluate(ctx, nil, parameters[0], nil)
		if err != nil {
			return nil, false, err
		}
		if len(nameParam) != 1 {
			return nil, false, fmt.Errorf("expected single name parameter")
		}
		nameStr, ok, err := Singleton[String](nameParam)
		if err != nil {
			return nil, false, err
		}
		if !ok {
			return nil, false, fmt.Errorf("name parameter cannot be null")
		}

		var logCollection Collection
		if len(parameters) == 2 {
			for i, elem := range target {
				projection, _, err := evaluate(ctx, Collection{elem}, parameters[1], &FunctionScope{index: i})
				if err != nil {
					return nil, false, err
				}
				logCollection = append(logCollection, projection...)
			}
		} else {
			logCollection = target
		}

		err = logger.Log(string(nameStr), logCollection)
		if err != nil {
			return nil, false, err
		}

		return target, inputOrdered, nil
	},
	"aggregate": func(
		ctx context.Context,
		root Element, target Collection,
		inputOrdered bool,
		parameters []Expression,
		evaluate EvaluateFunc,
	) (result Collection, resultOrdered bool, err error) {
		if len(parameters) == 0 || len(parameters) > 2 {
			return nil, false, fmt.Errorf("expected one or two parameters")
		}

		// If the input collection is empty, the result is empty
		if len(target) == 0 {
			return nil, true, nil
		}

		total := Collection{}
		totalOrdered := inputOrdered

		if len(parameters) == 2 {
			// If init value is provided, evaluate it
			var err error
			total, totalOrdered, err = evaluate(ctx, nil, parameters[1], nil)
			if err != nil {
				return nil, false, err
			}
		}

		// Iterate over the target collection
		for i, elem := range target {
			var ordered bool
			total, ordered, err = evaluate(ctx, Collection{elem}, parameters[0], &FunctionScope{index: i, total: total})
			if err != nil {
				return nil, false, err
			}
			if !ordered {
				resultOrdered = false
			}
		}

		return total, totalOrdered, nil
	},
	"now": func(
		ctx context.Context,
		root Element, target Collection,
		inputOrdered bool,
		parameters []Expression,
		evaluate EvaluateFunc,
	) (result Collection, resultOrdered bool, err error) {
		if len(parameters) != 0 {
			return nil, false, fmt.Errorf("expected no parameters")
		}

		instant := evaluationInstant(ctx)
		dt := DateTime{Value: instant, Precision: DateTimePrecisionFull, HasTimeZone: true}

		return Collection{dt}, inputOrdered, nil
	},
	"timeOfDay": func(
		ctx context.Context,
		root Element, target Collection,
		inputOrdered bool,
		parameters []Expression,
		evaluate EvaluateFunc,
	) (result Collection, resultOrdered bool, err error) {
		if len(parameters) != 0 {
			return nil, false, fmt.Errorf("expected no parameters")
		}

		instant := evaluationInstant(ctx)
		tod := time.Date(0, 1, 1, instant.Hour(), instant.Minute(), instant.Second(), instant.Nanosecond(), instant.Location())
		t := Time{Value: tod, Precision: TimePrecisionFull}

		return Collection{t}, inputOrdered, nil
	},
	"today": func(
		ctx context.Context,
		root Element, target Collection,
		inputOrdered bool,
		parameters []Expression,
		evaluate EvaluateFunc,
	) (result Collection, resultOrdered bool, err error) {
		if len(parameters) != 0 {
			return nil, false, fmt.Errorf("expected no parameters")
		}

		instant := evaluationInstant(ctx)
		dateValue := time.Date(instant.Year(), instant.Month(), instant.Day(), 0, 0, 0, 0, instant.Location())
		d := Date{Value: dateValue, Precision: DatePrecisionFull}

		return Collection{d}, inputOrdered, nil
	},
	"iif": func(
		ctx context.Context,
		root Element, target Collection,
		inputOrdered bool,
		parameters []Expression,
		evaluate EvaluateFunc,
	) (result Collection, resultOrdered bool, err error) {
		// Check parameters: criterion, true-result, and optional otherwise-result
		if len(parameters) < 2 || len(parameters) > 3 {
			return nil, false, fmt.Errorf("expected 2 or 3 parameters (criterion, true-result, [otherwise-result])")
		}

		if len(target) > 1 {
			return nil, false, fmt.Errorf("iif() requires an input collection with 0 or 1 items, got %d items", len(target))
		}

		// Determine the evaluation target and scope for $this context
		// Always set these, even for empty collections, to ensure proper scope resolution
		evalTarget := target
		var fnScope *FunctionScope

		// Preserve the parent function scope's index if it exists
		parentScope, ok := getFunctionScope(ctx)
		if ok {
			// Use parent scope's index
			fnScope = &FunctionScope{index: parentScope.index, total: target}
		} else {
			// No parent scope, set index to 0
			fnScope = &FunctionScope{index: 0, total: target}
		}

		// Evaluate the criterion expression with $this context
		criterion, _, err := evaluate(ctx, evalTarget, parameters[0], fnScope)
		if err != nil {
			return nil, false, err
		}

		// Convert criterion to boolean
		criterionBool, ok, err := Singleton[Boolean](criterion)
		if err != nil {
			return nil, false, err
		}

		// Short-circuit evaluation: only evaluate the taken branch
		// If criterion is true, return the value of the true-result argument
		if ok && bool(criterionBool) {
			trueResult, ordered, err := evaluate(ctx, evalTarget, parameters[1], fnScope)
			if err != nil {
				return nil, false, err
			}
			return trueResult, ordered, nil
		}

		// If criterion is false or an empty collection, return otherwise-result
		// If otherwise-result is not given, return an empty collection
		if len(parameters) == 3 {
			otherwiseResult, ordered, err := evaluate(ctx, evalTarget, parameters[2], fnScope)
			if err != nil {
				return nil, false, err
			}
			return otherwiseResult, ordered, nil
		}

		// No otherwise-result, return empty collection
		return nil, true, nil
	},
	"yearOf": func(
		ctx context.Context,
		root Element, target Collection,
		inputOrdered bool,
		parameters []Expression,
		evaluate EvaluateFunc,
	) (result Collection, resultOrdered bool, err error) {
		if len(target) == 0 {
			return nil, inputOrdered, nil
		}
		if len(target) > 1 {
			return nil, false, fmt.Errorf("expected single Date or DateTime, got %d items", len(target))
		}

		dt, ok, err := target[0].ToDateTime(false)
		if err != nil || !ok {
			d, ok, err := target[0].ToDate(false)
			if err != nil || !ok {
				return nil, false, fmt.Errorf("expected Date or DateTime, got %T", target[0])
			}
			dt = DateTime{Value: d.Value, Precision: DateTimePrecision(d.Precision)}
		}

		if dt.Precision == DateTimePrecisionYear {
			return Collection{Integer(dt.Value.Year())}, inputOrdered, nil
		}
		return nil, false, nil
	},
	"monthOf": func(
		ctx context.Context,
		root Element, target Collection,
		inputOrdered bool,
		parameters []Expression,
		evaluate EvaluateFunc,
	) (result Collection, resultOrdered bool, err error) {
		if len(target) == 0 {
			return nil, inputOrdered, nil
		}
		if len(target) > 1 {
			return nil, false, fmt.Errorf("expected single Date or DateTime, got %d items", len(target))
		}

		dt, ok, err := target[0].ToDateTime(false)
		if err != nil || !ok {
			d, ok, err := target[0].ToDate(false)
			if err != nil || !ok {
				return nil, false, fmt.Errorf("expected Date or DateTime, got %T", target[0])
			}
			dt = DateTime{Value: d.Value, Precision: DateTimePrecision(d.Precision)}
		}

		if dt.Precision == DateTimePrecisionYear {
			return nil, inputOrdered, nil
		}
		return Collection{Integer(dt.Value.Month())}, inputOrdered, nil
	},
	"dayOf": func(
		ctx context.Context,
		root Element, target Collection,
		inputOrdered bool,
		parameters []Expression,
		evaluate EvaluateFunc,
	) (result Collection, resultOrdered bool, err error) {
		if len(target) == 0 {
			return nil, inputOrdered, nil
		}
		if len(target) > 1 {
			return nil, false, fmt.Errorf("expected single Date or DateTime, got %d items", len(target))
		}

		dt, ok, err := target[0].ToDateTime(false)
		if err != nil || !ok {
			d, ok, err := target[0].ToDate(false)
			if err != nil || !ok {
				return nil, false, fmt.Errorf("expected Date or DateTime, got %T", target[0])
			}
			dt = DateTime{Value: d.Value, Precision: DateTimePrecision(d.Precision)}
		}

		if dt.Precision == DateTimePrecisionYear || dt.Precision == DateTimePrecisionMonth {
			return nil, inputOrdered, nil
		}
		return Collection{Integer(dt.Value.Day())}, inputOrdered, nil
	},
	"hourOf": func(
		ctx context.Context,
		root Element, target Collection,
		inputOrdered bool,
		parameters []Expression,
		evaluate EvaluateFunc,
	) (result Collection, resultOrdered bool, err error) {
		if len(target) == 0 {
			return nil, inputOrdered, nil
		}
		if len(target) > 1 {
			return nil, false, fmt.Errorf("expected single Date, DateTime or Time, got %d items", len(target))
		}

		var t time.Time
		var precision string

		dt, ok, err := target[0].ToDateTime(false)
		if err == nil && ok {
			t = dt.Value
			precision = string(dt.Precision)
		} else {
			d, ok, err := target[0].ToDate(false)
			if err == nil && ok {
				t = d.Value
				precision = string(d.Precision)
			} else {
				time, ok, err := target[0].ToTime(false)
				if err != nil || !ok {
					return nil, false, fmt.Errorf("expected Date, DateTime or Time, got %T", target[0])
				}
				t = time.Value
				precision = string(time.Precision)
			}
		}

		if precision == string(DateTimePrecisionYear) || precision == string(DateTimePrecisionMonth) || precision == string(DateTimePrecisionDay) {
			return nil, inputOrdered, nil
		}
		return Collection{Integer(t.Hour())}, inputOrdered, nil
	},
	"minuteOf": func(
		ctx context.Context,
		root Element, target Collection,
		inputOrdered bool,
		parameters []Expression,
		evaluate EvaluateFunc,
	) (result Collection, resultOrdered bool, err error) {
		if len(target) == 0 {
			return nil, inputOrdered, nil
		}
		if len(target) > 1 {
			return nil, false, fmt.Errorf("expected single Date, DateTime or Time, got %d items", len(target))
		}

		var t time.Time
		var precision string

		dt, ok, err := target[0].ToDateTime(false)
		if err == nil && ok {
			t = dt.Value
			precision = string(dt.Precision)
		} else {
			d, ok, err := target[0].ToDate(false)
			if err == nil && ok {
				t = d.Value
				precision = string(d.Precision)
			} else {
				time, ok, err := target[0].ToTime(false)
				if err != nil || !ok {
					return nil, false, fmt.Errorf("expected Date, DateTime or Time, got %T", target[0])
				}
				t = time.Value
				precision = string(time.Precision)
			}
		}

		if precision == string(DateTimePrecisionYear) || precision == string(DateTimePrecisionMonth) || precision == string(DateTimePrecisionDay) || precision == string(DateTimePrecisionHour) {
			return nil, inputOrdered, nil
		}
		return Collection{Integer(t.Minute())}, inputOrdered, nil
	},
	"secondOf": func(
		ctx context.Context,
		root Element, target Collection,
		inputOrdered bool,
		parameters []Expression,
		evaluate EvaluateFunc,
	) (result Collection, resultOrdered bool, err error) {
		if len(target) == 0 {
			return nil, inputOrdered, nil
		}
		if len(target) > 1 {
			return nil, false, fmt.Errorf("expected single Date, DateTime or Time, got %d items", len(target))
		}

		var t time.Time
		var precision string

		dt, ok, err := target[0].ToDateTime(false)
		if err == nil && ok {
			t = dt.Value
			precision = string(dt.Precision)
		} else {
			d, ok, err := target[0].ToDate(false)
			if err == nil && ok {
				t = d.Value
				precision = string(d.Precision)
			} else {
				time, ok, err := target[0].ToTime(false)
				if err != nil || !ok {
					return nil, false, fmt.Errorf("expected Date, DateTime or Time, got %T", target[0])
				}
				t = time.Value
				precision = string(time.Precision)
			}
		}

		if precision == string(DateTimePrecisionYear) || precision == string(DateTimePrecisionMonth) || precision == string(DateTimePrecisionDay) || precision == string(DateTimePrecisionHour) || precision == string(DateTimePrecisionMinute) {
			return nil, inputOrdered, nil
		}
		return Collection{Integer(t.Second())}, inputOrdered, nil
	},
	"millisecondOf": func(
		ctx context.Context,
		root Element, target Collection,
		inputOrdered bool,
		parameters []Expression,
		evaluate EvaluateFunc,
	) (result Collection, resultOrdered bool, err error) {
		if len(target) == 0 {
			return nil, inputOrdered, nil
		}
		if len(target) > 1 {
			return nil, false, fmt.Errorf("expected single Date, DateTime or Time, got %d items", len(target))
		}

		var t time.Time
		var precision string

		dt, ok, err := target[0].ToDateTime(false)
		if err == nil && ok {
			t = dt.Value
			precision = string(dt.Precision)
		} else {
			d, ok, err := target[0].ToDate(false)
			if err == nil && ok {
				t = d.Value
				precision = string(d.Precision)
			} else {
				time, ok, err := target[0].ToTime(false)
				if err != nil || !ok {
					return nil, false, fmt.Errorf("expected Date, DateTime or Time, got %T", target[0])
				}
				t = time.Value
				precision = string(time.Precision)
			}
		}

		if precision == string(DateTimePrecisionYear) || precision == string(DateTimePrecisionMonth) || precision == string(DateTimePrecisionDay) || precision == string(DateTimePrecisionHour) || precision == string(DateTimePrecisionMinute) {
			return nil, inputOrdered, nil
		}
		return Collection{Integer(t.Nanosecond() / 1000000)}, inputOrdered, nil
	},
	"timezoneOffsetOf": func(
		ctx context.Context,
		root Element, target Collection,
		inputOrdered bool,
		parameters []Expression,
		evaluate EvaluateFunc,
	) (result Collection, resultOrdered bool, err error) {
		if len(target) == 0 {
			return nil, inputOrdered, nil
		}
		if len(target) > 1 {
			return nil, false, fmt.Errorf("expected single DateTime, got %d items", len(target))
		}

		dt, ok, err := target[0].ToDateTime(false)
		if err != nil || !ok {
			return nil, false, fmt.Errorf("expected DateTime, got %T", target[0])
		}

		_, offset := dt.Value.Zone()
		hours := float64(offset) / 3600.0
		dec := apd.New(0, 0)
		dec.SetFloat64(hours)
		return Collection{Decimal{Value: dec}}, inputOrdered, nil
	},
	"dateOf": func(
		ctx context.Context,
		root Element, target Collection,
		inputOrdered bool,
		parameters []Expression,
		evaluate EvaluateFunc,
	) (result Collection, resultOrdered bool, err error) {
		if len(target) == 0 {
			return nil, inputOrdered, nil
		}
		if len(target) > 1 {
			return nil, false, fmt.Errorf("expected single Date or DateTime, got %d items", len(target))
		}

		dt, ok, err := target[0].ToDateTime(false)
		if err != nil || !ok {
			d, ok, err := target[0].ToDate(false)
			if err != nil || !ok {
				return nil, false, fmt.Errorf("expected Date or DateTime, got %T", target[0])
			}
			return Collection{d}, inputOrdered, nil
		}

		var precision DatePrecision
		switch dt.Precision {
		case DateTimePrecisionYear:
			precision = DatePrecisionYear
		case DateTimePrecisionMonth:
			precision = DatePrecisionMonth
		default:
			precision = DatePrecisionFull
		}

		return Collection{Date{Value: dt.Value, Precision: precision}}, inputOrdered, nil
	},
	"timeOf": func(
		ctx context.Context,
		root Element, target Collection,
		inputOrdered bool,
		parameters []Expression,
		evaluate EvaluateFunc,
	) (result Collection, resultOrdered bool, err error) {
		if len(target) == 0 {
			return nil, inputOrdered, nil
		}
		if len(target) > 1 {
			return nil, false, fmt.Errorf("expected single DateTime, got %d items", len(target))
		}

		dt, ok, err := target[0].ToDateTime(false)
		if err != nil || !ok {
			return nil, false, fmt.Errorf("expected DateTime, got %T", target[0])
		}

		if dt.Precision == DateTimePrecisionYear || dt.Precision == DateTimePrecisionMonth || dt.Precision == DateTimePrecisionDay {
			return nil, inputOrdered, nil
		}

		var precision TimePrecision
		switch dt.Precision {
		case DateTimePrecisionHour:
			precision = TimePrecisionHour
		case DateTimePrecisionMinute:
			precision = TimePrecisionMinute
		default:
			precision = TimePrecisionFull
		}

		return Collection{Time{Value: dt.Value, Precision: precision}}, inputOrdered, nil
	},
	"comparable": func(
		ctx context.Context,
		root Element, target Collection,
		inputOrdered bool,
		parameters []Expression,
		evaluate EvaluateFunc,
	) (result Collection, resultOrdered bool, err error) {
		// comparable(other: Quantity): Boolean
		// Returns true if the input and parameter Quantities have comparable units via UCUM conversion
		if len(parameters) != 1 {
			return nil, false, fmt.Errorf("expected one quantity parameter")
		}

		// Input must be a single Quantity
		if len(target) == 0 {
			return nil, inputOrdered, nil
		}
		if len(target) > 1 {
			return nil, false, fmt.Errorf("comparable() requires a single Quantity input, got %d items", len(target))
		}

		inputQty, ok, err := elementTo[Quantity](target[0], false)
		if err != nil || !ok {
			return nil, inputOrdered, nil
		}

		// Evaluate the parameter
		paramCollection, _, err := evaluate(ctx, nil, parameters[0], nil)
		if err != nil {
			return nil, false, err
		}
		if len(paramCollection) == 0 {
			return nil, inputOrdered, nil
		}
		if len(paramCollection) > 1 {
			return nil, false, fmt.Errorf("comparable() requires a single Quantity parameter, got %d items", len(paramCollection))
		}

		paramQty, ok, err := elementTo[Quantity](paramCollection[0], false)
		if err != nil || !ok {
			return nil, inputOrdered, nil
		}

		// Get canonical units
		inputUnit := canonicalUCUMUnit(string(inputQty.Unit))
		paramUnit := canonicalUCUMUnit(string(paramQty.Unit))

		// Same units are always comparable
		if inputUnit == paramUnit {
			return Collection{Boolean(true)}, inputOrdered, nil
		}

		// Try to convert from input unit to parameter unit
		// If conversion succeeds, units are comparable
		testValue := apd.New(1, 0)
		_, err = convertDecimalUnit(ctx, testValue, inputUnit, paramUnit)
		if err == nil {
			return Collection{Boolean(true)}, inputOrdered, nil
		}

		// Units are not comparable
		return Collection{Boolean(false)}, inputOrdered, nil
	},
}

// FHIRFunctions contains FHIR-specific extension functions that are not part of the base FHIRPath specification.
// These functions are defined in the FHIR specification and operate on FHIR resources and data types.
var FHIRFunctions = Functions{
	"extension": func(
		ctx context.Context,
		root Element, target Collection,
		inputOrdered bool,
		parameters []Expression,
		evaluate EvaluateFunc,
	) (result Collection, resultOrdered bool, err error) {
		if len(parameters) != 1 {
			return nil, false, fmt.Errorf("expected a single extension parameter")
		}

		extCollection, _, err := evaluate(ctx, nil, parameters[0], nil)
		if err != nil {
			return nil, false, err
		}

		extUrl, ok, err := Singleton[String](extCollection)
		if err != nil {
			return nil, false, err
		}
		if !ok {
			return nil, false, fmt.Errorf("expected extension parameter string")
		}

		var foundExtensions Collection
		for _, v := range target {
			for _, e := range v.Children("extension") {
				url, ok, err := Singleton[String](e.Children("url"))
				if err == nil && ok && url == extUrl {
					foundExtensions = append(foundExtensions, e)
					break
				}
			}
		}
		return foundExtensions, inputOrdered, nil
	},
	"hasValue": func(
		ctx context.Context,
		root Element, target Collection,
		inputOrdered bool,
		parameters []Expression,
		evaluate EvaluateFunc,
	) (result Collection, resultOrdered bool, err error) {
		// hasValue(): Boolean
		// Returns true if the single value is a FHIR primitive with a value (not just extensions).
		// Returns false if it's a primitive without a value.
		// Returns empty if the input is not a single FHIR primitive.
		if len(parameters) != 0 {
			return nil, false, fmt.Errorf("expected no parameters")
		}

		if len(target) == 0 {
			return nil, inputOrdered, nil
		}

		// Per spec: must be a single value
		if len(target) > 1 {
			return nil, inputOrdered, nil
		}

		elem := target[0]

		// Check if element implements hasValuer interface (FHIR primitives)
		if hv, ok := elem.(hasValuer); ok {
			return Collection{Boolean(hv.HasValue())}, inputOrdered, nil
		}

		// Not a FHIR primitive - return empty
		return nil, inputOrdered, nil
	},
	"getValue": func(
		ctx context.Context,
		root Element, target Collection,
		inputOrdered bool,
		parameters []Expression,
		evaluate EvaluateFunc,
	) (result Collection, resultOrdered bool, err error) {
		// getValue(): System.[type]
		// Per FHIRPath section 2.1.9.1.5.4 this returns the underlying system value
		// when the input is a single FHIR primitive that actually has a value.
		if len(parameters) != 0 {
			return nil, false, fmt.Errorf("expected no parameters")
		}

		if len(target) != 1 {
			return nil, inputOrdered, nil
		}

		elem := target[0]
		hv, ok := elem.(hasValuer)
		if !ok || !hv.HasValue() {
			return nil, inputOrdered, nil
		}

		primitive, ok := toPrimitive(elem)
		if !ok || primitive == nil {
			return nil, inputOrdered, nil
		}

		return Collection{primitive}, inputOrdered, nil
	},
}

func compareElementsForSort(a, b Element) (int, error) {
	cmp, ok, err := Collection{a}.Cmp(Collection{b})
	if err != nil {
		return 0, err
	}
	if !ok {
		return 0, fmt.Errorf("elements %T and %T are not comparable", a, b)
	}
	return cmp, nil
}

// calculateDateDuration calculates the number of whole calendar periods between two dates
func calculateDateDuration(start, end Date, precision string) (Collection, bool, error) {
	// Check precision validity for dates
	switch precision {
	case UnitYear, UnitMonth, UnitWeek, UnitDay:
		// Valid
	default:
		return nil, false, fmt.Errorf("invalid precision for Date: %s", precision)
	}

	// Check if dates have sufficient precision
	if !hasDatePrecision(start, precision) || !hasDatePrecision(end, precision) {
		return nil, true, nil
	}

	startTime := start.Value
	endTime := end.Value
	sign := int64(1)
	if endTime.Before(startTime) {
		startTime, endTime = endTime, startTime
		sign = -1
	}

	var count int64
	switch precision {
	case UnitYear:
		count = int64(endTime.Year() - startTime.Year())
		// Check if full year hasn't passed yet
		if endTime.Month() < startTime.Month() ||
			(endTime.Month() == startTime.Month() && endTime.Day() < startTime.Day()) {
			count--
		}
	case UnitMonth:
		years := endTime.Year() - startTime.Year()
		months := int(endTime.Month()) - int(startTime.Month())
		count = int64(years*12 + months)
		// Check if full month hasn't passed yet
		if endTime.Day() < startTime.Day() {
			count--
		}
	case UnitWeek:
		days := endTime.Sub(startTime).Hours() / 24
		count = int64(days / 7)
	case UnitDay:
		days := endTime.Sub(startTime).Hours() / 24
		count = int64(days)
	}

	return Collection{Integer(count * sign)}, true, nil
}

// calculateDateTimeDuration calculates the number of whole calendar periods between two datetimes
func calculateDateTimeDuration(start, end DateTime, precision string) (Collection, bool, error) {
	// Check precision validity
	switch precision {
	case UnitYear, UnitMonth, UnitWeek, UnitDay, UnitHour, UnitMinute, UnitSecond, UnitMillisecond:
		// Valid
	default:
		return nil, false, fmt.Errorf("invalid precision for DateTime: %s", precision)
	}

	// Check if datetimes have sufficient precision
	if !hasDateTimePrecision(start, precision) || !hasDateTimePrecision(end, precision) {
		return nil, true, nil
	}

	startTime := start.Value
	endTime := end.Value
	sign := int64(1)
	if endTime.Before(startTime) {
		startTime, endTime = endTime, startTime
		sign = -1
	}

	var count int64
	switch precision {
	case UnitYear:
		count = int64(endTime.Year() - startTime.Year())
		if endTime.Month() < startTime.Month() ||
			(endTime.Month() == startTime.Month() && endTime.Day() < startTime.Day()) ||
			(endTime.Month() == startTime.Month() && endTime.Day() == startTime.Day() &&
				endTime.Hour() < startTime.Hour()) ||
			(endTime.Month() == startTime.Month() && endTime.Day() == startTime.Day() &&
				endTime.Hour() == startTime.Hour() && endTime.Minute() < startTime.Minute()) ||
			(endTime.Month() == startTime.Month() && endTime.Day() == startTime.Day() &&
				endTime.Hour() == startTime.Hour() && endTime.Minute() == startTime.Minute() &&
				endTime.Second() < startTime.Second()) {
			count--
		}
	case UnitMonth:
		years := endTime.Year() - startTime.Year()
		months := int(endTime.Month()) - int(startTime.Month())
		count = int64(years*12 + months)
		if endTime.Day() < startTime.Day() ||
			(endTime.Day() == startTime.Day() && endTime.Hour() < startTime.Hour()) ||
			(endTime.Day() == startTime.Day() && endTime.Hour() == startTime.Hour() &&
				endTime.Minute() < startTime.Minute()) ||
			(endTime.Day() == startTime.Day() && endTime.Hour() == startTime.Hour() &&
				endTime.Minute() == startTime.Minute() && endTime.Second() < startTime.Second()) {
			count--
		}
	case UnitWeek:
		duration := endTime.Sub(startTime)
		count = int64(duration.Hours() / 24 / 7)
	case UnitDay:
		duration := endTime.Sub(startTime)
		count = int64(duration.Hours() / 24)
	case UnitHour:
		duration := endTime.Sub(startTime)
		count = int64(duration.Hours())
	case UnitMinute:
		duration := endTime.Sub(startTime)
		count = int64(duration.Minutes())
	case UnitSecond:
		duration := endTime.Sub(startTime)
		count = int64(duration.Seconds())
	case UnitMillisecond:
		duration := endTime.Sub(startTime)
		count = duration.Milliseconds()
	}

	return Collection{Integer(count * sign)}, true, nil
}

// calculateTimeDuration calculates the number of whole periods between two times
func calculateTimeDuration(start, end Time, precision string) (Collection, bool, error) {
	// Check precision validity for times
	switch precision {
	case UnitHour, UnitMinute, UnitSecond, UnitMillisecond:
		// Valid
	default:
		return nil, false, fmt.Errorf("invalid precision for Time: %s", precision)
	}

	// Check if times have sufficient precision
	if !hasTimePrecision(start, precision) || !hasTimePrecision(end, precision) {
		return nil, true, nil
	}

	startTime := start.Value
	endTime := end.Value
	sign := int64(1)
	if endTime.Before(startTime) {
		startTime, endTime = endTime, startTime
		sign = -1
	}

	var count int64
	duration := endTime.Sub(startTime)
	switch precision {
	case UnitHour:
		count = int64(duration.Hours())
	case UnitMinute:
		count = int64(duration.Minutes())
	case UnitSecond:
		count = int64(duration.Seconds())
	case UnitMillisecond:
		count = duration.Milliseconds()
	}

	return Collection{Integer(count * sign)}, true, nil
}

// calculateDateDifference calculates the number of boundaries crossed between two dates
func calculateDateDifference(start, end Date, precision string) (Collection, bool, error) {
	// Check precision validity for dates
	switch precision {
	case UnitYear, UnitMonth, UnitWeek, UnitDay:
		// Valid
	default:
		return nil, false, fmt.Errorf("invalid precision for Date: %s", precision)
	}

	// Check if dates have sufficient precision
	if !hasDatePrecision(start, precision) || !hasDatePrecision(end, precision) {
		return nil, true, nil
	}

	startTime := start.Value
	endTime := end.Value
	sign := int64(1)
	if endTime.Before(startTime) {
		startTime, endTime = endTime, startTime
		sign = -1
	}

	var count int64
	switch precision {
	case UnitYear:
		count = int64(endTime.Year() - startTime.Year())
	case UnitMonth:
		years := endTime.Year() - startTime.Year()
		months := int(endTime.Month()) - int(startTime.Month())
		count = int64(years*12 + months)
	case UnitWeek:
		// Week boundaries are Sundays
		startSunday := startTime
		for startSunday.Weekday() != time.Sunday {
			startSunday = startSunday.AddDate(0, 0, -1)
		}
		endSunday := endTime
		for endSunday.Weekday() != time.Sunday {
			endSunday = endSunday.AddDate(0, 0, -1)
		}
		days := endSunday.Sub(startSunday).Hours() / 24
		count = int64(days / 7)
	case UnitDay:
		// Day boundaries crossed
		startDay := time.Date(startTime.Year(), startTime.Month(), startTime.Day(), 0, 0, 0, 0, startTime.Location())
		endDay := time.Date(endTime.Year(), endTime.Month(), endTime.Day(), 0, 0, 0, 0, endTime.Location())
		days := endDay.Sub(startDay).Hours() / 24
		count = int64(days)
	}

	return Collection{Integer(count * sign)}, true, nil
}

// calculateDateTimeDifference calculates the number of boundaries crossed between two datetimes
func calculateDateTimeDifference(start, end DateTime, precision string) (Collection, bool, error) {
	// Check precision validity
	switch precision {
	case UnitYear, UnitMonth, UnitWeek, UnitDay, UnitHour, UnitMinute, UnitSecond, UnitMillisecond:
		// Valid
	default:
		return nil, false, fmt.Errorf("invalid precision for DateTime: %s", precision)
	}

	// Check if datetimes have sufficient precision
	if !hasDateTimePrecision(start, precision) || !hasDateTimePrecision(end, precision) {
		return nil, true, nil
	}

	startTime := start.Value
	endTime := end.Value
	sign := int64(1)
	if endTime.Before(startTime) {
		startTime, endTime = endTime, startTime
		sign = -1
	}

	var count int64
	switch precision {
	case UnitYear:
		count = int64(endTime.Year() - startTime.Year())
	case UnitMonth:
		years := endTime.Year() - startTime.Year()
		months := int(endTime.Month()) - int(startTime.Month())
		count = int64(years*12 + months)
	case UnitWeek:
		startSunday := startTime
		for startSunday.Weekday() != time.Sunday {
			startSunday = startSunday.Add(-24 * time.Hour)
		}
		startSunday = time.Date(startSunday.Year(), startSunday.Month(), startSunday.Day(), 0, 0, 0, 0, startSunday.Location())
		endSunday := endTime
		for endSunday.Weekday() != time.Sunday {
			endSunday = endSunday.Add(-24 * time.Hour)
		}
		endSunday = time.Date(endSunday.Year(), endSunday.Month(), endSunday.Day(), 0, 0, 0, 0, endSunday.Location())
		days := endSunday.Sub(startSunday).Hours() / 24
		count = int64(days / 7)
	case UnitDay:
		startDay := time.Date(startTime.Year(), startTime.Month(), startTime.Day(), 0, 0, 0, 0, startTime.Location())
		endDay := time.Date(endTime.Year(), endTime.Month(), endTime.Day(), 0, 0, 0, 0, endTime.Location())
		days := endDay.Sub(startDay).Hours() / 24
		count = int64(days)
	case UnitHour:
		startHour := time.Date(startTime.Year(), startTime.Month(), startTime.Day(), startTime.Hour(), 0, 0, 0, startTime.Location())
		endHour := time.Date(endTime.Year(), endTime.Month(), endTime.Day(), endTime.Hour(), 0, 0, 0, endTime.Location())
		count = int64(endHour.Sub(startHour).Hours())
	case UnitMinute:
		startMinute := time.Date(startTime.Year(), startTime.Month(), startTime.Day(), startTime.Hour(), startTime.Minute(), 0, 0, startTime.Location())
		endMinute := time.Date(endTime.Year(), endTime.Month(), endTime.Day(), endTime.Hour(), endTime.Minute(), 0, 0, endTime.Location())
		count = int64(endMinute.Sub(startMinute).Minutes())
	case UnitSecond:
		startSecond := time.Date(startTime.Year(), startTime.Month(), startTime.Day(), startTime.Hour(), startTime.Minute(), startTime.Second(), 0, startTime.Location())
		endSecond := time.Date(endTime.Year(), endTime.Month(), endTime.Day(), endTime.Hour(), endTime.Minute(), endTime.Second(), 0, endTime.Location())
		count = int64(endSecond.Sub(startSecond).Seconds())
	case UnitMillisecond:
		count = endTime.Sub(startTime).Milliseconds()
	}

	return Collection{Integer(count * sign)}, true, nil
}

// calculateTimeDifference calculates the number of boundaries crossed between two times
func calculateTimeDifference(start, end Time, precision string) (Collection, bool, error) {
	// Check precision validity for times
	switch precision {
	case UnitHour, UnitMinute, UnitSecond, UnitMillisecond:
		// Valid
	default:
		return nil, false, fmt.Errorf("invalid precision for Time: %s", precision)
	}

	// Check if times have sufficient precision
	if !hasTimePrecision(start, precision) || !hasTimePrecision(end, precision) {
		return nil, true, nil
	}

	startTime := start.Value
	endTime := end.Value
	sign := int64(1)
	if endTime.Before(startTime) {
		startTime, endTime = endTime, startTime
		sign = -1
	}

	var count int64
	switch precision {
	case UnitHour:
		startHour := time.Date(0, 1, 1, startTime.Hour(), 0, 0, 0, startTime.Location())
		endHour := time.Date(0, 1, 1, endTime.Hour(), 0, 0, 0, endTime.Location())
		count = int64(endHour.Sub(startHour).Hours())
	case UnitMinute:
		startMinute := time.Date(0, 1, 1, startTime.Hour(), startTime.Minute(), 0, 0, startTime.Location())
		endMinute := time.Date(0, 1, 1, endTime.Hour(), endTime.Minute(), 0, 0, endTime.Location())
		count = int64(endMinute.Sub(startMinute).Minutes())
	case UnitSecond:
		startSecond := time.Date(0, 1, 1, startTime.Hour(), startTime.Minute(), startTime.Second(), 0, startTime.Location())
		endSecond := time.Date(0, 1, 1, endTime.Hour(), endTime.Minute(), endTime.Second(), 0, endTime.Location())
		count = int64(endSecond.Sub(startSecond).Seconds())
	case UnitMillisecond:
		count = endTime.Sub(startTime).Milliseconds()
	}

	return Collection{Integer(count * sign)}, true, nil
}

// Helper functions to check if Date/DateTime/Time have sufficient precision
func hasDatePrecision(d Date, precision string) bool {
	switch precision {
	case UnitYear:
		return d.Precision == "year" || d.Precision == "month" || d.Precision == "day" || d.Precision == "full"
	case UnitMonth:
		return d.Precision == "month" || d.Precision == "day" || d.Precision == "full"
	case UnitWeek, UnitDay:
		return d.Precision == "day" || d.Precision == "full"
	}
	return false
}

func hasDateTimePrecision(dt DateTime, precision string) bool {
	switch precision {
	case UnitYear:
		return true // Year always available in DateTime
	case UnitMonth:
		return true // Month always available
	case UnitWeek, UnitDay:
		return true // Day always available
	case UnitHour:
		return dt.Precision == DateTimePrecisionHour ||
			dt.Precision == DateTimePrecisionMinute ||
			dt.Precision == DateTimePrecisionSecond ||
			dt.Precision == DateTimePrecisionMillisecond
	case UnitMinute:
		return dt.Precision == DateTimePrecisionMinute ||
			dt.Precision == DateTimePrecisionSecond ||
			dt.Precision == DateTimePrecisionMillisecond
	case UnitSecond:
		return dt.Precision == DateTimePrecisionSecond ||
			dt.Precision == DateTimePrecisionMillisecond
	case UnitMillisecond:
		return dt.Precision == DateTimePrecisionMillisecond
	}
	return false
}

func hasTimePrecision(t Time, precision string) bool {
	switch precision {
	case UnitHour:
		return true // Hour always available in Time
	case UnitMinute:
		return t.Precision == TimePrecisionMinute ||
			t.Precision == TimePrecisionSecond ||
			t.Precision == TimePrecisionMillisecond
	case UnitSecond:
		return t.Precision == TimePrecisionSecond ||
			t.Precision == TimePrecisionMillisecond
	case UnitMillisecond:
		return t.Precision == TimePrecisionMillisecond
	}
	return false
}
