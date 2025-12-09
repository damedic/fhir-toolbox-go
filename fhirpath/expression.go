package fhirpath

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"strconv"
	"strings"

	"github.com/antlr4-go/antlr/v4"
	"github.com/cockroachdb/apd/v3"
	parser "github.com/damedic/fhir-toolbox-go/fhirpath/internal/parser"
)

// Expression represents a parsed FHIRPath expression that can be evaluated against a FHIR resource.
// Expressions are created using the Parse or MustParse functions.
type Expression struct {
	tree          parser.IExpressionContext
	sortDirection sortDirection
}

type sortDirection uint8

const (
	sortDirectionNone sortDirection = iota
	sortDirectionAsc
	sortDirectionDesc
)

// String returns the string representation of the expression.
// This is useful for debugging or displaying the expression.
func (e Expression) String() string {
	if e.tree == nil {
		return ""
	}
	return e.tree.GetText()
}

// Parse parses a FHIRPath expression string and returns an Expression object.
// If the expression cannot be parsed, an error is returned.
//
// Example:
//
//	expr, err := fhirpath.Parse("Patient.name.given")
//	if err != nil {
//	    // Handle error
//	}
func Parse(expr string) (Expression, error) {
	tree, err := parse(expr)
	if err != nil {
		return Expression{}, err
	}
	return Expression{tree: tree}, nil
}

// MustParse parses a FHIRPath expression string and returns an Expression object.
// If the expression cannot be parsed, it panics.
//
// This function is useful when you know the expression is valid and want to avoid
// error checking, such as in tests or with hardcoded expressions.
//
// Example:
//
//	expr := fhirpath.MustParse("Patient.name.given")
func MustParse(path string) Expression {
	expr, err := Parse(path)
	if err != nil {
		panic(err)
	}
	return expr
}

type SyntaxError struct {
	line, column int
	msg          string
}

func (s SyntaxError) Error() string {
	return fmt.Sprintf("%d:%d: %s", s.line, s.column, s.msg)
}

type SyntaxErrorListener struct {
	*antlr.DefaultErrorListener
	Errors []error
}

func (c *SyntaxErrorListener) SyntaxError(
	recognizer antlr.Recognizer,
	offendingSymbol any,
	line, column int,
	msg string, e antlr.RecognitionException) {
	c.Errors = append(c.Errors, SyntaxError{
		line:   line,
		column: column,
		msg:    msg,
	})
}

func parse(expr string) (parser.IExpressionContext, error) {
	errListener := SyntaxErrorListener{}
	inputStream := antlr.NewInputStream(expr)

	lexer := parser.NewFHIRPathLexer(inputStream)
	lexer.RemoveErrorListeners()
	lexer.AddErrorListener(&errListener)

	stream := antlr.NewCommonTokenStream(lexer, antlr.TokenDefaultChannel)
	parser := parser.NewFHIRPathParser(stream)
	parser.RemoveErrorListeners()
	parser.AddErrorListener(&errListener)

	entireExpr := parser.EntireExpression()
	if entireExpr.EOF() == nil {
		return nil, fmt.Errorf(
			"can not parse expression at index %v: %v",
			len(entireExpr.Expression().GetText()), entireExpr.GetText(),
		)
	}

	return entireExpr.Expression(), errors.Join(errListener.Errors...)
}

// Evaluate evaluates a FHIRPath expression against a target element and returns the resulting collection.
//
// The context parameter can be used to provide additional configuration for the evaluation,
// such as decimal precision settings, trace logging, or environment variables.
// For FHIR resources, you can use the context provided by the model package (e.g., r4.Context()).
//
// The target parameter is the element against which the expression will be evaluated.
// This is typically a FHIR resource like a Patient or Observation.
//
// The expr parameter is the parsed FHIRPath expression to evaluate.
//
// Example:
//
//	patient := r4.Patient{...}
//	expr := fhirpath.MustParse("Patient.name.given")
//	result, err := fhirpath.Evaluate(r4.Context(), patient, expr)
//	if err != nil {
//	    // Handle error
//	}
//	fmt.Println(result) // Output: [Donald]
func Evaluate(ctx context.Context, target Element, expr Expression) (Collection, error) {
	ctx = withEvaluationInstant(ctx)
	for name, value := range systemVariables {
		if name == "context" {
			ctx = WithEnv(ctx, name, Collection{target})
		} else {
			ctx = WithEnv(ctx, name, value)
		}
	}

	result, _, err := evalExpression(
		ctx,
		target, Collection{target},
		true,
		expr.tree,
		true,
	)
	return result, err
}

func evalExpression(
	ctx context.Context,
	root Element, target Collection,
	inputOrdered bool,
	tree parser.IExpressionContext,
	isRoot bool,
) (result Collection, resultOrdered bool, err error) {

	switch t := tree.(type) {
	case *parser.ExpressionContext:
		return nil, false, fmt.Errorf("can not evaluate empty expression")
	case *parser.TermExpressionContext:
		return evalTerm(ctx, root, target, inputOrdered, t.Term(), isRoot)
	case *parser.InvocationExpressionContext:
		expr, ordered, err := evalExpression(ctx, root, target, inputOrdered, t.Expression(), isRoot)
		if err != nil {
			return nil, false, err
		}
		return evalInvocation(ctx, root, expr, ordered, t.Invocation(), false)
	case *parser.IndexerExpressionContext:
		expr, ordered, err := evalExpression(ctx, root, target, inputOrdered, t.Expression(0), isRoot)
		if err != nil {
			return nil, false, err
		}
		if !ordered {
			return nil, false, errors.New("can not index into unordered collection")
		}
		indexCollection, _, err := evalExpression(ctx, root, target, inputOrdered, t.Expression(1), false)
		if err != nil {
			return nil, false, err
		}
		index, ok, err := Singleton[Integer](indexCollection)
		if err != nil {
			return nil, false, err
		}
		if !ok {
			return nil, false, fmt.Errorf("can not index with null index")
		}
		i := int(index)
		if i >= len(expr) {
			return nil, false, nil
		} else {
			return Collection{expr[i]}, true, nil
		}
	case *parser.PolarityExpressionContext:
		expr, ordered, err := evalExpression(ctx, root, target, inputOrdered, t.Expression(), isRoot)
		if err != nil {
			return nil, false, err
		}
		op := t.GetChild(0).(antlr.ParseTree).GetText()

		switch op {
		case "+":
			// noop
			return expr, ordered, nil
		case "-":
			result, err = expr.Multiply(ctx, Collection{Integer(-1)})
			return result, true, err

		}
		return nil, false, nil
	case *parser.MultiplicativeExpressionContext:
		left, _, err := evalExpression(ctx, root, target, inputOrdered, t.Expression(0), isRoot)
		if err != nil {
			return nil, false, err
		}
		right, _, err := evalExpression(ctx, root, target, inputOrdered, t.Expression(1), isRoot)
		if err != nil {
			return nil, false, err
		}
		op := t.GetChild(1).(antlr.ParseTree).GetText()

		switch op {
		case "*":
			result, err = left.Multiply(ctx, right)
		case "/":
			result, err = left.Divide(ctx, right)
		case "div":
			result, err = left.Div(ctx, right)
		case "mod":
			result, err = left.Mod(ctx, right)
		}
		return result, true, err
	case *parser.AdditiveExpressionContext:
		left, _, err := evalExpression(ctx, root, target, inputOrdered, t.Expression(0), isRoot)
		if err != nil {
			return nil, false, err
		}
		right, _, err := evalExpression(ctx, root, target, inputOrdered, t.Expression(1), isRoot)
		if err != nil {
			return nil, false, err
		}
		op := t.GetChild(1).(antlr.ParseTree).GetText()

		switch op {
		case "+":
			result, err = left.Add(ctx, right)
		case "-":
			result, err = left.Subtract(ctx, right)
		case "&":
			result, err = left.Concat(ctx, right)
		}
		return result, true, err
	case *parser.TypeExpressionContext:
		expr, _, err := evalExpression(ctx, root, target, inputOrdered, t.Expression(), isRoot)
		if err != nil {
			return nil, false, err
		}
		op := t.GetChild(1).(antlr.ParseTree).GetText()
		spec, err := evalQualifiedIdentifier(t.TypeSpecifier().QualifiedIdentifier())
		if err != nil {
			return nil, false, err
		}

		if len(expr) == 0 {
			// FHIRPath requires single-input
			// operators to return { } when invoked on an empty collection.
			// The HL7 test suite exercises this with Observation.issued is instant.
			return nil, true, nil
		}
		if len(expr) != 1 {
			return nil, false, fmt.Errorf("expected single input element")
		}

		switch op {
		case "is":
			r, err := isType(ctx, expr[0], spec)
			if err != nil {
				return nil, false, err
			}
			return Collection{r}, true, nil
		case "as":
			c, err := asType(ctx, expr[0], spec)
			if err != nil {
				return nil, false, err
			}
			if c != nil {
				return c, true, nil
			}
			return nil, false, nil

		}

		return nil, false, nil

	case *parser.UnionExpressionContext:
		// Each branch of a union gets its own environment stack frame
		// This ensures that variables defined on one side don't affect the other
		// We create fresh contexts for both sides here since they're separate evaluation trees
		leftCtx, _ := withNewEnvStackFrame(ctx)
		left, leftOrdered, err := evalExpression(leftCtx, root, target, inputOrdered, t.Expression(0), isRoot)
		if err != nil {
			return nil, false, err
		}
		rightCtx, _ := withNewEnvStackFrame(ctx)
		right, rightOrdered, err := evalExpression(rightCtx, root, target, inputOrdered, t.Expression(1), isRoot)
		if err != nil {
			return nil, false, err
		}

		return left.Union(right), leftOrdered && rightOrdered, nil

	case *parser.InequalityExpressionContext:
		left, _, err := evalExpression(ctx, root, target, inputOrdered, t.Expression(0), isRoot)
		if err != nil {
			return nil, false, err
		}
		right, _, err := evalExpression(ctx, root, target, inputOrdered, t.Expression(1), isRoot)
		if err != nil {
			return nil, false, err
		}
		op := t.GetChild(1).(antlr.ParseTree).GetText()

		cmp, ok, err := left.Cmp(right)
		if err != nil {
			return nil, false, err
		}
		if !ok {
			return nil, false, nil
		}

		result = Collection{Boolean(false)}
		switch op {
		case "<=":
			if cmp <= 0 {
				result, err = Collection{Boolean(true)}, nil
			}
		case "<":
			if cmp < 0 {
				result, err = Collection{Boolean(true)}, nil
			}
		case ">":
			if cmp > 0 {
				result, err = Collection{Boolean(true)}, nil
			}
		case ">=":
			if cmp >= 0 {
				result, err = Collection{Boolean(true)}, nil
			}
		}
		return result, true, err

	case *parser.EqualityExpressionContext:
		left, leftOrdered, err := evalExpression(ctx, root, target, inputOrdered, t.Expression(0), isRoot)
		if err != nil {
			return nil, false, err
		}
		right, rightOrdered, err := evalExpression(ctx, root, target, inputOrdered, t.Expression(1), isRoot)
		if err != nil {
			return nil, false, err
		}
		op := t.GetChild(1).(antlr.ParseTree).GetText()

		// for equality check, order is important
		if (op == "=" || op == "!=") &&
			(len(left) > 1 || len(right) > 1) &&
			(!leftOrdered || !rightOrdered) {
			return nil, false, fmt.Errorf("expected ordered inputs for equality expression")
		}

		switch op {
		case "=":
			eq, ok := left.Equal(right)
			if ok {
				result, err = Collection{Boolean(eq)}, nil
			}
		case "~":
			eq := left.Equivalent(right)
			result, err = Collection{Boolean(eq)}, nil
		case "!=":
			eq, ok := left.Equal(right)
			if ok {
				result, err = Collection{Boolean(!eq)}, nil
			}
		case "!~":
			eq := left.Equivalent(right)
			result, err = Collection{Boolean(!eq)}, nil
		}
		return result, true, err
	case *parser.MembershipExpressionContext:
		left, _, err := evalExpression(ctx, root, target, inputOrdered, t.Expression(0), isRoot)
		if err != nil {
			return nil, false, err
		}
		right, _, err := evalExpression(ctx, root, target, inputOrdered, t.Expression(1), isRoot)
		if err != nil {
			return nil, false, err
		}
		op := t.GetChild(1).(antlr.ParseTree).GetText()

		switch op {
		case "in":
			if len(left) == 0 {
				return nil, false, nil
			} else if len(left) > 1 {
				return nil, false, fmt.Errorf("left operand of \"in\" (membership) has more than 1 value")
			}
			result, err = Collection{Boolean(right.Contains(left[0]))}, nil
		case "contains":
			if len(right) == 0 {
				return nil, false, nil
			} else if len(right) > 1 {
				return nil, false, fmt.Errorf("left operand of \"contains\" (membership) has more than 1 value")
			}
			result, err = Collection{Boolean(left.Contains(right[0]))}, nil
		}
		return result, true, err

	case *parser.AndExpressionContext:
		left, _, err := evalExpression(ctx, root, target, inputOrdered, t.Expression(0), isRoot)
		if err != nil {
			return nil, false, err
		}
		right, _, err := evalExpression(ctx, root, target, inputOrdered, t.Expression(1), isRoot)
		if err != nil {
			return nil, false, err
		}

		leftSingle, leftOk, err := Singleton[Boolean](left)
		if err != nil {
			return nil, false, err
		}
		rightSingle, rightOk, err := Singleton[Boolean](right)
		if err != nil {
			return nil, false, err
		}

		if leftOk && leftSingle == true &&
			rightOk && rightSingle == true {
			result, err = Collection{Boolean(true)}, nil
		} else if leftOk && leftSingle == false {
			result, err = Collection{Boolean(false)}, nil
		} else if rightOk && rightSingle == false {
			result, err = Collection{Boolean(false)}, nil
		}
		return result, true, err

	case *parser.OrExpressionContext:
		left, _, err := evalExpression(ctx, root, target, inputOrdered, t.Expression(0), isRoot)
		if err != nil {
			return nil, false, err
		}
		right, _, err := evalExpression(ctx, root, target, inputOrdered, t.Expression(1), isRoot)
		if err != nil {
			return nil, false, err
		}
		op := t.GetChild(1).(antlr.ParseTree).GetText()

		leftSingle, leftOk, err := Singleton[Boolean](left)
		if err != nil {
			return nil, false, err
		}
		rightSingle, rightOk, err := Singleton[Boolean](right)
		if err != nil {
			return nil, false, err
		}

		switch op {
		case "or":
			if leftOk && leftSingle == false &&
				rightOk && rightSingle == false {
				result, err = Collection{Boolean(false)}, nil
			} else if leftOk && leftSingle == true {
				result, err = Collection{Boolean(true)}, nil
			} else if rightOk && rightSingle == true {
				result, err = Collection{Boolean(true)}, nil
			}
		case "xor":
			if (leftOk && leftSingle == true && rightOk && rightSingle == false) ||
				(leftOk && leftSingle == false && rightOk && rightSingle == true) {
				result, err = Collection{Boolean(true)}, nil
			} else if leftOk && rightOk &&
				rightSingle == leftSingle {
				result, err = Collection{Boolean(false)}, nil
			}
		}
		return result, true, err

	case *parser.ImpliesExpressionContext:
		left, _, err := evalExpression(ctx, root, target, inputOrdered, t.Expression(0), isRoot)
		if err != nil {
			return nil, false, err
		}
		right, _, err := evalExpression(ctx, root, target, inputOrdered, t.Expression(1), isRoot)
		if err != nil {
			return nil, false, err
		}

		leftSingle, leftOk, err := Singleton[Boolean](left)
		if err != nil {
			return nil, false, err
		}
		rightSingle, rightOk, err := Singleton[Boolean](right)
		if err != nil {
			return nil, false, err
		}

		if leftOk && leftSingle == true {
			if rightOk {
				return Collection{rightSingle}, true, nil
			} else {
				return nil, true, nil
			}
		} else if leftOk && leftSingle == false {
			return Collection{Boolean(true)}, true, nil
		} else if rightOk && rightSingle == true {
			return Collection{Boolean(true)}, true, nil
		} else {
			return nil, true, nil
		}

	default:
		panic(fmt.Sprintf("unexpected expression %T", tree))
	}
}

func evalTerm(
	ctx context.Context,
	root Element, target Collection,
	inputOrdered bool,
	tree parser.ITermContext,
	isRoot bool,
) (result Collection, resultOrdered bool, err error) {
	switch t := tree.(type) {
	case *parser.InvocationTermContext:
		return evalInvocation(ctx, root, target, inputOrdered, t.Invocation(), isRoot)
	case *parser.LiteralTermContext:
		return evalLiteral(t.Literal())
	case *parser.ExternalConstantTermContext:
		return evalExternalConstant(ctx, t.ExternalConstant())
	case *parser.ParenthesizedTermContext:
		return evalExpression(ctx, root, target, inputOrdered, t.Expression(), isRoot)
	default:
		return nil, false, fmt.Errorf("unexpected term %T", tree)
	}
}

func evalLiteral(
	tree parser.ILiteralContext,
) (result Collection, resultOrdered bool, err error) {
	s := tree.GetText()

	switch tt := tree.(type) {
	case *parser.NullLiteralContext:
		return nil, true, nil
	case *parser.BooleanLiteralContext:
		if s == "true" {
			return Collection{Boolean(true)}, true, nil
		} else if s == "false" {
			return Collection{Boolean(false)}, true, nil
		} else {
			return nil, false, fmt.Errorf("expected boolean literal, got %s", s)
		}
	case *parser.StringLiteralContext:
		unescaped, err := unescape(s[1 : len(s)-1])
		return Collection{String(unescaped)}, true, err
	case *parser.NumberLiteralContext:
		if strings.Contains(s, ".") {
			d, _, err := apd.NewFromString(s)
			return Collection{Decimal{Value: d}}, true, err
		}

		val, err := strconv.ParseInt(s, 10, 32)
		if err != nil {
			return nil, false, err
		}
		return Collection{Integer(val)}, true, nil
	case *parser.LongNumberLiteralContext:
		value := strings.TrimSuffix(s, "L")
		v, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return nil, false, err
		}
		return Collection{Long(v)}, true, nil
	case *parser.DateLiteralContext:
		d, err := ParseDate(s)
		return Collection{d}, true, err
	case *parser.TimeLiteralContext:
		t, err := ParseTime(s)
		return Collection{t}, true, err
	case *parser.DateTimeLiteralContext:
		dt, err := ParseDateTime(s)
		return Collection{dt}, true, err
	case *parser.QuantityLiteralContext:
		q, err := ParseQuantity(tt.Quantity().GetText())
		return Collection{q}, true, err
	default:
		return nil, false, fmt.Errorf("unexpected term %T: %v", tree, tree)
	}
}

type envKey struct{}

var systemVariables = map[string]Collection{
	"context": nil,
	"ucum":    Collection{String("http://unitsofmeasure.org")},
	"loinc":   Collection{String("http://loinc.org")},
	"sct":     Collection{String("http://snomed.info/sct")},
}

func WithEnv(ctx context.Context, name string, value Collection) context.Context {
	frame, ok := envStackFrame(ctx)
	if !ok {
		ctx, frame = withNewEnvStackFrame(ctx)
	}
	frame[name] = value
	return ctx
}

func withNewEnvStackFrame(ctx context.Context) (context.Context, map[string]Collection) {
	frame, ok := envStackFrame(ctx)
	if !ok {
		frame = make(map[string]Collection, len(systemVariables))
		for name, value := range systemVariables {
			frame[name] = value
		}
	}
	clonedFrame := maps.Clone(frame)
	return context.WithValue(ctx, envKey{}, clonedFrame), clonedFrame
}

func envStackFrame(ctx context.Context) (map[string]Collection, bool) {
	val, ok := ctx.Value(envKey{}).(map[string]Collection)
	if !ok {
		return nil, false
	}
	return val, true
}

func envValue(ctx context.Context, name string) (Collection, bool) {
	frame, ok := envStackFrame(ctx)
	if !ok {
		return nil, false
	}
	val, ok := frame[name]
	return val, ok
}

func evalExternalConstant(
	ctx context.Context,
	tree parser.IExternalConstantContext,
) (result Collection, resultOrdered bool, err error) {
	name := strings.TrimLeft(tree.GetText(), "%")
	value, ok := envValue(ctx, name)
	if !ok {
		return nil, false, fmt.Errorf("environment variable %q undefined", name)
	}
	return value, true, nil
}

func Singleton[T Element](c Collection) (v T, ok bool, err error) {
	if len(c) == 0 {
		return v, false, nil
	} else if len(c) > 1 {
		return v, false, fmt.Errorf("can not convert to singleton: collection contains > 1 values")
	}

	// convert to input type
	v, ok, err = elementTo[T](c[0], false)

	// if not convertible but contains a single value, evaluate to true
	if _, wantBool := any(v).(Boolean); err != nil && wantBool {
		return any(Boolean(true)).(T), true, nil
	}

	return v, ok, err
}
