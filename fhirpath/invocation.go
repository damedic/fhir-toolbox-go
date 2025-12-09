package fhirpath

import (
	"context"
	"fmt"
	"strings"

	"github.com/antlr4-go/antlr/v4"
	parser "github.com/damedic/fhir-toolbox-go/fhirpath/internal/parser"
)

func evalInvocation(
	ctx context.Context,
	root Element, target Collection,
	inputOrdered bool,
	tree parser.IInvocationContext,
	isRoot bool,
) (Collection, bool, error) {
	switch t := tree.(type) {
	case *parser.MemberInvocationContext:
		ident, err := evalIdentifier(t.Identifier())
		if err != nil {
			return nil, false, err
		}

		// Try field access first if we have target elements
		// This prevents identifiers that happen to be type names (like "id")
		// from being treated as type checks when they should be field accesses
		var members Collection
		for _, e := range target {
			members = append(members, e.Children(ident)...)
		}

		// If field access succeeded, return the results
		if len(members) > 0 {
			return members, inputOrdered, nil
		}

		// Fall back to type check if isRoot and identifier resolves to a type
		if isRoot {
			expectedType, ok := resolveType(ctx, TypeSpecifier{Name: ident})
			if ok {
				rootType := root.TypeInfo()
				if !subTypeOf(ctx, rootType, expectedType) {
					return nil, false, fmt.Errorf("expected element of type %s, got %s", expectedType, rootType)
				}
				return Collection{root}, inputOrdered, nil
			}
		}

		// Return empty if neither field access nor type check succeeded
		return members, inputOrdered, nil
	case *parser.FunctionInvocationContext:
		return evalFunc(ctx, root, target, inputOrdered, t.Function())
	case *parser.ThisInvocationContext:
		scope, err := getFunctionScope(ctx)
		if err == nil {
			return Collection{scope.this}, true, nil
		}
		return Collection{root}, true, nil
	case *parser.IndexInvocationContext:
		scope, err := getFunctionScope(ctx)
		if err != nil {
			return nil, false, err
		}
		return Collection{Integer(scope.index)}, true, nil
	case *parser.TotalInvocationContext:
		scope, err := getFunctionScope(ctx)
		if err != nil {
			return nil, false, err
		}
		if !scope.aggregate {
			return nil, false, fmt.Errorf("$total not defined (only in aggregate)")
		}
		return scope.total, true, nil
	default:
		return nil, false, fmt.Errorf("unexpected invocation %T", tree)
	}
}

func evalQualifiedIdentifier(tree parser.IQualifiedIdentifierContext) (TypeSpecifier, error) {
	var idents []string
	for _, i := range tree.AllIdentifier() {
		ident, err := evalIdentifier(i)
		if err != nil {
			return TypeSpecifier{}, err
		}
		idents = append(idents, ident)
	}

	return TypeSpecifier{
		Namespace: strings.Join(idents[:len(idents)-1], "."),
		Name:      idents[len(idents)-1],
	}, nil
}

func evalIdentifier(tree parser.IIdentifierContext) (string, error) {
	s := tree.GetText()

	if tree.DELIMITEDIDENTIFIER() != nil {
		return unescape(s[1 : len(s)-1])
	}

	return s, nil
}

func evalFunc(
	ctx context.Context,
	root Element, target Collection,
	inputOrdered bool,
	tree parser.IFunctionContext,
) (Collection, bool, error) {
	var (
		ident      string
		paramExprs []Expression
		err        error
	)

	if tree.Identifier() == nil {
		ident = "sort"
		paramExprs, err = buildSortArguments(tree)
		if err != nil {
			return nil, false, err
		}
	} else {
		ident, err = evalIdentifier(tree.Identifier())
		if err != nil {
			return nil, false, err
		}

		if paramList := tree.ParamList(); paramList != nil {
			paramExprs = buildParamExpressions(paramList.AllExpression())
		}
	}

	return callFunc(ctx, root, target, inputOrdered, ident, paramExprs)
}

func callFunc(
	ctx context.Context,
	root Element, target Collection,
	inputOrdered bool,
	ident string,
	paramExprs []Expression,
) (Collection, bool, error) {
	fn, ok := getFunction(ctx, ident)
	if !ok {
		return nil, false, fmt.Errorf("function \"%s\" not found", ident)
	}

	result, ordered, err := fn(
		ctx,
		root, target,
		inputOrdered,
		paramExprs,
		func(
			ctx context.Context,
			target Collection,
			expr Expression,
			fnScope ...FunctionScope,
		) (result Collection, resultOrdered bool, err error) {
			// Create isolated environment scope for ALL parameter evaluations
			// This prevents variables defined in parameter expressions from colliding
			ctx, _ = withNewEnvStackFrame(ctx)

			parentScope, parentErr := getFunctionScope(ctx)

			if len(fnScope) > 0 {
				scope := functionScope{
					index: fnScope[0].index,
				}

				if len(target) == 1 {
					scope.this = target[0]
				}

				// Preserve aggregate context from parent
				if parentErr == nil && parentScope.aggregate {
					scope.aggregate = true
					scope.total = parentScope.total
				}

				// Set aggregate context if this is the aggregate function
				if ident == "aggregate" {
					scope.aggregate = true
					scope.total = fnScope[0].total
				}

				ctx = withFunctionScope(ctx, scope)
			}
			// Determine the evaluation target for the parameter expression:
			//  1. Use the explicit target supplied by the caller (e.g., select/where).
			//  2. Otherwise, fall back to the current function scope's $this if present.
			//  3. Finally, fall back to the root element of the overall evaluation.
			evalTarget := target
			if len(evalTarget) == 0 {
				if scope, err := getFunctionScope(ctx); err == nil && scope.this != nil {
					evalTarget = Collection{scope.this}
				} else if root != nil {
					evalTarget = Collection{root}
				}
			}

			return evalExpression(ctx,
				root, evalTarget,
				true,
				expr.tree, true,
			)
		},
	)
	if err != nil {
		return nil, false, err
	}
	return result, ordered, nil
}

func buildParamExpressions(paramTerms []parser.IExpressionContext) []Expression {
	if len(paramTerms) == 0 {
		return nil
	}
	exprs := make([]Expression, 0, len(paramTerms))
	for _, param := range paramTerms {
		exprs = append(exprs, Expression{tree: param})
	}
	return exprs
}

func buildSortArguments(tree parser.IFunctionContext) ([]Expression, error) {
	sortArgs := tree.AllSortArgument()
	if len(sortArgs) == 0 {
		return nil, nil
	}

	exprs := make([]Expression, 0, len(sortArgs))
	for _, arg := range sortArgs {
		argCtx, ok := arg.(*parser.SortDirectionArgumentContext)
		if !ok {
			return nil, fmt.Errorf("unexpected sort argument type %T", arg)
		}

		dir := sortDirectionFromArgument(argCtx)
		exprCtx := argCtx.Expression()
		exprCtx, legacyDir := normalizeLegacySortDirection(exprCtx)
		if dir == sortDirectionNone {
			dir = legacyDir
		}
		if dir == sortDirectionNone {
			dir = sortDirectionAsc
		}

		exprs = append(exprs, Expression{
			tree:          exprCtx,
			sortDirection: dir,
		})
	}
	return exprs, nil
}

func sortDirectionFromArgument(arg *parser.SortDirectionArgumentContext) sortDirection {
	for i := 0; i < arg.GetChildCount(); i++ {
		if terminal, ok := arg.GetChild(i).(antlr.TerminalNode); ok {
			switch terminal.GetText() {
			case "asc":
				return sortDirectionAsc
			case "desc":
				return sortDirectionDesc
			}
		}
	}
	return sortDirectionNone
}

func normalizeLegacySortDirection(expr parser.IExpressionContext) (parser.IExpressionContext, sortDirection) {
	if polarity, ok := expr.(*parser.PolarityExpressionContext); ok {
		if polarity.GetChildCount() > 0 {
			if token, ok := polarity.GetChild(0).(antlr.ParseTree); ok && token.GetText() == "-" {
				return polarity.Expression(), sortDirectionDesc
			}
		}
	}
	return expr, sortDirectionNone
}
