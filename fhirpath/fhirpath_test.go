package fhirpath_test

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"testing"

	"github.com/cockroachdb/apd/v3"
	"github.com/damedic/fhir-toolbox-go/fhirpath"
	"github.com/damedic/fhir-toolbox-go/model"
	"github.com/damedic/fhir-toolbox-go/model/gen/r4"
	"github.com/damedic/fhir-toolbox-go/model/gen/r4b"
	"github.com/damedic/fhir-toolbox-go/model/gen/r5"
	"github.com/damedic/fhir-toolbox-go/testdata"
	"github.com/damedic/fhir-toolbox-go/testdata/assert"
)

// runFHIRPathTest executes a single FHIRPath test and validates the result
func runFHIRPathTest(t *testing.T, ctx context.Context, test testdata.FHIRPathTest) {
	defer func() {
		if err := recover(); err != nil {
			t.Fatal(err)
		}
	}()

	expr, err := fhirpath.Parse(test.Expression.Expression)
	if err != nil && (test.Invalid != "" || test.Expression.Invalid != "") {
		return
	}
	if err != nil {
		t.Fatalf("Unexpected error parsing expression: %v", err)
	}

	result, err := fhirpath.Evaluate(
		ctx,
		test.InputResource,
		expr,
	)
	if err != nil && test.Expression.Invalid != "" {
		return
	}
	if err != nil {
		t.Fatalf("Unexpected error evaluating expression: %v", err)
	}

	if test.Predicate {
		v, ok, err := fhirpath.Singleton[fhirpath.Boolean](result)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if !ok {
			t.Fatalf("expected boolean value to exist")
		}
		result = fhirpath.Collection{v}
	}

	expected := test.OutputCollection()
	fmt.Printf(
		"FHIRPath test %s (inputfile=%s)\n  expression: %s\n  expected: %s\n  actual: %s\n",
		test.Name,
		test.InputFile,
		strings.TrimSpace(test.Expression.Expression),
		expected.String(),
		result.String(),
	)
	assert.FHIRPathEqual(t, expected, result)
}

func runFHIRPathSuite(t *testing.T, ctx context.Context, release model.Release) {
	t.Helper()

	tests := testdata.GetFHIRPathTests(release)

	for _, group := range tests.Groups {
		group := group
		name := group.Name
		if group.Description != "" {
			name = fmt.Sprintf("%s (%s)", name, group.Description)
		}

		t.Run(name, func(t *testing.T) {
			for _, test := range group.Tests {
				test := test
				name := test.Name
				if test.Description != "" {
					name = fmt.Sprintf("%s (%s)", name, test.Description)
				}

				t.Run(name, func(t *testing.T) {
					if ok, reason := shouldSkipTest(test, release); ok {
						t.Skip(reason)
					}
					runFHIRPathTest(t, ctx, test)
				})
			}
		})
	}
}

func TestFHIRPathTestSuites(t *testing.T) {
	for _, release := range testdata.TestReleases {
		release := release
		t.Run(release.String(), func(t *testing.T) {
			var ctx context.Context
			switch release.(type) {
			case model.R4:
				ctx = r4.Context()
			case model.R4B:
				ctx = r4b.Context()
			case model.R5:
				ctx = r5.Context()
			default:
				t.Fatalf("no context configured for release %s", release.String())
			}
			ctx = fhirpath.WithAPDContext(ctx, apd.BaseContext.WithPrecision(8))
			runFHIRPathSuite(t, ctx, release)
		})
	}
}

func isCDATest(test testdata.FHIRPathTest) bool {
	return strings.EqualFold(test.Mode, "cda")
}

func isR5Release(release model.Release) bool {
	_, ok := release.(model.R5)
	return ok
}

func isNotR5Release(release model.Release) bool {
	return !isR5Release(release)
}

type skipRule struct {
	pattern       *regexp.Regexp
	releaseFilter func(model.Release) bool
	reason        string
}

// Test skip rules are organized into two categories:
//
// 1. testSkipsSpecIssues: Tests that don't align with the spec or where the spec is unclear/ambiguous.
//    These represent issues with the test suite itself or areas where the spec needs clarification.
//
// 2. testSkipsImplementationGaps: Tests that are correct per spec, but our implementation doesn't match yet.
//    These represent work that contributors can help with - functions, semantics, or infrastructure to be added.

// Tests that are not in line with spec or where spec is unclear/ambiguous
var testSkipsSpecIssues = []skipRule{
	// testPolymorphismB: Test is marked invalid="semantic" and demonstrates invalid polymorphic field access.
	// The expression "Observation.valueQuantity.unit" is semantically incorrect per FHIR - should use
	// "Observation.value.unit" or "Observation.value.as(Quantity).unit" instead. Per FHIRPath spec, semantic
	// errors don't throw exceptions but return empty collections. This test is informational only.
	{regexp.MustCompile(`^testPolymorphismB$`), nil, "test demonstrates invalid polymorphic field access (invalid=\"semantic\") - informational only"},
	// testPrecedence3 & testPrecedence4: The tests are wrong. In the formal FHIRPath grammar,
	// type operators (is, as) bind tighter than comparison operators (>, <, etc.)
	{regexp.MustCompile(`^testPrecedence[34]$`), nil, "test uses wrong precedence - type operators bind tighter than comparison operators"},
	// testPlusDate19: R4/R4B test expects @...T00:00:00.000 + 0.1 's' = @...T00:00:00.000 (unchanged),
	// but R5 test (correctly) expects @...T00:00:00.100. Implementation follows R5 behavior.
	{regexp.MustCompile(`^testPlusDate19$`), isNotR5Release, "R4/R4B test expects no change when adding 0.1s, but implementation (correctly) adds fractional seconds per R5 test"},
	// Contested tests: These tests expect behavior contrary to the FHIR spec. Per the FHIR specification,
	// derived types like FHIR.code are subtypes of their base types (FHIR.string in this case).
	// Tests marked "contested" expect ofType/as operations to NOT match derived types against base types,
	// but our implementation correctly follows the FHIR type hierarchy.
	{regexp.MustCompile(`^testFHIRPathAsFunction(11|16)$`), nil, "Contested: expects code NOT to be subtype of string, but FHIR spec defines it as such"},
	// testIif6: Test expects empty result when iif() receives non-Boolean criterion, but per FHIRPath singleton
	// evaluation rule, a single non-Boolean value evaluates to true in Boolean context. Our implementation
	// correctly applies this rule.
	{regexp.MustCompile(`^testIif6$`), nil, "test expects strict behavior that contradicts singleton evaluation rule - implementation correctly applies spec"},
}

// Tests that are correct per spec, but our implementation doesn't match yet
var testSkipsImplementationGaps = []skipRule{
	// Functions not yet implemented
	{regexp.MustCompile(`^testMultipleResolve$`), nil, "resolve() function not implemented"},
	{regexp.MustCompile(`^testConformsTo.*`), nil, "conformsTo() function not implemented"},

	// Terminology service infrastructure not available
	{regexp.MustCompile(`^txTest0[1-3]$`), nil, "Terminology service not configured"},
	{regexp.MustCompile(`^testVariables4$`), nil, "%vs variables not supported (requires terminology service)"},
	{regexp.MustCompile(`^testExtension2$`), nil, "%ext variables not supported"},
}

var unimplementedTestSkips = append(testSkipsSpecIssues, testSkipsImplementationGaps...)

func shouldSkipTest(test testdata.FHIRPathTest, release model.Release) (bool, string) {
	if isCDATest(test) && isR5Release(release) {
		return true, "CDA-based FHIRPath inputs not supported yet"
	}

	for _, rule := range unimplementedTestSkips {
		if rule.pattern.MatchString(test.Name) {
			if rule.releaseFilter == nil || rule.releaseFilter(release) {
				return true, rule.reason
			}
		}
	}

	return false, ""
}
