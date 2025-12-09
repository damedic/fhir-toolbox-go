package assert

import (
	"testing"

	"github.com/damedic/fhir-toolbox-go/fhirpath"
	"github.com/damedic/fhir-toolbox-go/testdata/assert/internal/diff"
)

func FHIRPathEqual(t *testing.T, expected, actual fhirpath.Collection) {
	// use equivalence to have empty results { } ~ { } result in true
	if !expected.Equivalent(actual) {
		t.Error(string(diff.Diff("expected", []byte(expected.String()), "actual", []byte(actual.String()))))
	}
}
