package fhirpath

import "testing"

// A FHIR primitive without a value (only extensions) has no system value, so
// comparing it behaves like comparing with empty and yields empty instead of
// an error. Regression test for the upstream test "testNoValueComparison".
func TestCollectionCmpValuelessPrimitive(t *testing.T) {
	valueless := fakeFHIRPrimitive{Element: String("abc"), hasValue: false}
	valued := fakeFHIRPrimitive{Element: String("abc"), hasValue: true}

	tests := []struct {
		name        string
		left, right Collection
		wantOk      bool
	}{
		{"valueless left", Collection{valueless}, Collection{String("x")}, false},
		{"valueless right", Collection{String("x")}, Collection{valueless}, false},
		{"both valueless", Collection{valueless}, Collection{valueless}, false},
		{"valued left", Collection{valued}, Collection{String("x")}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, ok, err := tt.left.Cmp(tt.right)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if ok != tt.wantOk {
				t.Fatalf("ok = %v, want %v", ok, tt.wantOk)
			}
		})
	}
}
