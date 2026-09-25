package fhirpath_test

import (
	"context"
	"testing"

	"github.com/damedic/fhir-toolbox-go/fhirpath"
	"github.com/damedic/fhir-toolbox-go/model"
	"github.com/damedic/fhir-toolbox-go/model/gen/r4"
	"github.com/damedic/fhir-toolbox-go/utils/ptr"
)

func patientWithContained() r4.Patient {
	return r4.Patient{
		Id: &r4.Id{Value: ptr.To("p1")},
		Contained: []model.Resource{
			r4.Organization{Id: &r4.Id{Value: ptr.To("o1")}},
			r4.Practitioner{Id: &r4.Id{Value: ptr.To("pr1")}},
		},
		ManagingOrganization: &r4.Reference{Reference: &r4.String{Value: ptr.To("#o1")}},
	}
}

func evalStrings(t *testing.T, ctx context.Context, target fhirpath.Element, expr string) []string {
	t.Helper()
	result, err := fhirpath.Evaluate(ctx, target, fhirpath.MustParse(expr))
	if err != nil {
		t.Fatalf("%s: unexpected error: %v", expr, err)
	}
	var out []string
	for _, e := range result {
		s, ok, err := e.ToString(false)
		if err != nil || !ok {
			t.Fatalf("%s: result %v is not a string", expr, e)
		}
		out = append(out, string(s))
	}
	return out
}

func evalBool(t *testing.T, ctx context.Context, target fhirpath.Element, expr string) bool {
	t.Helper()
	result, err := fhirpath.Evaluate(ctx, target, fhirpath.MustParse(expr))
	if err != nil {
		t.Fatalf("%s: unexpected error: %v", expr, err)
	}
	b, ok, err := fhirpath.Singleton[fhirpath.Boolean](result)
	if err != nil || !ok {
		t.Fatalf("%s: expected boolean result, got %v (err %v)", expr, result, err)
	}
	return bool(b)
}

// Both variables refer to the target when it is a resource. Spec: "Special variables".
func TestResourceVariablesBoundToTarget(t *testing.T) {
	ctx := r4.Context()
	patient := patientWithContained()

	tests := []struct {
		expr string
		want []string
	}{
		{"%resource.id", []string{"p1"}},
		{"%rootResource.id", []string{"p1"}},
		{"%context.id", []string{"p1"}},
		// Plain path navigation does not pass "into" the contained resource.
		{"contained.id", []string{"o1", "pr1"}},
	}
	for _, tt := range tests {
		t.Run(tt.expr, func(t *testing.T) {
			got := evalStrings(t, ctx, patient, tt.expr)
			if len(got) != len(tt.want) {
				t.Fatalf("got %v, want %v", got, tt.want)
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Fatalf("got %v, want %v", got, tt.want)
				}
			}
		})
	}
}

// %resource is re-bound to a contained resource while a scoped function focuses on it,
// %rootResource is not. Spec: "%resource ... When passing through resolve() or into a
// contained resource will be changed to the new resource context." and "%rootResource ...
// (Does not change during execution)".
func TestResourceVariableRebindsInContained(t *testing.T) {
	ctx := r4.Context()
	patient := patientWithContained()

	tests := []struct {
		expr string
		want []string
	}{
		{"contained.select(%resource.id)", []string{"o1", "pr1"}},
		{"contained.select(%rootResource.id)", []string{"p1", "p1"}},
		{"contained.select(%context.id)", []string{"p1", "p1"}},
		{"contained.where(%resource.id = 'pr1').id", []string{"pr1"}},
		// Nested scoped functions inherit the re-bound %resource.
		{"contained.select(id.select(%resource.id))", []string{"o1", "pr1"}},
		// Non-resource focus items keep the enclosing %resource.
		{"contained.id.select(%resource.id)", []string{"p1", "p1"}},
	}
	for _, tt := range tests {
		t.Run(tt.expr, func(t *testing.T) {
			got := evalStrings(t, ctx, patient, tt.expr)
			if len(got) != len(tt.want) {
				t.Fatalf("got %v, want %v", got, tt.want)
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Fatalf("got %v, want %v", got, tt.want)
				}
			}
		})
	}
}

// The re-binding is scoped to the function parameter and reverts afterwards.
func TestResourceVariableRevertsAfterScopedFunction(t *testing.T) {
	ctx := r4.Context()
	patient := patientWithContained()

	if !evalBool(t, ctx, patient, "contained.select(%resource.id).count() = 2 and %resource.id = 'p1'") {
		t.Fatal("%resource should refer to the Patient again after select() returns")
	}
	if !evalBool(t, ctx, patient, "contained.exists(%resource.id = 'o1') and %resource.id = 'p1'") {
		t.Fatal("%resource should refer to the Patient again after exists() returns")
	}
	// Local references in the outer resource can be checked from inside a contained resource
	// via %rootResource.
	if !evalBool(t, ctx, patient, "contained.all(('#' + id) in %rootResource.descendants().reference or id = 'pr1')") {
		t.Fatal("expected %rootResource to give access to the containing Patient")
	}
}

// Bundle entries are nested resources as well.
func TestResourceVariableRebindsInBundleEntries(t *testing.T) {
	ctx := r4.Context()
	bundle := r4.Bundle{
		Id:   &r4.Id{Value: ptr.To("b1")},
		Type: r4.Code{Value: ptr.To("collection")},
		Entry: []r4.BundleEntry{
			{Resource: r4.Patient{Id: &r4.Id{Value: ptr.To("p1")}}},
			{Resource: r4.Patient{Id: &r4.Id{Value: ptr.To("p2")}}},
		},
	}

	got := evalStrings(t, ctx, bundle, "entry.resource.select(%resource.id)")
	if len(got) != 2 || got[0] != "p1" || got[1] != "p2" {
		t.Fatalf("got %v, want [p1 p2]", got)
	}
	got = evalStrings(t, ctx, bundle, "entry.resource.select(%rootResource.id)")
	if len(got) != 2 || got[0] != "b1" || got[1] != "b1" {
		t.Fatalf("got %v, want [b1 b1]", got)
	}
	// The focus item of entry.select() is a BundleEntry, not a resource.
	got = evalStrings(t, ctx, bundle, "entry.select(%resource.id)")
	if len(got) != 2 || got[0] != "b1" || got[1] != "b1" {
		t.Fatalf("got %v, want [b1 b1]", got)
	}
}

// Values supplied by the caller take precedence, which is how validators evaluate invariants
// against a nested element.
func TestResourceVariablesCallerOverride(t *testing.T) {
	patient := patientWithContained()
	practitioner := r4.Practitioner{Id: &r4.Id{Value: ptr.To("pr1")}}
	name := r4.HumanName{Family: &r4.String{Value: ptr.To("Windsor")}}

	ctx := r4.Context()
	ctx = fhirpath.WithEnv(ctx, "resource", fhirpath.Collection{practitioner})
	ctx = fhirpath.WithEnv(ctx, "rootResource", fhirpath.Collection{patient})

	// Target is a resource, but the caller-supplied bindings win.
	got := evalStrings(t, ctx, patient, "%resource.id | %rootResource.id")
	if len(got) != 2 || got[0] != "pr1" || got[1] != "p1" {
		t.Fatalf("got %v, want [pr1 p1]", got)
	}

	// Target is a nested element; the caller-supplied bindings make the variables available.
	got = evalStrings(t, ctx, name, "family + '/' + %resource.id + '/' + %rootResource.id")
	if len(got) != 1 || got[0] != "Windsor/pr1/p1" {
		t.Fatalf("got %v, want [Windsor/pr1/p1]", got)
	}
}

// Without a resource target and without caller-supplied values, the variables stay undefined
// so that the mistake surfaces instead of silently yielding a wrong answer.
func TestResourceVariablesUndefinedForNonResourceTarget(t *testing.T) {
	ctx := r4.Context()
	name := r4.HumanName{Family: &r4.String{Value: ptr.To("Windsor")}}

	for _, expr := range []string{"%resource", "%rootResource"} {
		_, err := fhirpath.Evaluate(ctx, name, fhirpath.MustParse(expr))
		if err == nil {
			t.Fatalf("%s: expected error for non-resource target, got nil", expr)
		}
	}
}
