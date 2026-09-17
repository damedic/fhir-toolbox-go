package wrap

import (
	"context"
	"testing"

	"github.com/damedic/fhir-toolbox-go/capabilities"
	"github.com/damedic/fhir-toolbox-go/capabilities/search"
	"github.com/damedic/fhir-toolbox-go/model"
	"github.com/damedic/fhir-toolbox-go/model/gen/r4"
	"github.com/damedic/fhir-toolbox-go/utils/ptr"
)

// genericOnlyBackend implements GenericCapabilities but not ConcreteCapabilities.
// This is the pattern used by the REST client (e.g. proxy example).
type genericOnlyBackend struct{}

var _ capabilities.GenericCapabilities = genericOnlyBackend{}

func (genericOnlyBackend) CapabilityStatement(ctx context.Context) (model.CapabilityStatement, error) {
	return nil, nil
}

// concreteOnlyBackend implements ConcreteCapabilities and one concrete read.
type concreteOnlyBackend struct {
	readPatientCalled bool
}

func (b *concreteOnlyBackend) CapabilityBase(ctx context.Context) (r4.CapabilityStatement, error) {
	return r4.CapabilityStatement{
		Status: r4.Code{Value: ptr.To("active")},
		Kind:   r4.Code{Value: ptr.To("instance")},
		Implementation: &r4.CapabilityStatementImplementation{
			Description: r4.String{Value: ptr.To("concrete backend")},
			Url:         &r4.Url{Value: ptr.To("http://example.com")},
		},
	}, nil
}

func (b *concreteOnlyBackend) ReadPatient(ctx context.Context, id string) (r4.Patient, error) {
	b.readPatientCalled = true
	return r4.Patient{Id: &r4.Id{Value: ptr.To(id)}}, nil
}

// mixedBackend implements GenericCapabilities and adds a concrete ReadPatient override.
// The concrete method should take precedence over the generic Read.
type mixedBackend struct {
	genericReadCalled  bool
	concreteReadCalled bool
}

var _ capabilities.GenericCapabilities = (*mixedBackend)(nil)
var _ capabilities.GenericRead = (*mixedBackend)(nil)

func (b *mixedBackend) CapabilityStatement(ctx context.Context) (model.CapabilityStatement, error) {
	return r4.CapabilityStatement{
		Status: r4.Code{Value: ptr.To("active")},
		Kind:   r4.Code{Value: ptr.To("instance")},
		Implementation: &r4.CapabilityStatementImplementation{
			Description: r4.String{Value: ptr.To("mixed backend")},
			Url:         &r4.Url{Value: ptr.To("http://example.com")},
		},
	}, nil
}

func (b *mixedBackend) Read(ctx context.Context, resourceType, id string) (model.Resource, error) {
	b.genericReadCalled = true
	return r4.Patient{Id: &r4.Id{Value: ptr.To(id)}}, nil
}

func (b *mixedBackend) ReadPatient(ctx context.Context, id string) (r4.Patient, error) {
	b.concreteReadCalled = true
	return r4.Patient{
		Id:   &r4.Id{Value: ptr.To(id)},
		Meta: &r4.Meta{VersionId: &r4.Id{Value: ptr.To("concrete-override")}},
	}, nil
}

// invalidBackend implements neither GenericCapabilities nor ConcreteCapabilities.
type invalidBackend struct{}

func TestGenericAccepted(t *testing.T) {
	tests := []struct {
		name   string
		wrapFn func(any) (capabilities.GenericCapabilities, error)
	}{
		{"R4", func(b any) (capabilities.GenericCapabilities, error) { return Generic[model.R4](b) }},
		{"R4B", func(b any) (capabilities.GenericCapabilities, error) { return Generic[model.R4B](b) }},
		{"R5", func(b any) (capabilities.GenericCapabilities, error) { return Generic[model.R5](b) }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			backend := genericOnlyBackend{}
			_, err := tt.wrapFn(backend)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestDoubleWrapDoesNotError(t *testing.T) {
	backend := genericOnlyBackend{}
	first, err := Generic[model.R4](backend)
	if err != nil {
		t.Fatalf("first wrap: %v", err)
	}
	_, err = Generic[model.R4](first)
	if err != nil {
		t.Fatalf("second wrap: %v", err)
	}
}

func TestConcreteWrapped(t *testing.T) {
	backend := &concreteOnlyBackend{}
	wrapped, err := Generic[model.R4](backend)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should be wrapped, not returned as-is (concrete gets wrapped in Generic struct)
	if wrapped == nil {
		t.Fatal("wrapped backend should not be nil")
	}

	reader, ok := wrapped.(capabilities.GenericRead)
	if !ok {
		t.Fatal("wrapped concrete backend does not implement GenericRead")
	}

	resource, err := reader.Read(context.Background(), "Patient", "42")
	if err != nil {
		t.Fatalf("Read returned error: %v", err)
	}
	if !backend.readPatientCalled {
		t.Fatal("concrete ReadPatient was not called")
	}
	id, _ := resource.ResourceId()
	if id != "42" {
		t.Fatalf("expected id 42, got %s", id)
	}
}

func TestConcreteNotImplementedResource(t *testing.T) {
	backend := &concreteOnlyBackend{}
	wrapped, err := Generic[model.R4](backend)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	reader := wrapped.(capabilities.GenericRead)
	_, err = reader.Read(context.Background(), "Observation", "1")
	if err == nil {
		t.Fatal("expected error for unimplemented resource type, got nil")
	}
}

func TestInvalidBackendErrors(t *testing.T) {
	backend := invalidBackend{}
	_, err := Generic[model.R4](backend)
	if err == nil {
		t.Fatal("expected error for backend with no capabilities, got nil")
	}
}

func TestMixedBackendReadPrecedence(t *testing.T) {
	tests := []struct {
		name               string
		resourceType       string
		wantConcreteCalled bool
		wantGenericCalled  bool
		wantConcreteMarker bool // Meta.VersionId == "concrete-override"
	}{
		{
			name:               "concrete ReadPatient takes precedence",
			resourceType:       "Patient",
			wantConcreteCalled: true,
			wantGenericCalled:  false,
			wantConcreteMarker: true,
		},
		{
			name:               "generic Read used for Observation",
			resourceType:       "Observation",
			wantConcreteCalled: false,
			wantGenericCalled:  true,
			wantConcreteMarker: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			backend := &mixedBackend{}
			wrapped, err := Generic[model.R4](backend)
			if err != nil {
				t.Fatalf("Generic[R4] returned error: %v", err)
			}

			reader := wrapped.(capabilities.GenericRead)
			resource, err := reader.Read(context.Background(), tt.resourceType, "1")
			if err != nil {
				t.Fatalf("Read returned error: %v", err)
			}
			if backend.concreteReadCalled != tt.wantConcreteCalled {
				t.Fatalf("concreteReadCalled = %v, want %v", backend.concreteReadCalled, tt.wantConcreteCalled)
			}
			if backend.genericReadCalled != tt.wantGenericCalled {
				t.Fatalf("genericReadCalled = %v, want %v", backend.genericReadCalled, tt.wantGenericCalled)
			}
			if tt.wantConcreteMarker {
				patient := resource.(r4.Patient)
				if patient.Meta == nil || patient.Meta.VersionId == nil || *patient.Meta.VersionId.Value != "concrete-override" {
					t.Fatal("response did not come from concrete ReadPatient")
				}
			}
		})
	}
}

func TestMixedBackendCapabilityStatementAugmented(t *testing.T) {
	backend := &mixedBackend{}
	wrapped, err := Generic[model.R4](backend)
	if err != nil {
		t.Fatalf("Generic[R4] returned error: %v", err)
	}

	cs, err := wrapped.CapabilityStatement(context.Background())
	if err != nil {
		t.Fatalf("CapabilityStatement returned error: %v", err)
	}

	r4cs := cs.(r4.CapabilityStatement)

	// The concrete ReadPatient should be detected in the CapabilityStatement
	var hasPatientRead bool
	for _, rest := range r4cs.Rest {
		for _, resource := range rest.Resource {
			if resource.Type.Value != nil && *resource.Type.Value == "Patient" {
				for _, interaction := range resource.Interaction {
					if interaction.Code.Value != nil && *interaction.Code.Value == "read" {
						hasPatientRead = true
					}
				}
			}
		}
	}
	if !hasPatientRead {
		t.Fatal("Patient read interaction not found in augmented CapabilityStatement")
	}
}

func TestCapabilityStatementFromConcrete(t *testing.T) {
	backend := &concreteOnlyBackend{}
	wrapped, err := Generic[model.R4](backend)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	cs, err := wrapped.CapabilityStatement(context.Background())
	if err != nil {
		t.Fatalf("CapabilityStatement returned error: %v", err)
	}

	r4cs, ok := cs.(r4.CapabilityStatement)
	if !ok {
		t.Fatalf("expected r4.CapabilityStatement, got %T", cs)
	}

	// The concrete ReadPatient should be detected and advertised
	var hasReadInteraction bool
	for _, rest := range r4cs.Rest {
		for _, resource := range rest.Resource {
			if resource.Type.Value != nil && *resource.Type.Value == "Patient" {
				for _, interaction := range resource.Interaction {
					if interaction.Code.Value != nil && *interaction.Code.Value == "read" {
						hasReadInteraction = true
					}
				}
			}
		}
	}
	if !hasReadInteraction {
		t.Fatal("read interaction for Patient not found in CapabilityStatement")
	}
}

// searchParameterResource returns the SearchParameter rest resource of the given
// CapabilityStatement and fails the test if it is not present exactly once.
func searchParameterResource(t *testing.T, cs model.CapabilityStatement) r4.CapabilityStatementRestResource {
	t.Helper()
	r4cs, ok := cs.(r4.CapabilityStatement)
	if !ok {
		t.Fatalf("expected r4.CapabilityStatement, got %T", cs)
	}
	var found []r4.CapabilityStatementRestResource
	for _, rest := range r4cs.Rest {
		for _, resource := range rest.Resource {
			if resource.Type.Value != nil && *resource.Type.Value == "SearchParameter" {
				found = append(found, resource)
			}
		}
	}
	if len(found) != 1 {
		t.Fatalf("expected exactly one SearchParameter resource, got %d", len(found))
	}
	return found[0]
}

func assertNoDuplicateCapabilities(t *testing.T, resource r4.CapabilityStatementRestResource) {
	t.Helper()
	interactions := map[string]int{}
	for _, interaction := range resource.Interaction {
		if interaction.Code.Value != nil {
			interactions[*interaction.Code.Value]++
		}
	}
	for code, count := range interactions {
		if count != 1 {
			t.Errorf("interaction %q listed %d times, want 1", code, count)
		}
	}
	params := map[string]int{}
	for _, param := range resource.SearchParam {
		if param.Name.Value != nil {
			params[*param.Name.Value]++
		}
	}
	for name, count := range params {
		if count != 1 {
			t.Errorf("searchParam %q listed %d times, want 1", name, count)
		}
	}
}

// Regression test for https://github.com/damedic/fhir-toolbox-go/issues/11.
// Wrapping an already wrapped backend (e.g. user code wraps in capabilitiesR4.Generic
// and the REST server wraps again) must not duplicate the automatically added
// SearchParameter read/search-type interactions and the _id search parameter.
func TestDoubleWrapNoDuplicateSearchParameterCapabilities(t *testing.T) {
	first, err := Generic[model.R4](&concreteOnlyBackend{})
	if err != nil {
		t.Fatalf("first wrap: %v", err)
	}
	second, err := Generic[model.R4](first)
	if err != nil {
		t.Fatalf("second wrap: %v", err)
	}

	cs, err := second.CapabilityStatement(context.Background())
	if err != nil {
		t.Fatalf("CapabilityStatement returned error: %v", err)
	}

	sp := searchParameterResource(t, cs)
	assertNoDuplicateCapabilities(t, sp)
	if len(sp.Interaction) != 2 {
		t.Errorf("expected 2 interactions (read, search-type), got %d", len(sp.Interaction))
	}
	if len(sp.SearchParam) != 1 {
		t.Errorf("expected 1 searchParam (_id), got %d", len(sp.SearchParam))
	}
}

// baseWithPatientBackend declares Patient read and search in its CapabilityBase
// and also implements the corresponding concrete methods.
type baseWithPatientBackend struct {
	concreteOnlyBackend
}

func (b *baseWithPatientBackend) CapabilityBase(ctx context.Context) (r4.CapabilityStatement, error) {
	cs, err := b.concreteOnlyBackend.CapabilityBase(ctx)
	if err != nil {
		return cs, err
	}
	cs.Rest = []r4.CapabilityStatementRest{{
		Mode: r4.Code{Value: ptr.To("server")},
		Resource: []r4.CapabilityStatementRestResource{{
			Type: r4.Code{Value: ptr.To("Patient")},
			Interaction: []r4.CapabilityStatementRestResourceInteraction{
				{Code: r4.Code{Value: ptr.To("read")}},
				{Code: r4.Code{Value: ptr.To("search-type")}},
			},
			SearchParam: []r4.CapabilityStatementRestResourceSearchParam{{
				Name: r4.String{Value: ptr.To("_id")},
				Type: r4.Code{Value: ptr.To("token")},
			}},
		}},
	}}
	return cs, nil
}

func (b *baseWithPatientBackend) SearchCapabilitiesPatient(ctx context.Context) (r4.SearchCapabilities, error) {
	return r4.SearchCapabilities{
		Parameters: map[string]r4.SearchParameter{
			"_id": {Type: r4.Code{Value: ptr.To("token")}},
		},
	}, nil
}

func (b *baseWithPatientBackend) SearchPatient(ctx context.Context, parameters search.Parameters, options search.Options) (search.Result[r4.Patient], error) {
	return search.Result[r4.Patient]{}, nil
}

// Interactions and search parameters already declared in the base
// CapabilityStatement must not be listed twice after augmentation.
func TestBaseDeclaredCapabilitiesNotDuplicated(t *testing.T) {
	wrapped, err := Generic[model.R4](&baseWithPatientBackend{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	cs, err := wrapped.CapabilityStatement(context.Background())
	if err != nil {
		t.Fatalf("CapabilityStatement returned error: %v", err)
	}

	r4cs := cs.(r4.CapabilityStatement)
	var patient *r4.CapabilityStatementRestResource
	for _, rest := range r4cs.Rest {
		for i := range rest.Resource {
			if rest.Resource[i].Type.Value != nil && *rest.Resource[i].Type.Value == "Patient" {
				patient = &rest.Resource[i]
			}
		}
	}
	if patient == nil {
		t.Fatal("Patient resource not found in CapabilityStatement")
	}
	assertNoDuplicateCapabilities(t, *patient)
	if len(patient.Interaction) != 2 {
		t.Errorf("expected 2 interactions (read, search-type), got %d", len(patient.Interaction))
	}
	if len(patient.SearchParam) != 1 {
		t.Errorf("expected 1 searchParam (_id), got %d", len(patient.SearchParam))
	}
}
