package wrap

import (
	"context"
	"testing"

	"github.com/damedic/fhir-toolbox-go/capabilities"
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

// invalidBackend implements neither GenericCapabilities nor ConcreteCapabilities.
type invalidBackend struct{}

func TestGenericPassthrough(t *testing.T) {
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
			got, err := tt.wrapFn(backend)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != backend {
				t.Fatal("expected original backend to be returned as-is")
			}
		})
	}
}

func TestGenericIdempotent(t *testing.T) {
	backend := genericOnlyBackend{}
	first, err := Generic[model.R4](backend)
	if err != nil {
		t.Fatalf("first wrap: %v", err)
	}
	second, err := Generic[model.R4](first)
	if err != nil {
		t.Fatalf("second wrap: %v", err)
	}
	if second != backend {
		t.Fatal("double-wrap did not return original backend")
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
