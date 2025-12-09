// Package capabilities provides interfaces modeling capabilities.
// This flexible architecture allows different use cases, such as
//
//   - building FHIR® facades to legacy systems by implementing a custom backend
//   - using this library as a FHIR® client (via the provided REST client)
//
// # Concrete vs. Generic API
//
// The library provides two API styles.
// The concrete API:
//
//	func (a myAPI) ReadPatient(ctx context.Context, id string) (r4.Patient, error) {}
//
//	func (a myAPI) SearchPatient(ctx context.Context, options search.Options) (search.Result, error) {}
//
// and the generic API:
//
//	func (a myAPI) Read(ctx context.Context, resourceType, id string) (r4.Patient, error) {}
//
//	func (a myAPI) Search(ctx context.Context, resourceType string, options search.Options) (search.Result, error) {}
//
// You can implement your custom backend or client either way.
// The concrete API is ideal for building custom FHIR® facades where a limited set of resources is used.
// The generic API is better suited for e.g. building FHIR® clients or standalone FHIR® servers.
//
// # Interoperability
//
// Wrapper functions are available (in rest/internal/wrap) to convert between concrete and generic APIs:
//
//	genericAPI := wrap.Generic[model.R4](concreteAPI)
//
// and vice versa:
//
//	concreteAPI := wrap.ConcreteR4(genericAPI)
package capabilities

import (
	"context"
	"github.com/damedic/fhir-toolbox-go/model"
)

// The ConcreteCapabilities interface allows concrete implementations to provide a base CapabilityStatement
// that will be enhanced with the detected concrete capabilities. This is useful for setting implementation
// details, base URLs, and other metadata that should be preserved in the final CapabilityStatement.
// ConcreteCapabilities is a generic interface for backends that provide a
// base CapabilityStatement of the concrete release type C.
//
// C must be a release-specific type that satisfies model.CapabilityStatement
// (e.g., r4.CapabilityStatement, r4b.CapabilityStatement, r5.CapabilityStatement).
type ConcreteCapabilities[C model.CapabilityStatement] interface {
	CapabilityBase(ctx context.Context) (C, error)
}
