package model

import (
	"github.com/damedic/fhir-toolbox-go/fhirpath"
)

// Element is any element in the FHIR model.
//
// This includes Resources, Datatypes and BackboneElements.
type Element interface {
	fhirpath.Element
	MemSize() int
}

// Resource is any FHIR Resource.
type Resource interface {
	Element
	ResourceType() string
	ResourceId() (string, bool)
}

// CapabilityStatement is the interface type for a FHIR CapabilityStatement resource.
// It is satisfied by any release-specific CapabilityStatement type.
type CapabilityStatement interface {
	Resource
}

// Parameters is the interface type for a FHIR Parameters resource.
// It is satisfied by any release-specific Parameters type.
type Parameters interface {
	Resource
}
