package update

import "github.com/damedic/fhir-toolbox-go/model"

// Capabilities describe what update interactions the backend supports.
type Capabilities struct {
	// UpdateCreate indicates whether update operations can create new resources.
	UpdateCreate bool
}

// Result of an update operation.
//
// It contains the updated resource and a boolean indicating whether the resource was created or updated.
type Result[R model.Resource] struct {
	Resource R
	// Created indicates whether the resource was newly created (true) or an existing resource was updated (false).
	Created bool
}
