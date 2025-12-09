package capabilities

import (
	"context"

	"github.com/damedic/fhir-toolbox-go/capabilities/search"
	"github.com/damedic/fhir-toolbox-go/capabilities/update"
	"github.com/damedic/fhir-toolbox-go/model"
)

// The GenericCapabilities interface provides a generic capabilities method that returns a CapabilityStatement of the underlying concrete implementation.
type GenericCapabilities interface {
	CapabilityStatement(ctx context.Context) (model.CapabilityStatement, error)
}

// The GenericCreate interface provides a generic create capability.
//
// The persisted resource is returned.
type GenericCreate interface {
	Create(ctx context.Context, resource model.Resource) (model.Resource, error)
}

// The GenericRead interface provides a generic read capability by passing the `resourceType` as string.
type GenericRead interface {
	Read(ctx context.Context, resourceType, id string) (model.Resource, error)
}

// The GenericUpdate interface provides a generic update capability.
//
// The persisted resource is returned.
type GenericUpdate interface {
	Update(ctx context.Context, resource model.Resource) (update.Result[model.Resource], error)
}

// The GenericDelete interface provides a generic deletion capability by passing the `resourceType` as string.
type GenericDelete interface {
	Delete(ctx context.Context, resourceType, id string) error
}

// The GenericSearch interface provides a generic search capability by passing the `resourceType` as string.
type GenericSearch interface {
	// GenericCapabilities is required because it includes the search capabilities (parameters etc.).
	GenericCapabilities
	Search(ctx context.Context, resourceType string, parameters search.Parameters, options search.Options) (search.Result[model.Resource], error)
}

// The GenericOperation interface provides a unified way to invoke FHIR operations
// at system, type, or instance level. The `resourceType` and `id` may be empty
// depending on the invocation level.
//
// - System-level:    resourceType = "", id = ""
// - Type-level:      resourceType = <type>, id = ""
// - Instance-level:  resourceType = <type>, id = <id>
//
// The `code` is the operation name without the leading '$'.
// The `parameters` contains the input Parameters resource.
type GenericOperation interface {
	// GenericCapabilities is required because it references OperationDefinition resources.
	GenericCapabilities
	Invoke(ctx context.Context, resourceType, resourceID, code string, parameters model.Parameters) (model.Resource, error)
}
