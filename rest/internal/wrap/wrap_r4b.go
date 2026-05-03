//go:build r4b || !(r4 || r4b || r5)

package wrap

import (
	"fmt"

	"github.com/damedic/fhir-toolbox-go/capabilities"
	capabilitiesR4B "github.com/damedic/fhir-toolbox-go/capabilities/gen/r4b"
	r4b "github.com/damedic/fhir-toolbox-go/model/gen/r4b"
)

func init() {
	genericR4B = func(api any) (capabilities.GenericCapabilities, error) {
		switch api.(type) {
		case capabilities.ConcreteCapabilities[r4b.CapabilityStatement], capabilities.GenericCapabilities:
			// Always wrap so that concrete method overrides on the backend
			// are detected and given precedence over generic fallbacks.
			return capabilitiesR4B.Generic{Concrete: api}, nil
		default:
			return nil, fmt.Errorf("backend does not implement capabilities.GenericCapabilities or capabilities.ConcreteCapabilities for R4B")
		}
	}
}
