//go:build r4 || !(r4 || r4b || r5)

package wrap

import (
	"fmt"

	"github.com/damedic/fhir-toolbox-go/capabilities"
	capabilitiesR4 "github.com/damedic/fhir-toolbox-go/capabilities/gen/r4"
	r4 "github.com/damedic/fhir-toolbox-go/model/gen/r4"
)

func init() {
	genericR4 = func(api any) (capabilities.GenericCapabilities, error) {
		switch api.(type) {
		case capabilities.ConcreteCapabilities[r4.CapabilityStatement], capabilities.GenericCapabilities:
			// Always wrap so that concrete method overrides on the backend
			// are detected and given precedence over generic fallbacks.
			return capabilitiesR4.Generic{Concrete: api}, nil
		default:
			return nil, fmt.Errorf("backend does not implement capabilities.GenericCapabilities or capabilities.ConcreteCapabilities for R4")
		}
	}
}
