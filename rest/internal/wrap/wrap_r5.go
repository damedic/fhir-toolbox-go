//go:build r5 || !(r4 || r4b || r5)

package wrap

import (
	"fmt"

	"github.com/damedic/fhir-toolbox-go/capabilities"
	capabilitiesR5 "github.com/damedic/fhir-toolbox-go/capabilities/gen/r5"
	r5 "github.com/damedic/fhir-toolbox-go/model/gen/r5"
)

func init() {
	genericR5 = func(api any) (capabilities.GenericCapabilities, error) {
		switch api.(type) {
		case capabilities.ConcreteCapabilities[r5.CapabilityStatement], capabilities.GenericCapabilities:
			// Always wrap so that concrete method overrides on the backend
			// are detected and given precedence over generic fallbacks.
			return capabilitiesR5.Generic{Concrete: api}, nil
		default:
			return nil, fmt.Errorf("backend does not implement capabilities.GenericCapabilities or capabilities.ConcreteCapabilities for R5")
		}
	}
}
