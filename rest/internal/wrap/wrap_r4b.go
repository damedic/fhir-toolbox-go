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
		c, ok := api.(capabilities.ConcreteCapabilities[r4b.CapabilityStatement])
		if !ok {
			return nil, fmt.Errorf("backend does not implement capabilities.ConcreteCapabilities for R4B")
		}
		return capabilitiesR4B.Generic{Concrete: c}, nil
	}
}
