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
		c, ok := api.(capabilities.ConcreteCapabilities[r4.CapabilityStatement])
		if !ok {
			return nil, fmt.Errorf("backend does not implement capabilities.ConcreteCapabilities for R4")
		}
		return capabilitiesR4.Generic{Concrete: c}, nil
	}
}
