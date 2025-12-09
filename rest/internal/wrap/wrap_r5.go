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
		c, ok := api.(capabilities.ConcreteCapabilities[r5.CapabilityStatement])
		if !ok {
			return nil, fmt.Errorf("backend does not implement capabilities.ConcreteCapabilities for R5")
		}
		return capabilitiesR5.Generic{Concrete: c}, nil
	}
}
