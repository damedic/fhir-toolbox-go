package wrap

import (
	"fmt"
	"strings"

	"github.com/damedic/fhir-toolbox-go/capabilities"
	"github.com/damedic/fhir-toolbox-go/model"
)

var genericR4 = func(api any) (capabilities.GenericCapabilities, error) { return nil, disabledErr[model.R4]() }
var genericR4B = func(api any) (capabilities.GenericCapabilities, error) { return nil, disabledErr[model.R4B]() }
var genericR5 = func(api any) (capabilities.GenericCapabilities, error) { return nil, disabledErr[model.R5]() }

func disabledErr[R model.Release]() error {
	r := model.ReleaseName[R]()

	return fmt.Errorf(
		"release %s disabled by build tag; remove all build tags or add %s",
		r, strings.ToLower(r),
	)
}

func Generic[R model.Release](api any) (capabilities.GenericCapabilities, error) {
	// we assume already generic, do not wrap it again
	generic, ok := api.(capabilities.GenericCapabilities)
	if ok {
		return generic, nil
	}

	var r R
	switch any(r).(type) {
	case model.R4:
		return genericR4(api)
	case model.R4B:
		return genericR4B(api)
	case model.R5:
		return genericR5(api)
	default:
		// This should never happen as long as we control all implementations of the Release interface.
		// This is achieved by sealing the interface. See the interface definition for more information.
		panic("unsupported release")
	}
}
