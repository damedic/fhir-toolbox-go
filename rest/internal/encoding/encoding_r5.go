//go:build r5 || !(r4 || r4b || r5)

package encoding

import (
	"io"

	"github.com/damedic/fhir-toolbox-go/model/gen/r5"

	"github.com/damedic/fhir-toolbox-go/model"
)

func init() {
	decodeR5Resource = func(r io.Reader, format Format) (model.Resource, error) {
		contained, err := decode[model.R5, r5.ContainedResource](r, format)
		return contained.Resource, err
	}
}
