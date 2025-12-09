//go:Build r4b || !(r4 || r4b || r5)

package outcome

import (
	"github.com/damedic/fhir-toolbox-go/model"
	r4b "github.com/damedic/fhir-toolbox-go/model/gen/r4b"
	"github.com/damedic/fhir-toolbox-go/utils/ptr"
)

func init() {
	buildR4B = func(severity, code, diagnostics string) (model.Resource, error) {
		return r4b.OperationOutcome{
			Issue: []r4b.OperationOutcomeIssue{{
				Severity:    r4b.Code{Value: ptr.To(severity)},
				Code:        r4b.Code{Value: ptr.To(code)},
				Diagnostics: &r4b.String{Value: ptr.To(diagnostics)},
			}},
		}, nil
	}
}
