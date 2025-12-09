//go:Build r4 || !(r4 || r4b || r5)

package outcome

import (
	"github.com/damedic/fhir-toolbox-go/model"
	r4 "github.com/damedic/fhir-toolbox-go/model/gen/r4"
	"github.com/damedic/fhir-toolbox-go/utils/ptr"
)

func init() {
	buildR4 = func(severity, code, diagnostics string) (model.Resource, error) {
		return r4.OperationOutcome{
			Issue: []r4.OperationOutcomeIssue{{
				Severity:    r4.Code{Value: ptr.To(severity)},
				Code:        r4.Code{Value: ptr.To(code)},
				Diagnostics: &r4.String{Value: ptr.To(diagnostics)},
			}},
		}, nil
	}
}
