//go:Build r5 || !(r4 || r4b || r5)

package outcome

import (
	"github.com/damedic/fhir-toolbox-go/model"
	r5 "github.com/damedic/fhir-toolbox-go/model/gen/r5"
	"github.com/damedic/fhir-toolbox-go/utils/ptr"
)

func init() {
	buildR5 = func(severity, code, diagnostics string) (model.Resource, error) {
		return r5.OperationOutcome{
			Issue: []r5.OperationOutcomeIssue{{
				Severity:    r5.Code{Value: ptr.To(severity)},
				Code:        r5.Code{Value: ptr.To(code)},
				Diagnostics: &r5.String{Value: ptr.To(diagnostics)},
			}},
		}, nil
	}
}
