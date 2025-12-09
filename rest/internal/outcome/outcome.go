package outcome

import (
	"fmt"

	"github.com/damedic/fhir-toolbox-go/model"
)

// release-specific builders; replaced via Build-tagged files
var buildR4 = func(severity, code, diagnostics string) (model.Resource, error) { return nil, disabledErr[model.R4]() }
var buildR4B = func(severity, code, diagnostics string) (model.Resource, error) { return nil, disabledErr[model.R4B]() }
var buildR5 = func(severity, code, diagnostics string) (model.Resource, error) { return nil, disabledErr[model.R5]() }

// Build constructs a release-specific OperationOutcome resource with a single issue.
func Build[R model.Release](severity, code, diagnostics string) (model.Resource, error) {
	var rel R
	switch any(rel).(type) {
	case model.R4:
		return buildR4(severity, code, diagnostics)
	case model.R4B:
		return buildR4B(severity, code, diagnostics)
	case model.R5:
		return buildR5(severity, code, diagnostics)
	default:
		panic("unsupported release")
	}
}

// Error creates an error value backed by a release-specific OperationOutcome when available.
func Error[R model.Release](severity, code, diagnostics string) error {
	res, err := Build[R](severity, code, diagnostics)
	if err != nil {
		return fmt.Errorf("%s: %s", code, diagnostics)
	}
	if e, ok := res.(error); ok {
		return e
	}
	return fmt.Errorf("%s: %s", code, diagnostics)
}

func disabledErr[R model.Release]() error {
	r := model.ReleaseName[R]()
	return fmt.Errorf("release %s disabled by Build tag; remove all Build tags or add %s", r, r)
}
