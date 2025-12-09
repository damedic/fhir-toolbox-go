package generate

import (
	"github.com/damedic/fhir-toolbox-go/internal/generate/ir"
	. "github.com/dave/jennifer/jen"
)

type OperationOutcomeErrorGenerator struct {
	NoOpGenerator
}

func (g OperationOutcomeErrorGenerator) GenerateType(f *File, rt ir.ResourceOrType) bool {
	if rt.Name == "OperationOutcome" {
		implementErrorForOperationOutcome(f)
		return true
	}
	return false
}

func implementErrorForOperationOutcome(f *File) {
	f.Func().
		Params(Id("o").Id("OperationOutcome")).
		Id("Error").
		Params().
		String().
		Block(
			Return(Id("o").Dot("String").Call()),
		)
}
