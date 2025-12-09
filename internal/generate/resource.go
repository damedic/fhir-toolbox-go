package generate

import (
	"github.com/damedic/fhir-toolbox-go/internal/generate/ir"
	. "github.com/dave/jennifer/jen"
)

type ImplResourceGenerator struct {
	NoOpGenerator
}

func (g ImplResourceGenerator) GenerateType(f *File, rt ir.ResourceOrType) bool {
	if rt.IsResource {
		f.Func().Params(Id("r").Id(rt.Name)).Id("ResourceType").Params().String().Block(
			Return(Lit(rt.Name)),
		)
		f.Func().Params(Id("r").Id(rt.Name)).Id("ResourceId").Params().Params(String(), Bool()).Block(
			If(Id("r.Id").Op("==").Nil()).Block(
				Return(Lit(""), False()),
			),
			If(Id("r.Id.Value").Op("==").Nil()).Block(
				Return(Lit(""), False()),
			),
			Return(Id("*r.Id.Value"), True()),
		)
	}

	return true
}
