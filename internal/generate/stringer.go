package generate

import (
	"github.com/damedic/fhir-toolbox-go/internal/generate/ir"
	. "github.com/dave/jennifer/jen"
	"strings"
)

type StringerGenerator struct {
	NoOpGenerator
}

func (g StringerGenerator) GenerateType(f *File, rt ir.ResourceOrType) bool {
	for _, s := range rt.Structs {
		f.Func().Params(Id("r").Id(s.Name)).Id("String").Params().String().Block(
			List(Id("buf"), Id("err")).Op(":=").Qual("encoding/json", "MarshalIndent").Params(Id("r"), Lit(""), Lit("  ")),
			If(Id("err").Op("!=").Nil()).Block(
				Return(Lit("null")),
			),
			Return(Id("string").Params(Id("buf"))),
		)
	}

	return true
}

func (g StringerGenerator) GenerateAdditional(f func(fileName string, pkgName string) *File, release string, rt []ir.ResourceOrType) {
	if release != "basic" {
		implementStringerForContained(f("contained_resource", strings.ToLower(release)))
	}
}

func implementStringerForContained(f *File) {
	f.Func().Params(Id("r").Id("ContainedResource")).Id("String").Params().String().Block(
		List(Id("buf"), Id("err")).Op(":=").Qual("encoding/json", "MarshalIndent").Params(Id("r"), Lit(""), Lit("  ")),
		If(Id("err").Op("!=").Nil()).Block(
			Return(Lit("null")),
		),
		Return(Id("string").Params(Id("buf"))),
	)
}
