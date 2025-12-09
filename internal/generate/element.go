package generate

import (
	"github.com/damedic/fhir-toolbox-go/internal/generate/ir"
	. "github.com/dave/jennifer/jen"
	"slices"
)

type ImplElementGenerator struct {
	NoOpGenerator
}

func (g ImplElementGenerator) GenerateType(f *File, rt ir.ResourceOrType) bool {
	for _, t := range rt.Structs {
		implementMemSize(f, t)
	}

	return true
}

func implementMemSize(f *File, s ir.Struct) {
	f.Func().Params(Id("r").Id(s.Name)).Id("MemSize").Params().Params(Int()).BlockFunc(func(g *Group) {
		if s.IsDomainResource {
			g.Var().Id("emptyIface").Any()
		}

		g.Id("s").Op(":=").Add(size(Id("r")))

		for _, f := range s.Fields {
			t := f.PossibleTypes[0]

			if f.Multiple {
				g.For(List(Id("_"), Id("i")).Op(":=").Range().Id("r").Dot(f.Name)).Block(
					Id("s").Op("+=").Id("i").Dot("MemSize").Call(),
				)
				if t.IsNestedResource {
					g.Id("s").Op("+=").Parens(
						Cap(Id("r").Dot(f.Name)).Op("-").Len(Id("r").Dot(f.Name)),
					).Op("*").Add(
						Int().Call(Qual("reflect", "TypeOf").Call(Id("&emptyIface")).Dot("Elem").Call().Dot("Size").Call()),
					)
				} else {
					g.Id("s").Op("+=").Parens(
						Cap(Id("r").Dot(f.Name)).Op("-").Len(Id("r").Dot(f.Name)),
					).Op("*").Add(size(Id(t.Name).Values()))
				}
			} else if f.Optional || f.Polymorph {
				if s.Name == "Decimal" && f.Name == "Value" {
					g.If(Id("r").Dot(f.Name)).Op("!=").Nil().Block(
						Id("s").Op("+=").Int().Call(Id("r").Dot(f.Name).Dot("Size").Call()),
					)
				} else if t.Name == "string" {
					g.If(Id("r").Dot(f.Name)).Op("!=").Nil().Block(
						Id("s").Op("+=").Len(Op("*").Id("r").Dot(f.Name)).Op("+").Add(size(Op("*").Id("r").Dot(f.Name))),
					)
				} else if slices.Contains([]string{"bool", "int32", "int64", "uint32"}, t.Name) {
					g.If(Id("r").Dot(f.Name).Op("!=").Nil()).Block(
						Id("s").Op("+=").Add(size(Op("*").Id("r").Dot(f.Name))),
					)
				} else if t.IsNestedResource {
					g.If(Id("r").Dot(f.Name).Op("!=").Nil()).Block(
						Id("s").Op("+=").Parens(Id("r").Dot(f.Name)).Dot("MemSize").Call(),
					)
				} else {
					g.If(Id("r").Dot(f.Name).Op("!=").Nil()).Block(
						Id("s").Op("+=").Id("r").Dot(f.Name).Dot("MemSize").Call(),
					)
				}
			} else if (s.Name == "Extension" && f.Name == "Url") ||
				(s.Name == "Xhtml" && f.Name == "Value") {
				g.Id("s").Op("+=").Len(Id("r").Dot(f.Name))
			} else {
				g.Id("s").Op("+=").Id("r").Dot(f.Name).Dot("MemSize").Call().Op("-").Add(size(Id("r").Dot(f.Name)))
			}
		}

		g.Return(Id("s"))
	})
}

func size(s *Statement) *Statement {
	return Int().Call(Qual("reflect", "TypeOf").Call(s).Dot("Size").Call())
}
