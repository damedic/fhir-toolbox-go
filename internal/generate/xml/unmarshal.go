package xml

import (
	"github.com/damedic/fhir-toolbox-go/internal/generate"
	"github.com/damedic/fhir-toolbox-go/internal/generate/ir"
	. "github.com/dave/jennifer/jen"
	"strings"
)

type UnmarshalGenerator struct {
	generate.NoOpGenerator
	NotUseContainedResource bool
}

func (g UnmarshalGenerator) GenerateType(f *File, rt ir.ResourceOrType) bool {
	for _, t := range rt.Structs {
		g.implementUnmarshal(f, t)
	}

	return true
}

func (g UnmarshalGenerator) GenerateAdditional(f func(fileName string, pkgName string) *File, release string, rt []ir.ResourceOrType) {
	if !g.NotUseContainedResource {
		implementUnmarshalContained(f("contained_resource", strings.ToLower(release)), ir.FilterResources(rt))
	}
}

func (gen UnmarshalGenerator) implementUnmarshal(f *File, s ir.Struct) {
	f.Func().Params(Id("r").Op("*").Id(s.Name)).Id("UnmarshalXML").Params(
		Id("d").Op("*").Qual("encoding/xml", "Decoder"),
		Id("start").Qual("encoding/xml", "StartElement"),
	).Params(Error()).BlockFunc(func(g *Group) {
		validateStructNamespace(g, s)
		unmarshalAttrs(g, s)

		if s.Name == "Xhtml" {
			unmarshalXhtml(g)
		} else {
			gen.unmarshalFields(g, s)
		}
	})
}

func expectedNamespace(s ir.Struct) string {
	if s.Name == "Xhtml" {
		return NamespaceXHTML
	} else {
		return NamespaceFHIR
	}
}

func validateStructNamespace(g *Group, s ir.Struct) {
	expectedNamespace := expectedNamespace(s)

	g.If(Id("start.Name.Space").Op("!=").Lit(expectedNamespace)).Block(
		Return(Qual("fmt", "Errorf").Params(
			Lit("invalid namespace: \"%s\", expected: \""+expectedNamespace+"\""),
			Id("start.Name.Space"),
		)),
	)
}

func validateAttrNamespace(g *Group) *Statement {
	return g.If(Id("a.Name.Space").Op("!=").Lit("")).Block(
		Return(Qual("fmt", "Errorf").Params(
			Lit("invalid attribute namespace: \"%s\", expected default namespace"),
			Id("start.Name.Space"),
		)),
	)
}

func unmarshalAttrs(g *Group, s ir.Struct) {
	g.For(List(Id("_"), Id("a")).Op(":=").Range().Id("start.Attr")).BlockFunc(func(g *Group) {
		validateAttrNamespace(g)

		g.Switch(Id("a.Name.Local")).BlockFunc(func(g *Group) {
			g.Case(Lit("xmlns")).Block(
				// namespace was validated earlier
				Continue(),
			)
			if !s.IsResource {
				g.Case(Lit("id")).Block(
					Id("r.Id").Op("=").Id("&a.Value"),
				)
			}
			if s.IsPrimitive && s.Name != "Xhtml" {
				unmarshalPrimitiveValueAttr(g, s)
			}
			if s.Name == "Extension" {
				g.Case(Lit("url")).Block(
					Id("r.Url").Op("=").Id("a.Value"),
				)
			}
			g.Default().Block(Return(Qual("fmt", "Errorf").Params(
				Lit("invalid attribute: \"%s\""),
				Id("a.Name.Local"),
			)))
		})
	})
}

func unmarshalPrimitiveValueAttr(g *Group, s ir.Struct) {
	g.Case(Lit("value")).BlockFunc(func(g *Group) {
		if s.Name == "Boolean" {
			g.Var().Id("v").Bool()
			g.If(Id("a.Value").Op("==").Lit("true")).Block(
				Id("v").Op("=").True(),
			).Else().If(Id("a.Value").Op("==").Lit("false")).Block(
				Id("v").Op("=").False(),
			).Else().Block(
				Return(Qual("fmt", "Errorf").Params(
					Lit("can not parse \"%s\" as bool"),
					Id("a.Value"),
				)),
			)
			g.Id("r.Value").Op("=").Id("&v")
		} else if s.Name == "Decimal" {
			g.List(Id("d"), Id("_"), Id("err")).Op(":=").Qual("github.com/cockroachdb/apd/v3", "NewFromString").Call(Id("a.Value"))
			g.If(Err().Op("!=").Nil()).Block(
				Return(Id("err")),
			)
			g.Id("r.Value").Op("=").Id("d")
		} else if s.Name == "Integer" {
			g.List(Id("i"), Id("err")).Op(":=").Qual("strconv", "ParseInt").Call(Id("a.Value"), Lit(10), Lit(0))
			g.If(Err().Op("!=").Nil()).Block(
				Return(Id("err")),
			)
			g.Id("v").Op(":=").Id("int32").Call(Id("i"))
			g.Id("r.Value").Op("=").Id("&v")
		} else if s.Name == "Integer64" {
			g.List(Id("i"), Id("err")).Op(":=").Qual("strconv", "ParseInt").Call(Id("a.Value"), Lit(10), Lit(0))
			g.If(Err().Op("!=").Nil()).Block(
				Return(Id("err")),
			)
			g.Id("v").Op(":=").Id("i")
			g.Id("r.Value").Op("=").Id("&v")
		} else if s.Name == "PositiveInt" || s.Name == "UnsignedInt" {
			g.List(Id("i"), Id("err")).Op(":=").Qual("strconv", "ParseInt").Call(Id("a.Value"), Lit(10), Lit(0))
			g.If(Err().Op("!=").Nil()).Block(
				Return(Id("err")),
			)
			g.Id("v").Op(":=").Id("uint32").Call(Id("i"))
			g.Id("r.Value").Op("=").Id("&v")
		} else {
			g.Id("r.Value").Op("=").Id("&a.Value")
		}
	})
}

func (gen UnmarshalGenerator) unmarshalFields(g *Group, s ir.Struct) {
	g.For().Block(
		List(Id("token"), Id("err")).Op(":=").Id("d.Token").Params(),
		If(Err().Op("!=").Nil()).Block(
			Return(Id("err")),
		),
		Switch(Id("t").Op(":=").Id("token.(type)")).Block(
			Case(Qual("encoding/xml", "StartElement")).BlockFunc(func(g *Group) {
				g.Switch(Id("t.Name.Local")).BlockFunc(func(g *Group) {
					for _, f := range s.Fields {
						if !s.IsResource && f.Name == "Id" {
							continue
						}
						if s.IsPrimitive && f.Name == "Value" {
							continue
						}
						if s.Name == "Extension" && f.Name == "Url" {
							continue
						}

						gen.unmarshalField(g, f)
					}
				})
			}),
			Case(Qual("encoding/xml", "EndElement")).Block(Return(Nil())),
		),
	)
}

func (gen UnmarshalGenerator) unmarshalField(g *Group, f ir.StructField) {
	if f.Polymorph {
		for _, t := range f.PossibleTypes {
			g.Case(Lit(f.MarshalName+t.Name)).Block(
				If(Id("r."+f.Name).Op("!=").Nil()).Block(
					Return(Qual("fmt", "Errorf").Params(Lit(`multiple values for field "`+f.Name+`"`))),
				),
				Var().Id("v").Id(t.Name),
				Id("err").Op(":=").Id("d.DecodeElement").Call(Op("&").Id("v"), Id("&t")),
				If(Err().Op("!=").Nil()).Block(
					Return(Id("err")),
				),
				Id("r."+f.Name).Op("=").Id("v"),
			)
		}
	} else {
		t := f.PossibleTypes[0]

		g.Case(Lit(f.MarshalName)).BlockFunc(func(g *Group) {
			if t.IsNestedResource && gen.NotUseContainedResource {
				// For basic types, decode as generic interface and encode to XML string
				g.Var().Id("elem").Interface()
				g.Id("err").Op(":=").Id("d.DecodeElement").Call(Op("&").Id("elem"), Id("&t"))
				g.If(Err().Op("!=").Nil()).Block(
					Return(Id("err")),
				)
				g.Var().Id("raw").Qual("bytes", "Buffer")
				g.Id("err").Op("=").Qual("encoding/xml", "NewEncoder").Call(Op("&").Id("raw")).Dot("Encode").Call(Id("elem"))
				g.If(Err().Op("!=").Nil()).Block(
					Return(Id("err")),
				)
				g.Id("v").Op(":=").Id("RawResource").Values(Dict{
					Id("Content"): Id("raw.String").Call(),
					Id("IsJSON"):  False(),
					Id("IsXML"):   True(),
				})
				if f.Multiple {
					g.Id("r."+f.Name).Op("=").Append(Id("r."+f.Name), Id("v"))
				} else {
					g.Id("r." + f.Name).Op("=").Id("v")
				}
			} else if t.IsNestedResource {
				g.Var().Id("c").Id("ContainedResource")
				g.Id("err").Op(":=").Id("d.DecodeElement").Call(Op("&").Id("c"), Id("&t"))
				g.If(Err().Op("!=").Nil()).Block(
					Return(Id("err")),
				)
				if f.Multiple {
					g.Id("r."+f.Name).Op("=").Append(Id("r."+f.Name), Id("c.Resource"))
				} else {
					g.Id("r." + f.Name).Op("=").Id("c.Resource")
				}
			} else {
				g.Var().Id("v").Id(t.Name)

				g.Id("err").Op(":=").Id("d.DecodeElement").Call(Id("&v"), Id("&t"))
				g.If(Err().Op("!=").Nil()).Block(
					Return(Id("err")),
				)

				if f.Multiple {
					g.Id("r."+f.Name).Op("=").Append(Id("r."+f.Name), Id("v"))
				} else if f.Optional {
					g.Id("r." + f.Name).Op("=").Id("&v")
				} else {
					g.Id("r." + f.Name).Op("=").Id("v")
				}
			}
		})
	}
}

func unmarshalXhtml(g *Group) {
	g.Var().Id("v").Struct(
		Id("V").String().Tag(map[string]string{"xml": ",innerxml"}),
	)
	g.Id("err").Op(":=").Id("d.DecodeElement").Call(Id("&v"), Id("&start"))
	g.If(Err().Op("!=").Nil()).Block(
		Return(Id("err")),
	)
	g.Id("r.Value").Op("=").Id("v.V")
	g.Return(Nil())
}

func implementUnmarshalContained(f *File, resources []ir.ResourceOrType) {
	f.Func().Params(Id("cr").Op("*").Id("ContainedResource")).Id("UnmarshalXML").Params(
		Id("d").Op("*").Qual("encoding/xml", "Decoder"),
		Id("start").Qual("encoding/xml", "StartElement"),
	).Params(Error()).Block(
		// if name is lower means we are dealing with a contained resource
		If(Qual("unicode", "IsLower").Call(Rune().Call(Id("start.Name.Local").Index(Lit(0))))).Block(
			Err().Op(":=").Id("d.Decode").Call(Op("cr")),
			If(Id("err").Op("!=").Nil()).Block(
				Return(Id("err")),
			),
			For().Block(
				Id("t, err").Op(":=").Id("d.Token()"),
				If(Err().Op("!=").Nil()).Block(
					Return(Id("err")),
				),
				Id("_, ok").Op(":=").Id("t.").Params(Qual("encoding/xml", "EndElement")),
				If(Id("ok")).Block(Break()),
			),
			Return(Nil()),
		),

		If(Id("start.Name.Space").Op("!=").Lit(NamespaceFHIR)).Block(
			Return(Qual("fmt", "Errorf").Params(
				Lit("invalid namespace: \"%s\", expected: \""+NamespaceFHIR+"\""),
				Id("start.Name.Space"),
			)),
		),

		Switch(Id("start.Name.Local")).BlockFunc(func(g *Group) {
			for _, r := range resources {
				g.Case(Lit(r.Name)).Block(
					Var().Id("r").Id(r.Name),
					Id("err").Op(":=").Id("d.DecodeElement").Call(Op("&r"), Id("&start")),
					If(Id("err").Op("!=").Nil()).Block(
						Return(Id("err")),
					),
					Op("*").Id("cr").Op("=").Id("ContainedResource").Values(Id("r")),
					Return(Nil()),
				)
			}

			g.Default().Block(
				Return(Qual("fmt", "Errorf").Call(Lit("unknown resource type: %s"), Id("start.Name.Local"))),
			)
		}),
	)
}
