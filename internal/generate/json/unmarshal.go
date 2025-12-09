package json

import (
	"github.com/damedic/fhir-toolbox-go/internal/generate/ir"
	. "github.com/dave/jennifer/jen"
	"strings"
)

type UnmarshalGenerator struct {
	NotUseContainedResource bool
}

func (g UnmarshalGenerator) GenerateType(f *File, rt ir.ResourceOrType) bool {
	for _, t := range rt.Structs {
		if t.IsResource {
			implementUnmarshalExternal(f, t)
		}

		if t.IsPrimitive {
			implementUnmarshalPrimitive(f, t)
		} else {
			g.implementUnmarshalInternal(f, t)
		}
	}

	return true
}

func (g UnmarshalGenerator) GenerateAdditional(f func(fileName string, pkgName string) *File, release string, rt []ir.ResourceOrType) {
	if !g.NotUseContainedResource {
		implementUnmarshalContainedExternal(f("contained_resource", strings.ToLower(release)))
		implementUnmarshalContainedInternal(f("contained_resource", strings.ToLower(release)), ir.FilterResources(rt))
	}
	implementUnmarshalPrimitiveElement(f("json_primitive_element", strings.ToLower(release)))
}

func implementUnmarshalExternal(f *File, s ir.Struct) {
	f.Func().Params(Id("r").Op("*").Id(s.Name)).Id("UnmarshalJSON").Params(
		Id("b").Index().Byte(),
	).Params(Error()).Block(
		Id("d").Op(":=").Qual("encoding/json", "NewDecoder").Call(
			Qual("bytes", "NewReader").Call(Id("b")),
		),
		Return(Id("r").Dot("unmarshalJSON").Call(Id("d"))),
	)
}

func implementUnmarshalPrimitive(f *File, s ir.Struct) {
	var ty *Statement
	if s.Name == "Integer64" {
		ty = Id("string")
	} else if s.Name == "Decimal" {
		ty = Qual("github.com/cockroachdb/apd/v3", "Decimal")
	} else {
		for _, field := range s.Fields {
			if field.Name == "Value" {
				ty = Id(field.PossibleTypes[0].Name)
				break
			}
		}
	}

	var unmarshal Code
	if s.Name == "Decimal" {
		unmarshal = If(
			Id("err").Op(":=").Id("v").Dot("UnmarshalText").Call(Id("b")),
			Id("err").Op("!=").Nil()).Block(
			Return(Id("err")),
		)
	} else {
		unmarshal = If(
			Id("err").Op(":=").Qual("encoding/json", "Unmarshal").Params(Id("b"), Op("&").Id("v")),
			Id("err").Op("!=").Nil()).Block(
			Return(Id("err")),
		)
	}

	var assign Code
	if s.Name == "Xhtml" {
		assign = Id("*r").Op("=").Id(s.Name).Values(Id("Value").Op(":").Id("v"))
	} else if s.Name == "Integer64" {
		assign = Block(
			List(Id("i"), Id("err")).Op(":=").Qual("strconv", "ParseInt").Call(Id("v"), Lit(10), Lit(0)),
			If(Err().Op("!=").Nil()).Block(Return(Id("err"))),
			Id("*r").Op("=").Id(s.Name).Values(Id("Value").Op(":").Id("&i")),
		)
	} else {
		assign = Id("*r").Op("=").Id(s.Name).Values(Id("Value").Op(":").Id("&v"))
	}

	f.Func().Params(
		Id("r").Op("*").Id(s.Name),
	).Id("UnmarshalJSON").Params(
		Id("b").Index().Byte(),
	).Params(Error()).Block(
		// Check if JSON is null - if so, create empty primitive (no Value)
		If(String().Call(Id("b")).Op("==").Lit("null")).Block(
			Id("*r").Op("=").Id(s.Name).Values(),
			Return().Nil(),
		),
		Var().Id("v").Add(ty),
		unmarshal,
		assign,
		Return().Nil(),
	)
}

func (gen UnmarshalGenerator) implementUnmarshalInternal(f *File, s ir.Struct) {
	f.Func().Params(Id("r").Op("*").Id(s.Name)).Id("unmarshalJSON").Params(
		Id("d").Op("*").Qual("encoding/json", "Decoder"),
	).Params(Error()).BlockFunc(func(g *Group) {
		g.List(Id("t"), Err()).Op(":=").Id("d").Dot("Token").Call()
		g.If(Err().Op("!=").Nil()).Block(
			Return(Err()),
		)
		g.If(Id("t").Op("!=").Qual("encoding/json", "Delim")).Params(LitRune('{')).Block(
			returnInvalidTokenError(s.Name, "'{'"),
		)

		gen.unmarshalFields(g, s)

		g.List(Id("t"), Err()).Op("=").Id("d").Dot("Token").Call()
		g.If(Err().Op("!=").Nil()).Block(
			Return(Err()),
		)
		g.If(Id("t").Op("!=").Qual("encoding/json", "Delim")).Params(LitRune('}')).Block(
			returnInvalidTokenError(s.Name, "'}'"),
		)
		g.Return(Nil())
	})
}

func (gen UnmarshalGenerator) unmarshalFields(g *Group, s ir.Struct) {
	g.For(Id("d").Dot("More").Call()).Block(
		List(Id("t"), Err()).Op("=").Id("d").Dot("Token").Call(),
		If(Err().Op("!=").Nil()).Block(
			Return(Err()),
		),
		List(Id("f"), Id("ok")).Op(":=").Id("t").Op(".").Call(String()),
		If(Op("!").Id("ok")).Block(
			returnInvalidTokenError(s.Name, "field name"),
		),
		Switch(Id("f")).BlockFunc(func(g *Group) {
			if s.IsResource {
				g.Case(Lit("resourceType")).Block(
					List(Id("_"), Id("err")).Op(":=").Id("d").Dot("Token").Call(),
					If(Err().Op("!=").Nil()).Block(
						Return(Id("err")),
					),
				)
			}

			for _, f := range s.Fields {
				gen.unmarshalField(g, s, f)
			}

			g.Default().Block(
				Return(Qual("fmt", "Errorf").Params(
					Lit("invalid field: %s in "+s.Name+""),
					Id("f"),
				)),
			)
		}),
	)
}

func (gen UnmarshalGenerator) unmarshalField(g *Group, s ir.Struct, f ir.StructField) {
	if f.Polymorph {
		gen.unmarshalCasePolymorph(g, s, f)
	} else {
		gen.unmarshalCaseNonPolymorph(g, s, f)
	}
}

func (gen UnmarshalGenerator) unmarshalCasePolymorph(g *Group, s ir.Struct, f ir.StructField) {
	for _, t := range f.PossibleTypes {
		g.Case(Lit(f.MarshalName + t.Name)).BlockFunc(func(g *Group) {
			gen.unmarshalToValue(g, s, f, t)

			if t.IsPrimitive {
				g.If(Id("r." + f.Name).Op("!=").Nil()).Block(
					Id("r." + f.Name).Op("=").Id(t.Name).Values(Dict{
						Id("Id"):        Id("r." + f.Name).Dot("").Call(Id(t.Name)).Dot("Id"),
						Id("Extension"): Id("r." + f.Name).Dot("").Call(Id(t.Name)).Dot("Extension"),
						Id("Value"):     Id("v").Dot("Value"),
					}),
				).Else().Block(
					Id("r." + f.Name).Op("=").Id("v"),
				)
			} else {
				g.Id("r." + f.Name).Op("=").Id("v")
			}
		})

		if t.IsPrimitive {
			g.Case(Lit("_" + f.MarshalName + t.Name)).BlockFunc(func(g *Group) {
				unmarshalPrimitiveElement(g)

				g.If(Id("r." + f.Name).Op("!=").Nil()).Block(
					Id("r." + f.Name).Op("=").Id(t.Name).Values(Dict{
						Id("Id"):        Id("v").Dot("Id"),
						Id("Extension"): Id("v").Dot("Extension"),
						Id("Value"):     Id("r." + f.Name).Dot("").Call(Id(t.Name)).Dot("Value"),
					}),
				).Else().Block(
					Id("r." + f.Name).Op("=").Id(t.Name).Values(Dict{
						Id("Id"):        Id("v").Dot("Id"),
						Id("Extension"): Id("v").Dot("Extension"),
					}),
				)
			})
		}
	}
}

func (gen UnmarshalGenerator) unmarshalCaseNonPolymorph(g *Group, s ir.Struct, f ir.StructField) {
	t := f.PossibleTypes[0]

	g.Case(Lit(f.MarshalName)).BlockFunc(func(g *Group) {
		if f.Multiple {
			gen.unmarshalMultiple(g, s, f, t)
		} else {
			gen.unmarshalToValue(g, s, f, t)

			if t.IsNestedResource && gen.NotUseContainedResource {
				g.Id("r." + f.Name).Op("=").Id("v")
			} else if t.IsNestedResource {
				g.Id("r." + f.Name).Op("=").Id("v").Dot("Resource")
			} else if f.Optional {
				if t.IsPrimitive {
					g.If(Id("r." + f.Name).Op("==").Nil()).Block(
						Id("r." + f.Name).Op("=").Op("&").Id(t.Name).Values(),
					)
					g.Id("r." + f.Name).Dot("Value").Op("=").Id("v.Value")
				} else {
					g.Id("r." + f.Name).Op("=").Id("&v")
				}
			} else {
				if t.IsPrimitive {
					g.Id("r." + f.Name).Dot("Value").Op("=").Id("v.Value")
				} else {
					g.Id("r." + f.Name).Op("=").Id("v")
				}
			}
		}
	})

	if t.IsPrimitive {
		g.Case(Lit("_" + f.MarshalName)).BlockFunc(func(g *Group) {
			if f.Multiple {
				unmarshalPrimitiveElementMultiple(g, s, f, t)
			} else {
				unmarshalPrimitiveElement(g)

				if f.Optional {
					g.If(Id("r." + f.Name).Op("==").Nil()).Block(
						Id("r." + f.Name).Op("=").Op("&").Id(t.Name).Values(),
					)
				}

				g.Id("r." + f.Name).Dot("Id").Op("=").Id("v.Id")
				if !(t.Name == "Xhtml" && f.Name == "Div") {
					g.Id("r." + f.Name).Dot("Extension").Op("=").Id("v.Extension")
				}
			}
		})
	}
}

func (gen UnmarshalGenerator) unmarshalMultiple(g *Group, s ir.Struct, f ir.StructField, t ir.FieldType) {
	g.List(Id("t"), Err()).Op("=").Id("d").Dot("Token").Call()
	g.If(Err().Op("!=").Nil()).Block(
		Return(Err()),
	)
	g.If(Id("t").Op("!=").Qual("encoding/json", "Delim")).Params(LitRune('[')).Block(
		returnInvalidTokenError(s.Name, "'['"),
	)

	if t.IsPrimitive {
		g.For(
			Id("i").Op(":=").Lit(0),
			Id("d").Dot("More").Call(),
			Id("i").Op("++"),
		).BlockFunc(func(g *Group) {
			gen.unmarshalToValue(g, s, f, t)

			g.For(Len(Id("r." + f.Name)).Op("<=").Id("i")).Block(
				Id("r."+f.Name).Op("=").Append(Id("r."+f.Name), Id(t.Name).Values()),
			)
			g.Id("r." + f.Name).Index(Id("i")).Dot("Value").Op("=").Id("v.Value")
		})
	} else {
		g.For(Id("d").Dot("More").Call()).BlockFunc(func(g *Group) {
			gen.unmarshalToValue(g, s, f, t)

			if t.IsNestedResource && gen.NotUseContainedResource {
				g.Id("r."+f.Name).Op("=").Append(Id("r."+f.Name), Id("v"))
			} else if t.IsNestedResource {
				g.Id("r."+f.Name).Op("=").Append(Id("r."+f.Name), Id("v").Dot("Resource"))
			} else {
				g.Id("r."+f.Name).Op("=").Append(Id("r."+f.Name), Id("v"))
			}
		})
	}

	g.List(Id("t"), Err()).Op("=").Id("d").Dot("Token").Call()
	g.If(Err().Op("!=").Nil()).Block(
		Return(Err()),
	)
	g.If(Id("t").Op("!=").Qual("encoding/json", "Delim")).Params(LitRune(']')).Block(
		returnInvalidTokenError(s.Name, "']'"),
	)
}

func (gen UnmarshalGenerator) unmarshalToValue(g *Group, s ir.Struct, f ir.StructField, t ir.FieldType) {
	if t.IsNestedResource && gen.NotUseContainedResource {
		g.Var().Id("raw").Qual("encoding/json", "RawMessage")
		g.Id("err").Op(":=").Id("d").Dot("Decode").Call(Id("&raw"))
		g.If(Err().Op("!=").Nil()).Block(
			Return(Id("err")),
		)
		g.Id("v").Op(":=").Id("RawResource").Values(Dict{
			Id("Content"): String().Call(Id("raw")),
			Id("IsJSON"):  True(),
			Id("IsXML"):   False(),
		})
	} else if t.IsNestedResource {
		g.Var().Id("v").Id("ContainedResource")
		g.Id("err").Op(":=").Id("v").Dot("unmarshalJSON").Call(Id("d"))
	} else {
		g.Var().Id("v").Id(t.Name)
		if t.IsPrimitive ||
			(!s.IsResource && f.Name == "Id") ||
			(s.Name == "Extension" && f.Name == "Url") {
			g.Id("err").Op(":=").Id("d").Dot("Decode").Call(Id("&v"))
		} else {
			g.Id("err").Op(":=").Id("v").Dot("unmarshalJSON").Call(Id("d"))
		}
	}

	g.If(Err().Op("!=").Nil()).Block(
		Return(Id("err")),
	)
}

func unmarshalPrimitiveElementMultiple(g *Group, s ir.Struct, f ir.StructField, t ir.FieldType) {
	g.List(Id("t"), Err()).Op("=").Id("d").Dot("Token").Call()
	g.If(Err().Op("!=").Nil()).Block(
		Return(Err()),
	)
	g.If(Id("t").Op("!=").Qual("encoding/json", "Delim")).Params(LitRune('[')).Block(
		returnInvalidTokenError(s.Name, "'['"),
	)

	g.For(
		Id("i").Op(":=").Lit(0),
		Id("d").Dot("More").Call(),
		Id("i").Op("++"),
	).BlockFunc(func(g *Group) {
		unmarshalPrimitiveElement(g)

		g.For(Len(Id("r." + f.Name)).Op("<=").Id("i")).Block(
			Id("r."+f.Name).Op("=").Append(Id("r."+f.Name), Id(t.Name).Values()),
		)
		g.Id("r." + f.Name).Index(Id("i")).Dot("Id").Op("=").Id("v.Id")
		g.Id("r." + f.Name).Index(Id("i")).Dot("Extension").Op("=").Id("v.Extension")
	})

	g.List(Id("t"), Err()).Op("=").Id("d").Dot("Token").Call()
	g.If(Err().Op("!=").Nil()).Block(
		Return(Err()),
	)
	g.If(Id("t").Op("!=").Qual("encoding/json", "Delim")).Params(LitRune(']')).Block(
		returnInvalidTokenError(s.Name, "']'"),
	)
}

func unmarshalPrimitiveElement(g *Group) {
	g.Var().Id("v").Id("primitiveElement")

	g.Id("err").Op(":=").Id("v").Dot("unmarshalJSON").Call(Id("d"))

	g.If(Err().Op("!=").Nil()).Block(
		Return(Id("err")),
	)
}

func returnInvalidTokenError(in string, expected string) *Statement {
	return Return(Qual("fmt", "Errorf").Params(
		Lit("invalid token: %v, expected: "+expected+" in "+in+" element"),
		Id("t"),
	))
}

func implementUnmarshalContainedExternal(f *File) {
	f.Func().Params(Id("r").Op("*").Id("ContainedResource")).Id("UnmarshalJSON").Params(
		Id("b").Index().Byte(),
	).Params(Error()).Block(
		Id("d").Op(":=").Qual("encoding/json", "NewDecoder").Call(
			Qual("bytes", "NewReader").Call(Id("b")),
		),
		Return(Id("r").Dot("unmarshalJSON").Call(Id("d"))),
	)
}

func implementUnmarshalContainedInternal(f *File, resources []ir.ResourceOrType) {
	f.Func().Params(Id("cr").Op("*").Id("ContainedResource")).Id("unmarshalJSON").Params(
		Id("d").Op("*").Qual("encoding/json", "Decoder"),
	).Params(Error()).Block(
		Var().Id("rawValue").Qual("encoding/json", "RawMessage"),
		Id("err").Op(":=").Id("d").Dot("Decode").Call(Id("&rawValue")),
		If(Err().Op("!=").Nil()).Block(
			Return(Id("err")),
		),

		Var().Id("t").Struct(
			Id("ResourceType").String().Tag(map[string]string{"json": "resourceType"}),
		),
		Err().Op("=").Qual("encoding/json", "Unmarshal").Call(Id("rawValue"), Op("&").Id("t")),
		If(Err().Op("!=").Nil()).Block(
			Return(Err()),
		),

		Id("d").Op("=").Qual("encoding/json", "NewDecoder").
			Call(Qual("bytes", "NewReader").Call(Id("rawValue"))),

		Switch(Id("t.ResourceType")).BlockFunc(func(g *Group) {
			for _, r := range resources {
				g.Case(Lit(r.Name)).Block(
					Var().Id("r").Id(r.Name),
					Id("err").Op(":=").Id("r").Dot("unmarshalJSON").Call(Id("d")),
					If(Id("err").Op("!=").Nil()).Block(
						Return(Id("err")),
					),
					Op("*").Id("cr").Op("=").Id("ContainedResource").Values(Id("r")),
					Return(Nil()),
				)
			}

			g.Default().Block(
				Return(Qual("fmt", "Errorf").Call(Lit("unknown resource type: %s"), Id("t.ResourceType"))),
			)
		}),
	)
}

func implementUnmarshalPrimitiveElement(f *File) {
	f.Func().Params(Id("r").Op("*").Id("primitiveElement")).Id("unmarshalJSON").Params(
		Id("d").Op("*").Qual("encoding/json", "Decoder"),
	).Params(Error()).BlockFunc(func(g *Group) {
		g.List(Id("t"), Err()).Op(":=").Id("d").Dot("Token").Call()
		g.If(Err().Op("!=").Nil()).Block(
			Return(Err()),
		)
		g.If(Id("t").Op("==").Nil()).Block(
			Return(Nil()),
		).Else().If(Id("t").Op("!=").Qual("encoding/json", "Delim")).Params(LitRune('{')).Block(
			returnInvalidTokenError("primitive element", "'{'"),
		)

		g.For(Id("d").Dot("More").Call()).Block(
			List(Id("t"), Err()).Op("=").Id("d").Dot("Token").Call(),
			If(Err().Op("!=").Nil()).Block(
				Return(Err()),
			),
			List(Id("f"), Id("ok")).Op(":=").Id("t").Op(".").Call(String()),
			If(Op("!").Id("ok")).Block(
				returnInvalidTokenError("primitive element", "field name"),
			),
			Switch(Id("f")).BlockFunc(func(g *Group) {
				g.Case(Lit("id")).Block(
					Var().Id("v").String(),
					Id("err").Op(":=").Id("d").Dot("Decode").Call(Id("&v")),
					If(Err().Op("!=").Nil()).Block(
						Return(Id("err")),
					),
					Id("r.Id").Op("=").Id("&v"),
				)
				g.Case(Lit("extension")).Block(
					List(Id("t"), Err()).Op("=").Id("d").Dot("Token").Call(),
					If(Err().Op("!=").Nil()).Block(
						Return(Err()),
					),
					If(Id("t").Op("!=").Qual("encoding/json", "Delim")).Params(LitRune('[')).Block(
						returnInvalidTokenError("primitive element", "'['"),
					),
					For(Id("d").Dot("More").Call()).Block(
						Var().Id("v").Id("Extension"),
						Id("err").Op(":=").Id("v").Dot("unmarshalJSON").Call(Id("d")),
						If(Err().Op("!=").Nil()).Block(
							Return(Id("err")),
						),
						Id("r.Extension").Op("=").Append(Id("r.Extension"), Id("v")),
					),
					List(Id("t"), Err()).Op("=").Id("d").Dot("Token").Call(),
					If(Err().Op("!=").Nil()).Block(
						Return(Err()),
					),
					If(Id("t").Op("!=").Qual("encoding/json", "Delim")).Params(LitRune(']')).Block(
						returnInvalidTokenError("primitive element", "']'"),
					),
				)

				g.Default().Block(
					Return(Qual("fmt", "Errorf").Params(
						Lit("invalid field: %v in primitive element, expected \"id\" or \"extension\" (at index %v)"),
						Id("t"),
						Id("d").Dot("InputOffset").Call().Op("-").Lit(1),
					)),
				)
			}),
		)

		g.List(Id("t"), Err()).Op("=").Id("d").Dot("Token").Call()
		g.If(Err().Op("!=").Nil()).Block(
			Return(Err()),
		)
		g.If(Id("t").Op("!=").Qual("encoding/json", "Delim")).Params(LitRune('}')).Block(
			returnInvalidTokenError("primitive element", "'}'"),
		)
		g.Return(Nil())
	})
}
