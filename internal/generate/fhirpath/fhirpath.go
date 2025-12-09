package fhirpath

import (
	"fmt"
	"slices"
	"strings"

	"github.com/damedic/fhir-toolbox-go/internal/generate/ir"
	. "github.com/dave/jennifer/jen"
)

const (
	fhirpathModuleName = "github.com/damedic/fhir-toolbox-go/fhirpath"
	ucumSystem         = "http://unitsofmeasure.org"
)

var (
	stringTypes   = []string{"String", "Uri", "Code", "Oid", "Id", "Uuid", "Markdown", "Base64Binary", "Canonical", "Url"}
	intTypes      = []string{"Integer", "UnsignedInt", "PositiveInt"}
	longTypes     = []string{"Integer64"}
	dateTimeTypes = []string{"Date", "DateTime", "Instant"}
)

type FHIRPathGenerator struct{}

func (g FHIRPathGenerator) GenerateType(f *File, rt ir.ResourceOrType) bool {
	for _, s := range rt.Structs {
		generateChildrenFunc(f, s)
		generateToBooleanFunc(f, s)
		generateToStringFunc(f, s)
		generateToIntegerFunc(f, s)
		generateToLongFunc(f, s)
		generateToDecimalFunc(f, s)
		generateToDateFunc(f, s)
		generateToTimeFunc(f, s)
		generateToDateTimeFunc(f, s)
		generateToQuantityFunc(f, s)
		if s.IsPrimitive {
			generateHasValueFunc(f, s)
		}
		generateEqualFunc(f, s)
		generateEquivalentFunc(f, s)
		generateTypeInfoFunc(f, s)
	}

	return true
}

func generateChildrenFunc(f *File, s ir.Struct) {
	f.Func().Params(Id("r").Id(s.Name)).Id("Children").Params(Id("name").Op("...").String()).
		Qual(fhirpathModuleName, "Collection").BlockFunc(func(g *Group) {
		g.Var().Id("children").Qual(fhirpathModuleName, "Collection")
		for _, f := range s.Fields {
			if s.IsPrimitive && f.Name == "Value" {
				continue
			}

			g.If(Len(Id("name")).Op("==").Lit(0).Op("||").
				Qual("slices", "Contains").Call(Id("name"), Lit(f.MarshalName))).BlockFunc(func(g *Group) {
				if f.Multiple {
					g.For(List(Id("_"), Id("v")).Op(":=").Range().Id("r").Dot(f.Name)).Block(
						Id("children").Op("=").Append(Id("children"), Id("v")),
					)
				} else if f.Optional {
					g.If(Id("r").Dot(f.Name).Op("!=").Nil()).BlockFunc(func(g *Group) {
						if !s.IsResource && f.Name == "Id" {
							g.Id("children").Op("=").Append(
								Id("children"),
								Qual(fhirpathModuleName, "String").Call(Op("*").Id("r").Dot(f.Name)),
							)
						} else if f.Polymorph || f.PossibleTypes[0].IsNestedResource {
							g.Id("children").Op("=").Append(
								Id("children"),
								Id("r").Dot(f.Name),
							)
						} else {
							g.Id("children").Op("=").Append(
								Id("children"),
								Op("*").Id("r").Dot(f.Name),
							)
						}
					})
				} else {
					if s.Name == "Extension" && f.Name == "Url" {
						g.Id("children").Op("=").Append(
							Id("children"),
							Qual(fhirpathModuleName, "String").Call(Id("r").Dot(f.Name)),
						)
					} else {
						g.Id("children").Op("=").Append(
							Id("children"),
							Id("r").Dot(f.Name),
						)
					}
				}
			})
		}
		g.Return(Id("children"))
	})
}

func generateToBooleanFunc(f *File, s ir.Struct) {
	f.Func().Params(Id("r").Id(s.Name)).Id("ToBoolean").Params(Id("explicit").Bool()).Params(
		Qual(fhirpathModuleName, "Boolean"),
		Bool(),
		Error(),
	).BlockFunc(func(g *Group) {
		if s.Name == "Boolean" {
			g.If(Id("r").Dot("Value").Op("!=").Nil()).Block(
				Id("v").Op(":=").Qual(fhirpathModuleName, "Boolean").Call(Op("*").Id("r").Dot("Value")),
				Return(
					Id("v"),
					True(),
					Nil(),
				),
			).Else().Block(
				Return(
					False(),
					False(),
					Nil(),
				),
			)
		} else {
			g.Return(
				False(),
				False(),
				Qual("errors", "New").Call(Lit(fmt.Sprintf("can not convert %s to Boolean", s.Name))),
			)
		}
	})
}

func generateToStringFunc(f *File, s ir.Struct) {
	f.Func().Params(Id("r").Id(s.Name)).Id("ToString").Params(Id("explicit").Bool()).Params(
		Qual(fhirpathModuleName, "String"),
		Bool(),
		Error(),
	).BlockFunc(func(g *Group) {
		if slices.Contains(stringTypes, s.Name) {
			g.If(Id("r").Dot("Value").Op("!=").Nil()).Block(
				Id("v").Op(":=").Qual(fhirpathModuleName, "String").Call(Op("*").Id("r").Dot("Value")),
				Return(
					Id("v"),
					True(),
					Nil(),
				),
			).Else().Block(
				Return(
					Lit(""),
					False(),
					Nil(),
				),
			)
		} else {
			g.Return(
				Lit(""),
				False(),
				Qual("errors", "New").Call(Lit(fmt.Sprintf("can not convert %s to String", s.Name))),
			)
		}
	})
}

func generateToIntegerFunc(f *File, s ir.Struct) {
	f.Func().Params(Id("r").Id(s.Name)).Id("ToInteger").Params(Id("explicit").Bool()).Params(
		Qual(fhirpathModuleName, "Integer"),
		Bool(),
		Error(),
	).BlockFunc(func(g *Group) {
		if slices.Contains(intTypes, s.Name) {
			g.If(Id("r").Dot("Value").Op("!=").Nil()).Block(
				Id("v").Op(":=").Qual(fhirpathModuleName, "Integer").Call(Op("*").Id("r").Dot("Value")),
				Return(
					Id("v"),
					True(),
					Nil(),
				),
			).Else().Block(
				Return(
					Lit(0),
					False(),
					Nil(),
				),
			)
		} else {
			g.Return(
				Lit(0),
				False(),
				Qual("errors", "New").Call(Lit(fmt.Sprintf("can not convert %s to Integer", s.Name))),
			)
		}
	})
}

func generateToLongFunc(f *File, s ir.Struct) {
	f.Func().Params(Id("r").Id(s.Name)).Id("ToLong").Params(Id("explicit").Bool()).Params(
		Qual(fhirpathModuleName, "Long"),
		Bool(),
		Error(),
	).BlockFunc(func(g *Group) {
		switch {
		case s.Name == "Boolean":
			g.If(Id("r").Dot("Value").Op("==").Nil()).Block(
				Return(
					Qual(fhirpathModuleName, "Long").Call(Lit(0)),
					False(),
					Nil(),
				),
			)
			g.If(Op("*").Id("r").Dot("Value")).Block(
				Return(
					Qual(fhirpathModuleName, "Long").Call(Lit(1)),
					True(),
					Nil(),
				),
			).Else().Block(
				Return(
					Qual(fhirpathModuleName, "Long").Call(Lit(0)),
					True(),
					Nil(),
				),
			)
		case slices.Contains(intTypes, s.Name):
			g.If(Id("r").Dot("Value").Op("!=").Nil()).Block(
				Return(
					Qual(fhirpathModuleName, "Long").Call(Op("*").Id("r").Dot("Value")),
					True(),
					Nil(),
				),
			).Else().Block(
				Return(
					Qual(fhirpathModuleName, "Long").Call(Lit(0)),
					False(),
					Nil(),
				),
			)
		case slices.Contains(longTypes, s.Name):
			g.If(Id("r").Dot("Value").Op("!=").Nil()).Block(
				Return(
					Qual(fhirpathModuleName, "Long").Call(Op("*").Id("r").Dot("Value")),
					True(),
					Nil(),
				),
			).Else().Block(
				Return(
					Qual(fhirpathModuleName, "Long").Call(Lit(0)),
					False(),
					Nil(),
				),
			)
		case slices.Contains(stringTypes, s.Name):
			g.If(Id("r").Dot("Value").Op("==").Nil()).Block(
				Return(
					Qual(fhirpathModuleName, "Long").Call(Lit(0)),
					False(),
					Nil(),
				),
			)
			g.List(Id("v"), Id("err")).Op(":=").Qual("strconv", "ParseInt").Call(
				Op("*").Id("r").Dot("Value"),
				Lit(10),
				Lit(64),
			)
			g.If(Id("err").Op("==").Nil()).Block(
				Return(
					Qual(fhirpathModuleName, "Long").Call(Id("v")),
					True(),
					Nil(),
				),
			).Else().Block(
				Return(
					Qual(fhirpathModuleName, "Long").Call(Lit(0)),
					False(),
					Nil(),
				),
			)
		default:
			g.Return(
				Qual(fhirpathModuleName, "Long").Call(Lit(0)),
				False(),
				Qual("errors", "New").Call(Lit(fmt.Sprintf("can not convert %s to Long", s.Name))),
			)
		}
	})
}

func generateToDecimalFunc(f *File, s ir.Struct) {
	f.Func().Params(Id("r").Id(s.Name)).Id("ToDecimal").Params(Id("explicit").Bool()).Params(
		Qual(fhirpathModuleName, "Decimal"),
		Bool(),
		Error(),
	).BlockFunc(func(g *Group) {
		if s.Name == "Decimal" {
			g.If(Id("r").Dot("Value").Op("!=").Nil()).Block(
				Id("v").Op(":=").Qual(fhirpathModuleName, "Decimal").Values(Dict{
					Id("Value"): Id("r").Dot("Value"),
				}),
				Return(
					Id("v"),
					True(),
					Nil(),
				),
			).Else().Block(
				Return(
					Qual(fhirpathModuleName, "Decimal").Block(),
					False(),
					Nil(),
				),
			)
		} else {
			g.Return(
				Qual(fhirpathModuleName, "Decimal").Block(),
				False(),
				Qual("errors", "New").Call(Lit(fmt.Sprintf("can not convert %s to Decimal", s.Name))),
			)
		}
	})
}

func generateToDateFunc(f *File, s ir.Struct) {
	f.Func().Params(Id("r").Id(s.Name)).Id("ToDate").Params(Id("explicit").Bool()).Params(
		Qual(fhirpathModuleName, "Date"),
		Bool(),
		Error(),
	).BlockFunc(func(g *Group) {
		if s.Name == "Date" {
			g.If(Id("r").Dot("Value").Op("!=").Nil()).Block(
				List(Id("v"), Err()).Op(":=").Qual(fhirpathModuleName, "ParseDate").Call(Op("*").Id("r").Dot("Value")),
				Return(
					Id("v"),
					True(),
					Err(),
				),
			).Else().Block(
				Return(
					Qual(fhirpathModuleName, "Date").Block(),
					False(),
					Nil(),
				),
			)
		} else {
			g.Return(
				Qual(fhirpathModuleName, "Date").Block(),
				False(),
				Qual("errors", "New").Call(Lit(fmt.Sprintf("can not convert %s to Date", s.Name))),
			)
		}
	})
}

func generateToTimeFunc(f *File, s ir.Struct) {
	f.Func().Params(Id("r").Id(s.Name)).Id("ToTime").Params(Id("explicit").Bool()).Params(
		Qual(fhirpathModuleName, "Time"),
		Bool(),
		Error(),
	).BlockFunc(func(g *Group) {
		if s.Name == "Time" {
			g.If(Id("r").Dot("Value").Op("!=").Nil()).Block(
				List(Id("v"), Err()).Op(":=").Qual(fhirpathModuleName, "ParseTime").Call(Op("*").Id("r").Dot("Value")),
				Return(
					Id("v"),
					True(),
					Err(),
				),
			).Else().Block(
				Return(
					Qual(fhirpathModuleName, "Time").Block(),
					False(),
					Nil(),
				),
			)
		} else {
			g.Return(
				Qual(fhirpathModuleName, "Time").Block(),
				False(),
				Qual("errors", "New").Call(Lit(fmt.Sprintf("can not convert %s to Time", s.Name))),
			)
		}
	})
}

func generateToDateTimeFunc(f *File, s ir.Struct) {
	f.Func().Params(Id("r").Id(s.Name)).Id("ToDateTime").Params(Id("explicit").Bool()).Params(
		Qual(fhirpathModuleName, "DateTime"),
		Bool(),
		Error(),
	).BlockFunc(func(g *Group) {
		if slices.Contains(dateTimeTypes, s.Name) {
			g.If(Id("r").Dot("Value").Op("!=").Nil()).Block(
				List(Id("v"), Err()).Op(":=").Qual(fhirpathModuleName, "ParseDateTime").Call(Op("*").Id("r").Dot("Value")),
				Return(
					Id("v"),
					True(),
					Err(),
				),
			).Else().Block(
				Return(
					Qual(fhirpathModuleName, "DateTime").Block(),
					False(),
					Nil(),
				),
			)
		} else {
			g.Return(
				Qual(fhirpathModuleName, "DateTime").Block(),
				False(),
				Qual("errors", "New").Call(Lit(fmt.Sprintf("can not convert %s to DateTime", s.Name))),
			)
		}
	})
}

func generateToQuantityFunc(f *File, s ir.Struct) {
	f.Func().Params(Id("r").Id(s.Name)).Id("ToQuantity").Params(Id("explicit").Bool()).Params(
		Qual(fhirpathModuleName, "Quantity"),
		Bool(),
		Error(),
	).BlockFunc(func(g *Group) {
		if s.Name == "Quantity" {
			g.If(Id("r").Dot("System").Op("==").Nil().Op("||").
				Id("r").Dot("System").Dot("Value").Op("==").Nil().Op("||").
				Op("*").Id("r").Dot("System").Dot("Value").Op("!=").Lit(ucumSystem)).Block(
				Return(
					Qual(fhirpathModuleName, "Quantity").Block(),
					False(),
					Qual("errors", "New").Call(Lit(fmt.Sprintf("can not convert %s to Quantity, no UCUM system", s.Name))),
				),
			).Else().If(Id("r").Dot("Value").Op("==").Nil()).Block(
				Return(
					Qual(fhirpathModuleName, "Quantity").Block(),
					False(),
					Nil(),
				),
			)

			g.Var().Id("unit").String()
			g.If(Id("r").Dot("Code").Op("!=").Nil().Op("&&").
				Id("r").Dot("Code").Dot("Value").Op("!=").Nil()).Block(
				Switch(Op("*").Id("r").Dot("Code").Dot("Value")).Block(
					Case(Lit("a")).Id("unit").Op("=").Lit("year"),
					Case(Lit("mo")).Id("unit").Op("=").Lit("month"),
					Case(Lit("d")).Id("unit").Op("=").Lit("day"),
					Case(Lit("h")).Id("unit").Op("=").Lit("hour"),
					Case(Lit("min")).Id("unit").Op("=").Lit("minute"),
					Case(Lit("s")).Id("unit").Op("=").Lit("second"),
					Default().Id("unit").Op("=").Op("*").Id("r").Dot("Code").Dot("Value"),
				),
			)

			g.Return(
				Qual(fhirpathModuleName, "Quantity").Values(Dict{
					Id("Value"): Qual(fhirpathModuleName, "Decimal").Values(Dict{
						Id("Value"): Id("r").Dot("Value").Dot("Value"),
					}),
					Id("Unit"): Qual(fhirpathModuleName, "String").Call(Id("unit")),
				}),
				True(),
				Nil(),
			)
		} else {
			g.Return(
				Qual(fhirpathModuleName, "Quantity").Block(),
				False(),
				Qual("errors", "New").Call(Lit(fmt.Sprintf("can not convert %s to Quantity", s.Name))),
			)
		}
	})
}

func generateHasValueFunc(f *File, s ir.Struct) {
	f.Func().Params(Id("r").Id(s.Name)).Id("HasValue").Params().Bool().BlockFunc(func(g *Group) {
		if s.Name == "Xhtml" {
			// Xhtml.Value is a string field, not a pointer
			g.Return(Id("r.Value").Op("!=").Lit(""))
		} else if s.Name == "Decimal" {
			// Decimal.Value is *apd.Decimal
			g.Return(Id("r.Value").Op("!=").Nil())
		} else {
			// Most primitives have *string or *bool or *int32 Value field
			g.Return(Id("r.Value").Op("!=").Nil())
		}
	})
}

func generateTypeInfoFunc(f *File, s ir.Struct) {
	f.Func().Params(Id("r").Id(s.Name)).Id("TypeInfo").Params().Qual(fhirpathModuleName, "TypeInfo").
		Block(ReturnFunc(func(g *Group) {
			generateType(g, s)
		}))
}

func generateEqualFunc(f *File, s ir.Struct) {
	f.Func().Params(Id("r").Id(s.Name)).Id("Equal").Params(
		Id("other").Qual(fhirpathModuleName, "Element"),
	).Params(
		Bool(),
		Bool(),
	).BlockFunc(func(g *Group) {
		if method, ok := primitiveConversionMethod(s); ok {
			g.List(Id("v"), Id("ok"), Err()).Op(":=").Id("r").Dot(method).Call(False())
			g.If(Err().Op("!=").Nil().Op("||").Op("!").Id("ok")).Block(Return(False(), True()))
			g.Return(Id("v").Dot("Equal").Call(Id("other")))
			return
		}
		if s.IsPrimitive {
			if s.Name == "Xhtml" {
				g.List(Id("o"), Id("ok")).Op(":=").Id("other").Dot("").Call(Id(s.Name))
				g.If(Op("!").Id("ok")).Block(Return(False(), True()))
				g.Return(Id("r.Value").Op("==").Id("o.Value"), True())
				return
			}
			g.List(Id("o"), Id("ok")).Op(":=").Id("other").Dot("").Call(Id(s.Name))
			g.If(Op("!").Id("ok")).Block(Return(False(), True()))
			g.If(Id("r.Value").Op("==").Nil().Op("||").Id("o.Value").Op("==").Nil()).
				Block(Return(False(), True()))
			g.Return(Id("*r.Value").Op("==").Id("*o.Value"), True())
			return
		}
		if s.Name == "Quantity" {
			g.List(Id("a"), Id("ok"), Err()).Op(":=").Id("r").Dot("ToQuantity").Call(False())
			g.If(Err().Op("!=").Nil().Op("||").Op("!").Id("ok")).Block(Return(False(), True()))
			g.Return(Id("a").Dot("Equal").Call(Id("other")))
			return
		}
		g.Var().Id("o").Op("*").Id(s.Name)
		g.Switch(Id("other").Op(":=").Id("other").Dot("(type)")).Block(
			Case(Id(s.Name)).Block(
				Id("o").Op("=").Op("&").Id("other"),
			),
			Case(Op("*").Id(s.Name)).Block(
				Id("o").Op("=").Id("other"),
			),
			Default().Block(
				Return(False(), True()),
			),
		)
		g.If(Id("o").Op("==").Nil()).Block(
			Return(False(), True()),
		)
		g.List(Id("eq"), Id("ok")).Op(":=").Id("r").Dot("Children").Call().Dot("Equal").Call(
			Id("o").Dot("Children").Call(),
		)
		g.Return(Id("eq").Op("&&").Id("ok"), True())
	})
}

func generateEquivalentFunc(f *File, s ir.Struct) {
	f.Func().Params(Id("r").Id(s.Name)).Id("Equivalent").Params(
		Id("other").Qual(fhirpathModuleName, "Element"),
	).Params(Bool()).BlockFunc(func(g *Group) {
		if method, ok := primitiveConversionMethod(s); ok {
			g.List(Id("v"), Id("ok"), Err()).Op(":=").Id("r").Dot(method).Call(False())
			g.If(Err().Op("!=").Nil().Op("||").Op("!").Id("ok")).Block(Return(False()))
			g.Return(Id("v").Dot("Equivalent").Call(Id("other")))
			return
		}
		if !s.IsPrimitive && s.Name != "Quantity" {
			g.List(Id("o"), Id("ok")).Op(":=").Id("other.").Call(Id(s.Name))
			g.If(Op("!").Id("ok")).Block(Return(False()))
			if s.Name == "Coding" {
				g.List(Id("eq"), Id("ok")).Op(":=").Id("r.Code").Dot("Equal").Call(Id("o.Code"))
				g.If(Op("!").Id("ok").Op("||").Op("!").Id("eq")).Block(Return(False()))
				g.List(Id("eq"), Id("ok")).Op("=").Id("r.System").Dot("Equal").Call(Id("o.System"))
				g.Return(Id("eq").Op("&&").Id("ok"))
			} else if s.Name == "CodeableConcept" {
				g.Var().Id("leftCollection").Qual(fhirpathModuleName, "Collection")
				g.For(List(Id("_"), Id("c").Op(":=").Range().Id("r.Coding"))).Block(
					Id("leftCollection").Op("=").Append(Id("leftCollection"), Id("c")),
				)
				g.Var().Id("rightCollection").Qual(fhirpathModuleName, "Collection")
				g.For(List(Id("_"), Id("c").Op(":=").Range().Id("o.Coding"))).Block(
					Id("rightCollection").Op("=").Append(Id("leftCollection"), Id("c")),
				)
				g.Return().Len(Id("leftCollection").Dot("Union").Call(Id("rightCollection"))).Op(">").Lit(0)
			} else {
				g.Id("r.Id").Op("=").Nil()
				g.Id("o.Id").Op("=").Nil()
				g.List(Id("eq"), Id("ok")).Op(":=").Id("r").Dot("Equal").Call(Id("o"))
				g.Return(Id("eq").Op("&&").Id("ok"))
			}
			return
		}
		if s.IsPrimitive {
			if s.Name == "Xhtml" {
				g.List(Id("o"), Id("ok")).Op(":=").Id("other").Dot("").Call(Id(s.Name))
				g.If(Op("!").Id("ok")).Block(Return(False()))
				g.Return(Id("r.Value").Op("==").Id("o.Value"))
				return
			}
			g.List(Id("o"), Id("ok")).Op(":=").Id("other").Dot("").Call(Id(s.Name))
			g.If(Op("!").Id("ok")).Block(Return(False()))
			g.If(Id("r.Value").Op("==").Nil().Op("||").Id("o.Value").Op("==").Nil()).
				Block(Return(False()))
			g.Return(Id("*r.Value").Op("==").Id("*o.Value"))
			return
		}
		if s.Name == "Quantity" {
			g.List(Id("eq"), Id("ok")).Op(":=").Id("r").Dot("Equal").Call(Id("other"))
			g.Return(Id("eq").Op("&&").Id("ok"))
			return
		}
		g.List(Id("eq"), Id("ok")).Op(":=").Id("r").Dot("Equal").Call(Id("other"))
		g.Return(Id("eq").Op("&&").Id("ok"))
	})
}

func (g FHIRPathGenerator) GenerateAdditional(f func(fileName string, pkgName string) *File, release string, rt []ir.ResourceOrType) {
	contextFile := f("fhirpath", strings.ToLower(release))

	generateContextFunc(contextFile)
	generateWithContext(contextFile)
	generateTypes(contextFile, rt)
	generateFunctions(contextFile)
}

func generateContextFunc(f *File) *Statement {
	return f.Func().Id("Context").Params().Qual("context", "Context").Block(
		Return(
			Id("WithContext").Call(Qual("context", "Background").Call()),
		),
	)
}

func primitiveConversionMethod(s ir.Struct) (string, bool) {
	switch {
	case s.Name == "Boolean":
		return "ToBoolean", true
	case slices.Contains(stringTypes, s.Name):
		return "ToString", true
	case slices.Contains(intTypes, s.Name):
		return "ToInteger", true
	case s.Name == "Decimal":
		return "ToDecimal", true
	case s.Name == "Time":
		return "ToTime", true
	case slices.Contains(dateTimeTypes, s.Name):
		return "ToDateTime", true
	case s.Name == "Date":
		return "ToDate", true
	case s.Name == "Quantity":
		return "ToQuantity", true
	default:
		return "", false
	}
}

func generateWithContext(contextFile *File) *Statement {
	return contextFile.Func().Id("WithContext").Params(
		Id("ctx").Qual("context", "Context"),
	).Qual("context", "Context").Block(
		Id("ctx").Op("=").Qual(fhirpathModuleName, "WithNamespace").Call(
			Id("ctx"),
			Lit("FHIR"),
		),
		Id("ctx").Op("=").Qual(fhirpathModuleName, "WithTypes").Call(
			Id("ctx"),
			Id("allFHIRPathTypes"),
		),
		Id("ctx").Op("=").Qual(fhirpathModuleName, "WithFunctions").Call(
			Id("ctx"),
			Qual(fhirpathModuleName, "FHIRFunctions"),
		),
		Return(Id("ctx")),
	)
}

func generateFunctions(f *File) *Statement {
	return f.Var().Id("fhirFunctions").Op("=").Qual(fhirpathModuleName, "Functions").Values()
}
