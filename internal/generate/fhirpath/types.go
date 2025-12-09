package fhirpath

import (
	"github.com/damedic/fhir-toolbox-go/internal/generate/ir"
	. "github.com/dave/jennifer/jen"
)

func generateTypes(f *File, rt []ir.ResourceOrType) {
	f.Var().Id("allFHIRPathTypes").Op("=").Index().Qual(fhirpathModuleName, "TypeInfo").
		ValuesFunc(func(g *Group) {
			for _, t := range baseTypes {
				g.Add(t)
			}

			for _, t := range rt {
				generateType(g, t.Structs[0])
			}
		})
}

func generateType(g *Group, s ir.Struct) {
	var base *Statement
	if s.IsResource {
		base = Qual(fhirpathModuleName, "TypeSpecifier").Values(Dict{
			Id("Namespace"): Lit("FHIR"),
			Id("Name"):      Lit("DomainResource"),
		})
	} else if s.BaseType != "" {
		// Use the actual base type from FHIR spec for all types
		base = Qual(fhirpathModuleName, "TypeSpecifier").Values(Dict{
			Id("Namespace"): Lit("FHIR"),
			Id("Name"):      Lit(s.BaseType),
		})
	} else if s.IsPrimitive {
		// Fallback for primitives without baseDefinition
		base = Qual(fhirpathModuleName, "TypeSpecifier").Values(Dict{
			Id("Namespace"): Lit("FHIR"),
			Id("Name"):      Lit("Element"),
		})
	} else {
		// Fallback for non-primitives without baseDefinition
		base = Qual(fhirpathModuleName, "TypeSpecifier").Values(Dict{
			Id("Namespace"): Lit("FHIR"),
			Id("Name"):      Lit("DataType"),
		})
	}

	elements := Index().Qual(fhirpathModuleName, "ClassInfoElement").ValuesFunc(func(g *Group) {
		for _, f := range s.Fields {
			if s.IsPrimitive && f.Name == "Value" {
				continue
			}

			var t *Statement
			if f.Polymorph {
				t = Qual(fhirpathModuleName, "TypeSpecifier").Values(Dict{
					Id("Namespace"): Lit("System"),
					Id("Name"):      Lit("Any"),
					Id("List"):      Lit(f.Multiple),
				})
			} else {
				t = Qual(fhirpathModuleName, "TypeSpecifier").Values(Dict{
					Id("Namespace"): Lit("FHIR"),
					Id("Name"):      Lit(f.PossibleTypes[0].Name),
					Id("List"):      Lit(f.Multiple),
				})
			}
			g.Values(Dict{
				Id("Name"): Lit(f.MarshalName),
				Id("Type"): t,
			})
		}
	})

	g.Qual(fhirpathModuleName, "ClassInfo").Values(Dict{
		Id("Namespace"): Lit("FHIR"),
		Id("Name"):      Lit(s.MarshalName),
		Id("BaseType"):  base,
		Id("Element"):   elements,
	})
}

var baseTypes = []Code{
	Qual(fhirpathModuleName, "ClassInfo").Values(Dict{
		Id("Namespace"): Lit("FHIR"),
		Id("Name"):      Lit("Base"),
		Id("BaseType"): Qual(fhirpathModuleName, "TypeSpecifier").Values(Dict{
			Id("Namespace"): Lit("System"),
			Id("Name"):      Lit("Any"),
		}),
	}),
	Qual(fhirpathModuleName, "ClassInfo").Values(Dict{
		Id("Namespace"): Lit("FHIR"),
		Id("Name"):      Lit("Element"),
		Id("BaseType"): Qual(fhirpathModuleName, "TypeSpecifier").Values(Dict{
			Id("Namespace"): Lit("FHIR"),
			Id("Name"):      Lit("Base"),
		}),
	}),
	Qual(fhirpathModuleName, "ClassInfo").Values(Dict{
		Id("Namespace"): Lit("FHIR"),
		Id("Name"):      Lit("DataType"),
		Id("BaseType"): Qual(fhirpathModuleName, "TypeSpecifier").Values(Dict{
			Id("Namespace"): Lit("FHIR"),
			Id("Name"):      Lit("Element"),
		}),
	}),
	Qual(fhirpathModuleName, "ClassInfo").Values(Dict{
		Id("Namespace"): Lit("FHIR"),
		Id("Name"):      Lit("PrimitiveType"),
		Id("BaseType"): Qual(fhirpathModuleName, "TypeSpecifier").Values(Dict{
			Id("Namespace"): Lit("FHIR"),
			Id("Name"):      Lit("DataType"),
		}),
	}),
	Qual(fhirpathModuleName, "ClassInfo").Values(Dict{
		Id("Namespace"): Lit("FHIR"),
		Id("Name"):      Lit("BackboneElement"),
		Id("BaseType"): Qual(fhirpathModuleName, "TypeSpecifier").Values(Dict{
			Id("Namespace"): Lit("FHIR"),
			Id("Name"):      Lit("Element"),
		}),
		Id("Element"): Index().Qual(fhirpathModuleName, "ClassInfoElement").Values(Values(Dict{
			Id("Name"): Lit("modifierExtension"),
			Id("Type"): Qual(fhirpathModuleName, "TypeSpecifier").Values(Dict{
				Id("Namespace"): Lit("FHIR"),
				Id("Name"):      Lit("Extension"),
				Id("List"):      Lit(true),
			}),
		})),
	}),
	Qual(fhirpathModuleName, "ClassInfo").Values(Dict{
		Id("Namespace"): Lit("FHIR"),
		Id("Name"):      Lit("BackboneType"),
		Id("BaseType"): Qual(fhirpathModuleName, "TypeSpecifier").Values(Dict{
			Id("Namespace"): Lit("FHIR"),
			Id("Name"):      Lit("DataType"),
		}),
		Id("Element"): Index().Qual(fhirpathModuleName, "ClassInfoElement").Values(Values(Dict{
			Id("Name"): Lit("modifierExtension"),
			Id("Type"): Qual(fhirpathModuleName, "TypeSpecifier").Values(Dict{
				Id("Namespace"): Lit("FHIR"),
				Id("Name"):      Lit("Extension"),
				Id("List"):      Lit(true),
			}),
		})),
	}),
	Qual(fhirpathModuleName, "ClassInfo").Values(Dict{
		Id("Namespace"): Lit("FHIR"),
		Id("Name"):      Lit("Resource"),
		Id("BaseType"): Qual(fhirpathModuleName, "TypeSpecifier").Values(Dict{
			Id("Namespace"): Lit("FHIR"),
			Id("Name"):      Lit("Base"),
		}),
		Id("Element"): Index().Qual(fhirpathModuleName, "ClassInfoElement").Values(
			Values(Dict{
				Id("Name"): Lit("id"),
				Id("Type"): Qual(fhirpathModuleName, "TypeSpecifier").Values(Dict{
					Id("Namespace"): Lit("FHIR"),
					Id("Name"):      Lit("id"),
				}),
			}),
			Values(Dict{
				Id("Name"): Lit("meta"),
				Id("Type"): Qual(fhirpathModuleName, "TypeSpecifier").Values(Dict{
					Id("Namespace"): Lit("FHIR"),
					Id("Name"):      Lit("Meta"),
				}),
			}),
			Values(Dict{
				Id("Name"): Lit("implicitRules"),
				Id("Type"): Qual(fhirpathModuleName, "TypeSpecifier").Values(Dict{
					Id("Namespace"): Lit("FHIR"),
					Id("Name"):      Lit("uri"),
				}),
			}),
			Values(Dict{
				Id("Name"): Lit("language"),
				Id("Type"): Qual(fhirpathModuleName, "TypeSpecifier").Values(Dict{
					Id("Namespace"): Lit("FHIR"),
					Id("Name"):      Lit("code"),
				}),
			}),
		),
	}),
	Qual(fhirpathModuleName, "ClassInfo").Values(Dict{
		Id("Namespace"): Lit("FHIR"),
		Id("Name"):      Lit("DomainResource"),
		Id("BaseType"): Qual(fhirpathModuleName, "TypeSpecifier").Values(Dict{
			Id("Namespace"): Lit("FHIR"),
			Id("Name"):      Lit("Resource"),
		}),
		Id("Element"): Index().Qual(fhirpathModuleName, "ClassInfoElement").Values(
			Values(Dict{
				Id("Name"): Lit("id"),
				Id("Type"): Qual(fhirpathModuleName, "TypeSpecifier").Values(Dict{
					Id("Namespace"): Lit("FHIR"),
					Id("Name"):      Lit("id"),
				}),
			}),
			Values(Dict{
				Id("Name"): Lit("meta"),
				Id("Type"): Qual(fhirpathModuleName, "TypeSpecifier").Values(Dict{
					Id("Namespace"): Lit("FHIR"),
					Id("Name"):      Lit("Meta"),
				}),
			}),
			Values(Dict{
				Id("Name"): Lit("implicitRules"),
				Id("Type"): Qual(fhirpathModuleName, "TypeSpecifier").Values(Dict{
					Id("Namespace"): Lit("FHIR"),
					Id("Name"):      Lit("uri"),
				}),
			}),
			Values(Dict{
				Id("Name"): Lit("language"),
				Id("Type"): Qual(fhirpathModuleName, "TypeSpecifier").Values(Dict{
					Id("Namespace"): Lit("FHIR"),
					Id("Name"):      Lit("code"),
				}),
			}),
			Values(Dict{
				Id("Name"): Lit("text"),
				Id("Type"): Qual(fhirpathModuleName, "TypeSpecifier").Values(Dict{
					Id("Namespace"): Lit("FHIR"),
					Id("Name"):      Lit("Narrative"),
				}),
			}),
			Values(Dict{
				Id("Name"): Lit("contained"),
				Id("Type"): Qual(fhirpathModuleName, "TypeSpecifier").Values(Dict{
					Id("Namespace"): Lit("FHIR"),
					Id("Name"):      Lit("Resource"),
					Id("List"):      Lit(true),
				}),
			}),
			Values(Dict{
				Id("Name"): Lit("extension"),
				Id("Type"): Qual(fhirpathModuleName, "TypeSpecifier").Values(Dict{
					Id("Namespace"): Lit("FHIR"),
					Id("Name"):      Lit("Extension"),
					Id("List"):      Lit(true),
				}),
			}),
			Values(Dict{
				Id("Name"): Lit("modifierExtension"),
				Id("Type"): Qual(fhirpathModuleName, "TypeSpecifier").Values(Dict{
					Id("Namespace"): Lit("FHIR"),
					Id("Name"):      Lit("Extension"),
					Id("List"):      Lit(true),
				}),
			}),
		),
	}),
}
