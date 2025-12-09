package generate

import (
	"github.com/damedic/fhir-toolbox-go/internal/generate/ir"
	"strings"

	. "github.com/dave/jennifer/jen"
	"github.com/iancoleman/strcase"
)

type CapabilitiesGenerator struct {
	NoOpGenerator
}

func (g CapabilitiesGenerator) GenerateAdditional(f func(fileName string, pkgName string) *File, release string, rt []ir.ResourceOrType) {
	generateCapability(f("create", "capabilities"+release), ir.FilterResources(rt), release, "create")
	generateCapability(f("read", "capabilities"+release), ir.FilterResources(rt), release, "read")
	generateCapability(f("update", "capabilities"+release), ir.FilterResources(rt), release, "update")
	generateCapability(f("delete", "capabilities"+release), ir.FilterResources(rt), release, "delete")
	generateCapability(f("search", "capabilities"+release), ir.FilterResources(rt), release, "search")
}

func generateCapability(f *File, resources []ir.ResourceOrType, release, interaction string) {
	interactionName := strcase.ToCamel(interaction)

	for _, r := range resources {
		f.Commentf("// %s%s needs to be implemented to support the %s interaction.", r.Name, interactionName, interaction)
		f.Type().Id(r.Name + interactionName).InterfaceFunc(func(g *Group) {
			switch interaction {
			case "create":
				g.Id(interactionName+r.Name).
					Params(
						Id("ctx").Qual("context", "Context"),
						Id("resource").Qual(moduleName+"/model/gen/"+strings.ToLower(release), r.Name),
					).
					Params(
						Qual(moduleName+"/model/gen/"+strings.ToLower(release), r.Name),
						Error(),
					)
			case "read":
				g.Id(interactionName+r.Name).
					Params(
						Id("ctx").Qual("context", "Context"),
						Id("id").String(),
					).
					Params(
						Qual(moduleName+"/model/gen/"+strings.ToLower(release), r.Name),
						Error(),
					)
			case "update":
				g.Id(interactionName+r.Name).
					Params(
						Id("ctx").Qual("context", "Context"),
						Id("resource").Qual(moduleName+"/model/gen/"+strings.ToLower(release), r.Name),
					).
					Params(
						Qual(moduleName+"/capabilities/update", "Result").Index(Qual(moduleName+"/model/gen/"+strings.ToLower(release), r.Name)),
						Error(),
					)
			case "delete":
				g.Id(interactionName+r.Name).
					Params(
						Id("ctx").Qual("context", "Context"),
						Id("id").String(),
					).
					Params(
						Error(),
					)
			case "search":
				g.Id("SearchCapabilities"+r.Name).Params(Id("ctx").Qual("context", "Context")).Params(
					searchCapabilitiesType(release),
					Error(),
				)
				g.Id(interactionName+r.Name).
					Params(
						Id("ctx").Qual("context", "Context"),
						Id("parameters").Qual(moduleName+"/capabilities/search", "Parameters"),
						Id("options").Qual(moduleName+"/capabilities/search", "Options"),
					).
					Params(
						Qual(moduleName+"/capabilities/search", "Result").Index(Qual(moduleName+"/model/gen/"+strings.ToLower(release), r.Name)),
						Error(),
					)
			}
		})

		if interaction == "update" {
			f.Commentf("// %sUpdateCapabilities is optional and only needs to be implemented if the backend deviates from the default behavior.", r.Name)
			f.Type().Id(r.Name + "UpdateCapabilities").Interface(
				Id("UpdateCapabilities"+r.Name).Params(Id("ctx").Qual("context", "Context")).Params(
					Qual(moduleName+"/capabilities/update", "Capabilities"),
					Error(),
				),
			)
		}
	}
}
