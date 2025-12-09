package generate

import (
	"github.com/damedic/fhir-toolbox-go/internal/generate/ir"
	"slices"
	"strings"

	. "github.com/dave/jennifer/jen"
	"github.com/iancoleman/strcase"
)

const genericWrapperName = "Generic"
const concreteWrapperName = "Concrete"

type CapabilitiesWrapperGenerator struct {
	NoOpGenerator
}

func (g CapabilitiesWrapperGenerator) GenerateAdditional(f func(fileName string, pkgName string) *File, release string, rt []ir.ResourceOrType) {
	generateGenericWrapperStruct(f("generic", "capabilities"+release), release)
	generateWrapperCapabilityStatement(f("generic", "capabilities"+release), release, ir.FilterResources(rt))
	generateSearchParametersFn(f("generic", "capabilities"+release), release, ir.FilterResources(rt))
	generatePopulateSearchParameterFn(f("generic", "capabilities"+release), release)
	generateOperationDefinitionsFn(f("generic", "capabilities"+release), release)
	generateGeneric(f("generic", "capabilities"+release), release, ir.FilterResources(rt), "create")
	generateGeneric(f("generic", "capabilities"+release), release, ir.FilterResources(rt), "read")
	generateGeneric(f("generic", "capabilities"+release), release, ir.FilterResources(rt), "update")
	generateGeneric(f("generic", "capabilities"+release), release, ir.FilterResources(rt), "delete")
	generateGeneric(f("generic", "capabilities"+release), release, ir.FilterResources(rt), "search")

	generateConcreteWrapperStruct(f("concrete", "capabilities"+release), release)
	generateConcreteCapabilityBase(f("concrete", "capabilities"+release), release)
	generateConcrete(f("concrete", "capabilities"+release), release, ir.FilterResources(rt), "create")
	generateConcrete(f("concrete", "capabilities"+release), release, ir.FilterResources(rt), "read")
	generateConcrete(f("concrete", "capabilities"+release), release, ir.FilterResources(rt), "update")
	generateConcrete(f("concrete", "capabilities"+release), release, ir.FilterResources(rt), "delete")
	generateConcrete(f("concrete", "capabilities"+release), release, ir.FilterResources(rt), "search")

	generateGenericInvokeOperation(f("generic", "capabilities"+release), release)
}

func generateGenericWrapperStruct(f *File, release string) {
	f.Type().Id(genericWrapperName).Struct(
		Id("Concrete").Qual(moduleName+"/capabilities", "ConcreteCapabilities").Index(Qual(moduleName+"/model/gen/"+strings.ToLower(release), "CapabilityStatement")),
	)
}

func generateGeneric(f *File, release string, resources []ir.ResourceOrType, interaction string) {
	interactionName := strcase.ToCamel(interaction)

	params := []Code{Id("ctx").Qual("context", "Context")}
	shortcutParams := []Code{Id("ctx")}
	passParams := []Code{Id("ctx")}
	switchType := Id("resourceType")
	var returnType *Statement
	switch interaction {
	case "create":
		params = append(params, Id("resource").Qual(moduleName+"/model", "Resource"))
		shortcutParams = append(shortcutParams, Id("resource"))
		passParams = append(passParams, Id("r"))
		switchType = Id("r").Op(":=").Id("resource").Assert(Type())
		returnType = Qual(moduleName+"/model", "Resource")
	case "read":
		params = append(params, Id("resourceType").String(), Id("id").String())
		shortcutParams = append(shortcutParams, Id("resourceType"), Id("id"))
		passParams = append(passParams, Id("id"))
		returnType = Qual(moduleName+"/model", "Resource")
	case "update":
		params = append(params, Id("resource").Qual(moduleName+"/model", "Resource"))
		shortcutParams = append(shortcutParams, Id("resource"))
		passParams = append(passParams, Id("r"))
		switchType = Id("r").Op(":=").Id("resource").Assert(Type())
		returnType = Qual(moduleName+"/capabilities/update", "Result").Index(Qual(moduleName+"/model", "Resource"))
	case "delete":
		params = append(params, Id("resourceType").String(), Id("id").String())
		shortcutParams = append(shortcutParams, Id("resourceType"), Id("id"))
		passParams = append(passParams, Id("id"))
	case "search":
		params = append(params, Id("resourceType").String(), Id("parameters").Qual(moduleName+"/capabilities/search", "Parameters"), Id("options").Qual(moduleName+"/capabilities/search", "Options"))
		shortcutParams = append(shortcutParams, Id("resourceType"), Id("parameters"), Id("options"))
		passParams = append(passParams, Id("parameters"), Id("options"))
		returnType = Qual(moduleName+"/capabilities/search", "Result").Index(Qual(moduleName+"/model", "Resource"))
	}

	var returns []Code
	if returnType != nil {
		returns = append(returns, returnType)
	}
	returns = append(returns, Error())

	f.Func().Params(Id("w").Id(genericWrapperName)).Id(interactionName).
		Params(params...).
		Params(returns...).
		BlockFunc(func(g *Group) {
			g.List(Id("g"), Id("ok")).Op(":=").Id("w.Concrete").Assert(Qual(moduleName+"/capabilities", "Generic"+interactionName))
			g.If(Id("ok")).Block(
				Comment("// shortcut for the case that the underlying implementation already implements the generic API"),
				Return(Id("g."+interactionName).Params(shortcutParams...)),
			)

			g.Switch(switchType).BlockFunc(func(g *Group) {
				switch interaction {
				case "create":
					for _, r := range resources {
						g.Case(Qual(moduleName+"/model/gen/"+strings.ToLower(release), r.Name)).BlockFunc(func(g *Group) {
							g.List(Id("impl"), Id("ok")).Op(":=").Id("w.Concrete").Assert(Id(r.Name + interactionName))
							g.If(Op("!").Id("ok")).Block(Return(Nil(), notImplementedError(release, interaction, r.Name)))
							g.Return(Id("impl." + interactionName + r.Name).Call(passParams...))
						})
					}

					g.Default().Block(Return(Nil(), invalidResourceTypeError(release, Id("resource").Dot("ResourceType").Call())))
				case "read":
					for _, r := range resources {
						g.Case(Lit(r.Name)).BlockFunc(func(g *Group) {
							if r.Name == "SearchParameter" {
								// Special handling for SearchParameter - fallback to SearchCapabilities methods
								g.List(Id("impl"), Id("ok")).Op(":=").Id("w.Concrete").Assert(Id(r.Name + interactionName))
								g.If(Id("ok")).Block(
									Return(Id("impl." + interactionName + r.Name).Call(passParams...)),
								)

								// Fallback: gather SearchParameter from SearchCapabilities methods
								g.Comment("// Fallback: gather SearchParameter from SearchCapabilities methods if ReadSearchParameter not implemented")
								g.Comment("// Get base URL from CapabilityStatement for canonical references")
								g.List(Id("cs"), Id("err")).Op(":=").Id("w").Dot("Concrete").Dot("CapabilityBase").Call(Id("ctx"))
								g.If(Id("err").Op("!=").Nil()).Block(
									Return(Nil(), Id("err")),
								)
								g.Var().Id("baseUrl").String()
								g.If(Id("cs.Implementation").Op("!=").Nil().Op("&&").Id("cs.Implementation.Url").Op("!=").Nil().Op("&&").Id("cs.Implementation.Url.Value").Op("!=").Nil()).Block(
									Id("baseUrl").Op("=").Op("*").Id("cs.Implementation.Url.Value"),
								)
								g.List(Id("searchParameters"), Id("err")).Op(":=").Id("searchParameters").Call(Id("ctx"), Id("w").Dot("Concrete"), Id("baseUrl"))
								g.If(Id("err").Op("!=").Nil()).Block(
									Return(Nil(), Id("err")),
								)

								g.List(Id("searchParam"), Id("exists")).Op(":=").Id("searchParameters").Index(Id("id"))
								g.If(Id("exists")).Block(
									Return(Id("searchParam"), Nil()),
								)

								g.Return(Nil(), notFoundError(release, r.Name, Id("id")))
							} else if r.Name == "OperationDefinition" {
								// Special handling for OperationDefinition - fallback to generated definitions with canonical url
								g.List(Id("impl"), Id("ok")).Op(":=").Id("w.Concrete").Assert(Id(r.Name + interactionName))
								g.If(Id("ok")).Block(
									Return(Id("impl." + interactionName + r.Name).Call(passParams...)),
								)

								// Fallback: get baseUrl and resolve populated OperationDefinitions
								g.List(Id("cs"), Id("err")).Op(":=").Id("w").Dot("Concrete").Dot("CapabilityBase").Call(Id("ctx"))
								g.If(Id("err").Op("!=").Nil()).Block(
									Return(Nil(), Id("err")),
								)
								g.Var().Id("baseUrl").String()
								g.If(Id("cs.Implementation").Op("!=").Nil().Op("&&").Id("cs.Implementation.Url").Op("!=").Nil().Op("&&").Id("cs.Implementation.Url.Value").Op("!=").Nil()).Block(
									Id("baseUrl").Op("=").Op("*").Id("cs.Implementation.Url.Value"),
								)
								g.List(Id("defs"), Id("err")).Op(":=").Id("operationDefinitionsByID").Call(Id("ctx"), Id("w.Concrete"), Id("baseUrl"))
								g.If(Id("err").Op("!=").Nil()).Block(
									Return(Nil(), Id("err")),
								)
								g.If(List(Id("od"), Id("exists")).Op(":=").Id("defs").Index(Id("id")), Id("exists")).Block(
									Return(Id("od"), Nil()),
								)
								g.Return(Nil(), notFoundError(release, r.Name, Id("id")))
							} else {
								g.List(Id("impl"), Id("ok")).Op(":=").Id("w.Concrete").Assert(Id(r.Name + interactionName))
								g.If(Op("!").Id("ok")).Block(Return(Nil(), notImplementedError(release, interaction, r.Name)))
								g.Return(Id("impl." + interactionName + r.Name).Call(passParams...))
							}
						})
					}

					g.Default().Block(Return(Nil(), invalidResourceTypeError(release, Id("resourceType"))))
				case "update":
					for _, r := range resources {
						g.Case(Qual(moduleName+"/model/gen/"+strings.ToLower(release), r.Name)).BlockFunc(func(g *Group) {
							g.List(Id("impl"), Id("ok")).Op(":=").Id("w.Concrete").Assert(Id(r.Name + interactionName))
							g.If(Op("!").Id("ok")).Block(
								Return(returnType.Clone().Block(), notImplementedError(release, interaction, r.Name)),
							)
							g.List(Id("result"), Id("err")).Op(":=").Id("impl." + interactionName + r.Name).Call(passParams...)
							g.If(Id("err").Op("!=").Nil()).Block(
								Return(returnType.Clone().Block(), Id("err")),
							)
							g.Return(
								returnType.Clone().Block(Dict{
									Id("Resource"): Id("result").Dot("Resource"),
									Id("Created"):  Id("result").Dot("Created"),
								}),
								Nil(),
							)
						})
					}

					g.Default().Block(Return(returnType.Clone().Block(), invalidResourceTypeError(release, Id("resource").Dot("ResourceType").Call())))
				case "delete":
					for _, r := range resources {
						g.Case(Lit(r.Name)).BlockFunc(func(g *Group) {
							g.List(Id("impl"), Id("ok")).Op(":=").Id("w.Concrete").Assert(Id(r.Name + interactionName))
							g.If(Op("!").Id("ok")).Block(Return(notImplementedError(release, interaction, r.Name)))
							g.Return(Id("impl." + interactionName + r.Name).Call(passParams...))
						})
					}

					g.Default().Block(Return(invalidResourceTypeError(release, Id("resourceType"))))
				case "search":
					for _, r := range resources {
						g.Case(Lit(r.Name)).BlockFunc(func(g *Group) {
							if r.Name == "SearchParameter" {
								// Special handling for SearchParameter - fallback to searchParameters method
								g.List(Id("impl"), Id("ok")).Op(":=").Id("w.Concrete").Assert(Id(r.Name + interactionName))
								g.If(Id("ok")).Block(
									List(Id("result"), Id("err")).Op(":=").Id("impl."+interactionName+r.Name).Call(passParams...),
									If(Id("err").Op("!=").Nil()).Block(Return(returnType.Clone().Block(), Id("err"))),

									Id("genericResources").Op(":=").Make(Index().Qual(moduleName+"/model", "Resource"), Len(Id("result.Resources"))),
									For(List(Id("i"), Id("r")).Op(":=").Range().Id("result.Resources")).Block(
										Id("genericResources").Index(Id("i")).Op("=").Id("r"),
									),

									Return(returnType.Clone().Block(Dict{
										Id("Resources"): Id("genericResources"),
										Id("Included"):  Id("result").Dot("Included"),
										Id("Next"):      Id("result").Dot("Next"),
									}), Nil()),
								)

								// Fallback: gather SearchParameter from SearchCapabilities methods
								g.Comment("// Fallback: gather SearchParameter from SearchCapabilities methods if SearchSearchParameter not implemented")
								g.Comment("// Get base URL from CapabilityStatement for canonical references")
								g.List(Id("cs"), Id("err")).Op(":=").Id("w").Dot("Concrete").Dot("CapabilityBase").Call(Id("ctx"))
								g.If(Id("err").Op("!=").Nil()).Block(
									Return(returnType.Clone().Block(), Id("err")),
								)
								g.Var().Id("baseUrl").String()
								g.If(Id("cs.Implementation").Op("!=").Nil().Op("&&").Id("cs.Implementation.Url").Op("!=").Nil().Op("&&").Id("cs.Implementation.Url.Value").Op("!=").Nil()).Block(
									Id("baseUrl").Op("=").Op("*").Id("cs.Implementation.Url.Value"),
								)
								g.List(Id("searchParameters"), Id("err")).Op(":=").Id("searchParameters").Call(Id("ctx"), Id("w").Dot("Concrete"), Id("baseUrl"))
								g.If(Id("err").Op("!=").Nil()).Block(
									Return(returnType.Clone().Block(), Id("err")),
								)

								// Filter search results based on search options (support _id parameter)
								g.Id("filteredParameters").Op(":=").Make(Map(String()).Qual(moduleName+"/model/gen/"+strings.ToLower(release), "SearchParameter"))
								g.For(List(Id("id"), Id("searchParam")).Op(":=").Range().Id("searchParameters")).Block(
									Id("filteredParameters").Index(Id("id")).Op("=").Id("searchParam"),
								)

								// Check for _id parameter in search options
								g.If(List(Id("idParams"), Id("ok")).Op(":=").Id("parameters.Map()").Index(Qual(moduleName+"/capabilities/search", "ParameterKey").Values(Dict{Id("Name"): Lit("_id")})), Id("ok")).Block(
									Id("filteredParameters").Op("=").Make(Map(String()).Qual(moduleName+"/model/gen/"+strings.ToLower(release), "SearchParameter")),
									For(List(Id("_"), Id("idValues")).Op(":=").Range().Id("idParams")).Block(
										For(List(Id("_"), Id("idValue")).Op(":=").Range().Id("idValues")).Block(
											Id("idStr").Op(":=").Id("idValue").Dot("String").Call(),
											If(List(Id("searchParam"), Id("exists")).Op(":=").Id("searchParameters").Index(Id("idStr")), Id("exists")).Block(
												Id("filteredParameters").Index(Id("idStr")).Op("=").Id("searchParam"),
											),
										),
									),
								)

								// Convert map to slice for search result with deterministic ordering
								g.Comment("Sort IDs for deterministic ordering")
								g.Id("sortedIds").Op(":=").Make(Index().String(), Lit(0), Len(Id("filteredParameters")))
								g.For(List(Id("id"), Id("_")).Op(":=").Range().Id("filteredParameters")).Block(
									Id("sortedIds").Op("=").Append(Id("sortedIds"), Id("id")),
								)
								g.Qual("sort", "Strings").Call(Id("sortedIds"))

								g.Id("allResources").Op(":=").Make(Index().Qual(moduleName+"/model", "Resource"), Lit(0), Len(Id("filteredParameters")))
								g.For(List(Id("_"), Id("id")).Op(":=").Range().Id("sortedIds")).Block(
									Id("allResources").Op("=").Append(Id("allResources"), Id("filteredParameters").Index(Id("id"))),
								)

								// Apply cursor-based pagination (offset-based)
								g.Var().Id("offset").Int()
								g.Id("opts").Op(":=").Id("options")
								g.If(Id("opts.Cursor").Op("!=").Lit("")).Block(
									List(Id("parsedOffset"), Id("err")).Op(":=").Qual("strconv", "Atoi").Call(String().Call(Id("opts.Cursor"))),
									If(Id("err").Op("!=").Nil()).Block(
										Return(returnType.Clone().Block(), Qual("fmt", "Errorf").Call(Lit("invalid cursor: %w"), Id("err"))),
									),
									If(Id("parsedOffset").Op("<").Lit(0)).Block(
										Return(returnType.Clone().Block(), Qual("fmt", "Errorf").Call(Lit("invalid cursor: offset must be non-negative"))),
									),
									Id("offset").Op("=").Id("parsedOffset"),
								)

								// Apply offset
								g.Var().Id("resources").Index().Qual(moduleName+"/model", "Resource")
								g.If(Id("offset").Op("<").Len(Id("allResources"))).Block(
									Id("resources").Op("=").Id("allResources").Index(Id("offset").Op(":")),
								)

								// Apply count limit and determine next cursor
								g.Var().Id("nextCursor").Qual(moduleName+"/capabilities/search", "Cursor")
								g.If(Id("opts.Count").Op(">").Lit(0).Op("&&").Len(Id("resources")).Op(">").Id("opts.Count")).Block(
									Id("resources").Op("=").Id("resources").Index(Op(":").Id("opts.Count")),
									Id("nextOffset").Op(":=").Id("offset").Op("+").Id("opts.Count"),
									If(Id("nextOffset").Op("<").Len(Id("allResources"))).Block(
										Id("nextCursor").Op("=").Qual(moduleName+"/capabilities/search", "Cursor").Call(Qual("strconv", "Itoa").Call(Id("nextOffset"))),
									),
								)

								g.Return(returnType.Clone().Block(Dict{
									Id("Resources"): Id("resources"),
									Id("Included"):  Index().Qual(moduleName+"/model", "Resource").Values(),
									Id("Next"):      Id("nextCursor"),
								}), Nil())
							} else if r.Name == "OperationDefinition" {
								// Special handling for OperationDefinition - fallback to populated definitions (canonical URL) with same flow as SearchParameter
								g.List(Id("impl"), Id("ok")).Op(":=").Id("w.Concrete").Assert(Id(r.Name + interactionName))
								g.If(Id("ok")).Block(
									List(Id("result"), Id("err")).Op(":=").Id("impl."+interactionName+r.Name).Call(passParams...),
									If(Id("err").Op("!=").Nil()).Block(Return(returnType.Clone().Block(), Id("err"))),

									Id("genericResources").Op(":=").Make(Index().Qual(moduleName+"/model", "Resource"), Len(Id("result.Resources"))),
									For(List(Id("i"), Id("r")).Op(":=").Range().Id("result.Resources")).Block(
										Id("genericResources").Index(Id("i")).Op("=").Id("r"),
									),

									Return(returnType.Clone().Block(Dict{
										Id("Resources"): Id("genericResources"),
										Id("Included"):  Id("result").Dot("Included"),
										Id("Next"):      Id("result").Dot("Next"),
									}), Nil()),
								)

								// Fallback: Get base URL and resolve populated OperationDefinitions
								g.List(Id("cs"), Id("err")).Op(":=").Id("w").Dot("Concrete").Dot("CapabilityBase").Call(Id("ctx"))
								g.If(Id("err").Op("!=").Nil()).Block(
									Return(returnType.Clone().Block(), Id("err")),
								)
								g.Var().Id("baseUrl").String()
								g.If(Id("cs.Implementation").Op("!=").Nil().Op("&&").Id("cs.Implementation.Url").Op("!=").Nil().Op("&&").Id("cs.Implementation.Url.Value").Op("!=").Nil()).Block(
									Id("baseUrl").Op("=").Op("*").Id("cs.Implementation.Url.Value"),
								)
								g.List(Id("defs"), Id("err")).Op(":=").Id("operationDefinitionsByID").Call(Id("ctx"), Id("w.Concrete"), Id("baseUrl"))
								g.If(Id("err").Op("!=").Nil()).Block(
									Return(returnType.Clone().Block(), Id("err")),
								)

								// Filter based on _id similar to SearchParameter
								g.Id("filtered").Op(":=").Make(Map(String()).Qual(moduleName+"/model/gen/"+strings.ToLower(release), "OperationDefinition"))
								g.For(List(Id("id"), Id("od")).Op(":=").Range().Id("defs")).Block(
									Id("filtered").Index(Id("id")).Op("=").Id("od"),
								)
								g.If(List(Id("idParams"), Id("ok")).Op(":=").Id("parameters.Map()").Index(Qual(moduleName+"/capabilities/search", "ParameterKey").Values(Dict{Id("Name"): Lit("_id")})), Id("ok")).Block(
									Id("filtered").Op("=").Make(Map(String()).Qual(moduleName+"/model/gen/"+strings.ToLower(release), "OperationDefinition")),
									For(List(Id("_"), Id("idValues")).Op(":=").Range().Id("idParams")).Block(
										For(List(Id("_"), Id("idValue")).Op(":=").Range().Id("idValues")).Block(
											Id("idStr").Op(":=").Id("idValue").Dot("String").Call(),
											If(List(Id("od"), Id("exists")).Op(":=").Id("defs").Index(Id("idStr")), Id("exists")).Block(
												Id("filtered").Index(Id("idStr")).Op("=").Id("od"),
											),
										),
									),
								)

								// Sort IDs for deterministic ordering
								g.Id("sortedIds").Op(":=").Make(Index().String(), Lit(0), Len(Id("filtered")))
								g.For(List(Id("id"), Id("_")).Op(":=").Range().Id("filtered")).Block(
									Id("sortedIds").Op("=").Append(Id("sortedIds"), Id("id")),
								)
								g.Qual("sort", "Strings").Call(Id("sortedIds"))

								// Build results and apply pagination like SearchParameter fallback
								g.Id("allResources").Op(":=").Make(Index().Qual(moduleName+"/model", "Resource"), Lit(0), Len(Id("filtered")))
								g.For(List(Id("_"), Id("id")).Op(":=").Range().Id("sortedIds")).Block(
									Id("allResources").Op("=").Append(Id("allResources"), Id("filtered").Index(Id("id"))),
								)
								g.Var().Id("offset").Int()
								g.Id("opts").Op(":=").Id("options")
								g.If(Id("opts.Cursor").Op("!=").Lit("")).Block(
									List(Id("parsedOffset"), Id("err")).Op(":=").Qual("strconv", "Atoi").Call(String().Call(Id("opts.Cursor"))),
									If(Id("err").Op("!=").Nil()).Block(
										Return(returnType.Clone().Block(), Qual("fmt", "Errorf").Call(Lit("invalid cursor: %w"), Id("err"))),
									),
									If(Id("parsedOffset").Op("<").Lit(0)).Block(
										Return(returnType.Clone().Block(), Qual("fmt", "Errorf").Call(Lit("invalid cursor: offset must be non-negative"))),
									),
									Id("offset").Op("=").Id("parsedOffset"),
								)
								g.Var().Id("resources").Index().Qual(moduleName+"/model", "Resource")
								g.If(Id("offset").Op("<").Len(Id("allResources"))).Block(
									Id("resources").Op("=").Id("allResources").Index(Id("offset").Op(":")),
								)
								g.Var().Id("nextCursor").Qual(moduleName+"/capabilities/search", "Cursor")
								g.If(Id("opts.Count").Op(">").Lit(0).Op("&&").Len(Id("resources")).Op(">").Id("opts.Count")).Block(
									Id("resources").Op("=").Id("resources").Index(Op(":").Id("opts.Count")),
									Id("nextOffset").Op(":=").Id("offset").Op("+").Id("opts.Count"),
									If(Id("nextOffset").Op("<").Len(Id("allResources")).Block(
										Id("nextCursor").Op("=").Qual(moduleName+"/capabilities/search", "Cursor").Call(Qual("strconv", "Itoa").Call(Id("nextOffset"))),
									)),
								)
								g.Return(returnType.Clone().Block(Dict{
									Id("Resources"): Id("resources"),
									Id("Included"):  Index().Qual(moduleName+"/model", "Resource").Values(),
									Id("Next"):      Id("nextCursor"),
								}), Nil())
							} else {
								g.List(Id("impl"), Id("ok")).Op(":=").Id("w.Concrete").Assert(Id(r.Name + interactionName))
								g.If(Op("!").Id("ok")).Block(Return(returnType.Clone().Block(), notImplementedError(release, interaction, r.Name)))
								g.List(Id("result"), Id("err")).Op(":=").Id("impl." + interactionName + r.Name).Call(passParams...)
								g.If(Id("err").Op("!=").Nil()).Block(Return(returnType.Clone().Block(), Id("err")))

								g.Id("genericResources").Op(":=").Make(Index().Qual(moduleName+"/model", "Resource"), Len(Id("result.Resources")))
								g.For(List(Id("i"), Id("r")).Op(":=").Range().Id("result.Resources")).Block(
									Id("genericResources").Index(Id("i")).Op("=").Id("r"),
								)

								g.Return(returnType.Clone().Block(Dict{
									Id("Resources"): Id("genericResources"),
									Id("Included"):  Id("result").Dot("Included"),
									Id("Next"):      Id("result").Dot("Next"),
								}), Nil())
							}
						})
					}

					g.Default().Block(Return(
						returnType.Clone().Block(),
						invalidResourceTypeError(release, Id("resourceType")),
					))
				}

			})
		})
}

func generateWrapperCapabilityStatement(f *File, release string, resources []ir.ResourceOrType) {
	f.Func().Params(Id("w").Id(genericWrapperName)).Id("CapabilityStatement").
		Params(
			Id("ctx").Qual("context", "Context"),
		).
		Params(
			Qual(moduleName+"/model", "CapabilityStatement"),
			Error(),
		).
		BlockFunc(func(g *Group) {
			// Check if concrete implementation also implements GenericCapabilities for shortcut
			g.List(Id("gen"), Id("ok")).Op(":=").Id("w.Concrete").Assert(Qual(moduleName+"/capabilities", "GenericCapabilities"))
			g.If(Id("ok")).Block(
				Comment("// shortcut for the case that the underlying implementation already implements the generic API"),
				Return(Id("gen.CapabilityStatement").Params(Id("ctx"))),
			)

			// Generate CapabilityStatement from ConcreteCapabilities
			g.Comment("// Generate CapabilityStatement from concrete implementation")
			g.List(Id("baseCapabilityStatement"), Id("err")).Op(":=").Id("w.Concrete.CapabilityBase").Call(Id("ctx"))
			g.If(Id("err").Op("!=").Nil()).Block(
				Return(Nil(), Id("err")),
			)

			// Extract and validate base URL for canonical references
			g.Var().Id("baseUrl").String()
			g.If(Id("baseCapabilityStatement.Implementation").Op("==").Nil().Op("||").Id("baseCapabilityStatement.Implementation.Url").Op("==").Nil().Op("||").Id("baseCapabilityStatement.Implementation.Url.Value").Op("==").Nil()).Block(
				Return(Qual(moduleName+"/model/gen/"+strings.ToLower(release), "CapabilityStatement").Values(), Qual("fmt", "Errorf").Call(Lit("base CapabilityStatement must have implementation.url set for canonical SearchParameter references"))),
			)
			g.Id("baseUrl").Op("=").Op("*").Id("baseCapabilityStatement.Implementation.Url.Value")

			// Initialize resourcesMap from base CapabilityStatement
			g.Id("resourcesMap").Op(":=").Make(Map(String()).Qual(moduleName+"/model/gen/"+strings.ToLower(release), "CapabilityStatementRestResource"))
			g.For(List(Id("_"), Id("rest")).Op(":=").Range().Id("baseCapabilityStatement.Rest")).Block(
				For(List(Id("_"), Id("resource")).Op(":=").Range().Id("rest.Resource")).Block(
					If(Id("resource.Type.Value").Op("!=").Nil()).Block(
						Id("resourcesMap").Index(Op("*").Id("resource.Type.Value")).Op("=").Id("resource"),
					),
				),
			)

			g.Var().Id("errs").Index().Error()

			// Helper function to add interactions
			g.Id("addInteraction").Op(":=").Func().Params(
				Id("name").String(),
				Id("interactionCode").String(),
			).Params(
				Qual(moduleName+"/model/gen/"+strings.ToLower(release), "CapabilityStatementRestResource"),
			).Block(
				List(Id("r"), Id("ok")).Op(":=").Id("resourcesMap").Index(Id("name")),
				If(Op("!").Id("ok")).Block(
					Id("r").Op("=").Qual(moduleName+"/model/gen/"+strings.ToLower(release), "CapabilityStatementRestResource").Values(Dict{
						Id("Type"): Qual(moduleName+"/model/gen/"+strings.ToLower(release), "Code").Values(Dict{Id("Value"): Op("&").Id("name")}),
					}),
				),
				Id("r").Dot("Interaction").Op("=").Append(
					Id("r").Dot("Interaction"),
					Qual(moduleName+"/model/gen/"+strings.ToLower(release), "CapabilityStatementRestResourceInteraction").Values(Dict{
						Id("Code"): Qual(moduleName+"/model/gen/"+strings.ToLower(release), "Code").Values(Dict{Id("Value"): Qual(moduleName+"/utils/ptr", "To").Call(Id("interactionCode"))}),
					}),
				),
				Return(Id("r")),
			)

			for _, r := range resources {
				for _, interaction := range []string{"create", "read", "delete"} {
					interactionName := strcase.ToCamel(interaction)

					g.If(
						List(Id("_"), Id("ok")).Op(":=").Id("w.Concrete").Dot("").Call(
							Id(r.Name+interactionName),
						),
						Id("ok"),
					).Block(
						Id("resourcesMap").Index(Lit(r.Name)).Op("=").Id("addInteraction").Call(Lit(r.Name), Lit(interaction)),
					)
				}

				// Update
				g.If(
					List(Id("c"), Id("ok")).Op(":=").Id("w.Concrete").Dot("").Call(
						Id(r.Name+"Update"),
					),
					Id("ok"),
				).Block(
					Id("r").Op(":=").Id("addInteraction").Call(Lit(r.Name), Lit("update")),
					List(Id("c"), Id("ok")).Op(":=").Id("c").Dot("").Call(Id(r.Name+"UpdateCapabilities")),
					If(Id("ok")).Block(
						List(Id("c"), Err()).Op(":=").Id("c").Dot("UpdateCapabilities"+r.Name).Call(Id("ctx")),
						If(Err().Op("!=").Nil()).Block(
							Id("errs").Op("=").Append(Id("errs"), Err()),
						).Else().Block(
							Id("r").Dot("UpdateCreate").Op("=").Op("&").Qual(moduleName+"/model/gen/"+strings.ToLower(release), "Boolean").Values(Dict{
								Id("Value"): Qual(moduleName+"/utils/ptr", "To").Call(Id("c").Dot("UpdateCreate")),
							}),
						),
					),
					Id("resourcesMap").Index(Lit(r.Name)).Op("=").Id("r"),
				)

				// Search
				g.If(
					List(Id("c"), Id("ok")).Op(":=").Id("w.Concrete").Dot("").Call(
						Id(r.Name+"Search"),
					),
					Id("ok"),
				).Block(
					List(Id("c"), Err()).Op(":=").Id("c").Dot("SearchCapabilities"+r.Name).Call(Id("ctx")),
					If(Err().Op("!=").Nil()).Block(
						Id("errs").Op("=").Append(Id("errs"), Err()),
					).Else().Block(
						Id("r").Op(":=").Id("addInteraction").Call(Lit(r.Name), Lit("search-type")),

						// Add includes
						For(List(Id("_"), Id("include")).Op(":=").Range().Id("c").Dot("Includes")).Block(
							Id("r").Dot("SearchInclude").Op("=").Append(
								Id("r").Dot("SearchInclude"),
								Qual(moduleName+"/model/gen/"+strings.ToLower(release), "String").Values(Dict{Id("Value"): Op("&").Id("include")}),
							),
						),

						// Add search parameters
						For(List(Id("n"), Id("p")).Op(":=").Range().Id("c").Dot("Parameters")).Block(
							List(Id("fhirpathType"), Id("ok"), Id("err")).Op(":=").Qual(moduleName+"/fhirpath", "Singleton").Index(Qual(moduleName+"/fhirpath", "String")).Call(Id("p").Dot("Children").Call(Lit("type"))),
							If(Op("!").Id("ok").Op("||").Id("err").Op("!=").Nil()).Block(
								Continue(),
							),
							Id("resolvedType").Op(":=").String().Call(Id("fhirpathType")),

							// Extract SearchParameter ID for canonical reference
							Var().Id("definition").Op("*").Qual(moduleName+"/model/gen/"+strings.ToLower(release), "Canonical"),
							If(Id("baseUrl").Op("!=").Lit("")).Block(
								Id("searchParameterId").Op(":=").Lit(""),
								List(Id("fhirpathId"), Id("idOk"), Id("idErr")).Op(":=").Qual(moduleName+"/fhirpath", "Singleton").Index(Qual(moduleName+"/fhirpath", "String")).Call(Id("p").Dot("Children").Call(Lit("id"))),
								If(Id("idOk").Op("&&").Id("idErr").Op("==").Nil()).Block(
									Id("searchParameterId").Op("=").String().Call(Id("fhirpathId")),
								).Else().Block(
									Comment("// If no explicit ID is set, create one of pattern {resourceType}-{name}"),
									Id("searchParameterId").Op("=").Id("sanitizeIdentifier").Call(Lit(r.Name+"-").Op("+").Id("n")),
								),
								Id("canonicalUrl").Op(":=").Id("baseUrl").Op("+").Lit("/SearchParameter/").Op("+").Id("searchParameterId"),
								Id("definition").Op("=").Op("&").Qual(moduleName+"/model/gen/"+strings.ToLower(release), "Canonical").Values(Dict{Id("Value"): Op("&").Id("canonicalUrl")}),
							),

							Id("r").Dot("SearchParam").Op("=").Append(
								Id("r").Dot("SearchParam"),
								Qual(moduleName+"/model/gen/"+strings.ToLower(release), "CapabilityStatementRestResourceSearchParam").Values(Dict{
									Id("Name"):       Qual(moduleName+"/model/gen/"+strings.ToLower(release), "String").Values(Dict{Id("Value"): Op("&").Id("n")}),
									Id("Type"):       Qual(moduleName+"/model/gen/"+strings.ToLower(release), "Code").Values(Dict{Id("Value"): Op("&").Id("resolvedType")}),
									Id("Definition"): Id("definition"),
								}),
							),
						),

						Id("resourcesMap").Index(Lit(r.Name)).Op("=").Id("r"),
					),
				)
			}

			// Add SearchParameter read and search capabilities only if SearchParameterSearch is not implemented
			g.If(List(Id("_"), Id("ok")).Op(":=").Id("w.Concrete").Assert(Id("SearchParameterSearch")).Op(";").Op("!").Id("ok")).Block(
				// Add SearchParameter read capability
				Id("resourcesMap").Index(Lit("SearchParameter")).Op("=").Id("addInteraction").Call(Lit("SearchParameter"), Lit("read")),

				// Add SearchParameter search capability with _id parameter
				Id("spResource").Op(":=").Id("addInteraction").Call(Lit("SearchParameter"), Lit("search-type")),
				Id("idParam").Op(":=").Lit("_id"),
				Id("tokenType").Op(":=").Lit("token"),
				Id("idDefinition").Op(":=").Id("baseUrl").Op("+").Lit("/SearchParameter/SearchParameter-id"),
				Id("spResource").Dot("SearchParam").Op("=").Append(
					Id("spResource").Dot("SearchParam"),
					Qual(moduleName+"/model/gen/"+strings.ToLower(release), "CapabilityStatementRestResourceSearchParam").Values(Dict{
						Id("Name"): Qual(moduleName+"/model/gen/"+strings.ToLower(release), "String").Values(Dict{
							Id("Value"): Op("&").Id("idParam"),
						}),
						Id("Type"): Qual(moduleName+"/model/gen/"+strings.ToLower(release), "Code").Values(Dict{
							Id("Value"): Op("&").Id("tokenType"),
						}),
						Id("Definition"): Op("&").Qual(moduleName+"/model/gen/"+strings.ToLower(release), "Canonical").Values(Dict{
							Id("Value"): Op("&").Id("idDefinition"),
						}),
					}),
				),
				Id("resourcesMap").Index(Lit("SearchParameter")).Op("=").Id("spResource"),
			)

			// Check for errors before proceeding
			g.If(Len(Id("errs")).Op(">").Lit(0)).Block(
				Return(Qual(moduleName+"/model/gen/"+strings.ToLower(release), "CapabilityStatement").Values(), Qual("errors", "Join").Call(Id("errs").Op("..."))),
			)

			// Defer building resources list until after operations are attached

			// Update CapabilityStatement with detected capabilities
			g.Id("capabilityStatement").Op(":=").Id("baseCapabilityStatement")

			// Set FHIR version if not explicitly set based on concrete implementation
			var fhirVersion string
			switch release {
			case "R4":
				fhirVersion = "4.0"
			case "R4B":
				fhirVersion = "4.3"
			case "R5":
				fhirVersion = "5.0"
			default:
				fhirVersion = release // fallback to release name
			}

			g.If(Id("capabilityStatement.FhirVersion.Value").Op("==").Nil()).Block(
				Id("capabilityStatement.FhirVersion").Op("=").Qual(moduleName+"/model/gen/"+strings.ToLower(release), "Code").Values(Dict{
					Id("Value"): Qual(moduleName+"/utils/ptr", "To").Call(Lit(fhirVersion)),
				}),
			)

			// Ensure REST section exists
			g.If(Len(Id("capabilityStatement.Rest")).Op("==").Lit(0)).Block(
				Id("capabilityStatement.Rest").Op("=").Index().Qual(moduleName+"/model/gen/"+strings.ToLower(release), "CapabilityStatementRest").Values(
					Values(Dict{
						Id("Mode"): Qual(moduleName+"/model/gen/"+strings.ToLower(release), "Code").Values(Dict{Id("Value"): Qual(moduleName+"/utils/ptr", "To").Call(Lit("server"))}),
					}),
				),
			)

			// Defer assigning resources until after operation definitions are added

			// Populate operations using operationDefinitionsByCode helper
			// Note: function signature includes context, codegen will pass ctx
			g.Id("defs").Op(":=").Id("operationDefinitionsByCode").Call(Id("ctx"), Id("w.Concrete"))
			g.Var().Id("sysOps").Index().Qual(moduleName+"/model/gen/"+strings.ToLower(release), "CapabilityStatementRestResourceOperation")
			g.For(List(Id("code"), Id("entry")).Op(":=").Range().Id("defs")).BlockFunc(func(g *Group) {
				g.Id("id").Op(":=").Id("code")
				g.If(Id("entry.Def.Id").Op("!=").Nil().Op("&&").Id("entry.Def.Id.Value").Op("!=").Nil()).Block(
					Id("id").Op("=").Op("*").Id("entry.Def.Id.Value"),
				)
				g.Id("canonical").Op(":=").Id("baseUrl").Op("+").Lit("/OperationDefinition/").Op("+").Id("id")
				g.If(Id("entry.Def.System").Dot("Value").Op("!=").Nil().Op("&&").Op("*").Id("entry.Def.System").Dot("Value")).Block(
					Id("sysOps").Op("=").Append(Id("sysOps"), Qual(moduleName+"/model/gen/"+strings.ToLower(release), "CapabilityStatementRestResourceOperation").Values(Dict{
						Id("Name"):       Qual(moduleName+"/model/gen/"+strings.ToLower(release), "String").Values(Dict{Id("Value"): Op("&").Id("code")}),
						Id("Definition"): Qual(moduleName+"/model/gen/"+strings.ToLower(release), "Canonical").Values(Dict{Id("Value"): Op("&").Id("canonical")}),
					})),
				)
				g.For(List(Id("_"), Id("rt")).Op(":=").Range().Id("entry.Def.Resource")).BlockFunc(func(g *Group) {
					g.If(Id("rt").Dot("Value").Op("==").Nil()).Block(Continue())
					g.Id("rtype").Op(":=").Op("*").Id("rt").Dot("Value")
					g.List(Id("r"), Id("ok")).Op(":=").Id("resourcesMap").Index(Id("rtype"))
					g.If(Op("!").Id("ok")).Block(
						Id("r").Op("=").Qual(moduleName+"/model/gen/"+strings.ToLower(release), "CapabilityStatementRestResource").Values(Dict{
							Id("Type"): Qual(moduleName+"/model/gen/"+strings.ToLower(release), "Code").Values(Dict{Id("Value"): Op("&").Id("rtype")}),
						}),
					)
					// Append operation to resource
					g.Id("r").Dot("Operation").Op("=").Append(Id("r").Dot("Operation"), Qual(moduleName+"/model/gen/"+strings.ToLower(release), "CapabilityStatementRestResourceOperation").Values(Dict{
						Id("Name"):       Qual(moduleName+"/model/gen/"+strings.ToLower(release), "String").Values(Dict{Id("Value"): Op("&").Id("code")}),
						Id("Definition"): Qual(moduleName+"/model/gen/"+strings.ToLower(release), "Canonical").Values(Dict{Id("Value"): Op("&").Id("canonical")}),
					}))
					// Store back in map
					g.Id("resourcesMap").Index(Id("rtype")).Op("=").Id("r")
					Id("r").Dot("Operation").Op("=").Append(Id("r").Dot("Operation"), Qual(moduleName+"/model/gen/"+strings.ToLower(release), "CapabilityStatementRestResourceOperation").Values(Dict{
						Id("Name"):       Qual(moduleName+"/model/gen/"+strings.ToLower(release), "String").Values(Dict{Id("Value"): Op("&").Id("code")}),
						Id("Definition"): Qual(moduleName+"/model/gen/"+strings.ToLower(release), "Canonical").Values(Dict{Id("Value"): Op("&").Id("canonical")}),
					}))
					Id("resourcesMap").Index(Id("rtype")).Op("=").Id("r")
				})
			})
			// Sort system-level operations by name for deterministic output
			Qual("slices", "SortStableFunc").Call(
				Id("sysOps"),
				Func().Params(
					Id("a"),
					Id("b").Qual(moduleName+"/model/gen/"+strings.ToLower(release), "CapabilityStatementRestResourceOperation"),
				).Int().Block(
					Return(Qual("cmp", "Compare").Call(Op("*").Id("a").Dot("Name").Dot("Value"), Op("*").Id("b").Dot("Name").Dot("Value"))),
				),
			)
			g.Id("capabilityStatement.Rest").Index(Lit(0)).Dot("Operation").Op("=").Id("sysOps")

			// Build resourcesList from resourcesMap
			g.Id("resourcesList").Op(":=").Make(Index().Qual(moduleName+"/model/gen/"+strings.ToLower(release), "CapabilityStatementRestResource"), Lit(0), Len(Id("resourcesMap")))
			g.For(List(Id("_"), Id("r")).Op(":=").Range().Id("resourcesMap")).Block(
				// Sort search params by name
				Qual("slices", "SortStableFunc").Call(
					Id("r").Dot("SearchParam"),
					Func().Params(Id("a"), Id("b").Qual(moduleName+"/model/gen/"+strings.ToLower(release), "CapabilityStatementRestResourceSearchParam")).Int().Block(
						Return(Qual("cmp", "Compare").Call(Op("*").Id("a").Dot("Name").Dot("Value"), Op("*").Id("b").Dot("Name").Dot("Value"))),
					),
				),
				// Sort interactions in standard order: create, read, update, delete, search-type
				Qual("slices", "SortStableFunc").Call(
					Id("r").Dot("Interaction"),
					Func().Params(Id("a"), Id("b").Qual(moduleName+"/model/gen/"+strings.ToLower(release), "CapabilityStatementRestResourceInteraction")).Int().Block(
						Id("order").Op(":=").Map(String()).Int().Values(Dict{
							Lit("create"):      Lit(1),
							Lit("read"):        Lit(2),
							Lit("update"):      Lit(3),
							Lit("delete"):      Lit(4),
							Lit("search-type"): Lit(5),
						}),
						Id("aCode").Op(":=").Lit(""),
						If(Id("a").Dot("Code").Dot("Value").Op("!=").Nil()).Block(
							Id("aCode").Op("=").Op("*").Id("a").Dot("Code").Dot("Value"),
						),
						Id("bCode").Op(":=").Lit(""),
						If(Id("b").Dot("Code").Dot("Value").Op("!=").Nil()).Block(
							Id("bCode").Op("=").Op("*").Id("b").Dot("Code").Dot("Value"),
						),
						Return(Qual("cmp", "Compare").Call(Id("order").Index(Id("aCode")), Id("order").Index(Id("bCode")))),
					),
				),
				// Sort resource-level operations by name
				Qual("slices", "SortStableFunc").Call(
					Id("r").Dot("Operation"),
					Func().Params(
						Id("a"),
						Id("b").Qual(moduleName+"/model/gen/"+strings.ToLower(release), "CapabilityStatementRestResourceOperation"),
					).Int().Block(
						Return(Qual("cmp", "Compare").Call(Op("*").Id("a").Dot("Name").Dot("Value"), Op("*").Id("b").Dot("Name").Dot("Value"))),
					),
				),
				Id("resourcesList").Op("=").Append(Id("resourcesList"), Id("r")),
			)
			// Sort resources by type
			g.Qual("slices", "SortFunc").Call(
				Id("resourcesList"),
				Func().Params(Id("a"), Id("b").Qual(moduleName+"/model/gen/"+strings.ToLower(release), "CapabilityStatementRestResource")).Int().Block(
					Return(Qual("cmp", "Compare").Call(Op("*").Id("a").Dot("Type").Dot("Value"), Op("*").Id("b").Dot("Type").Dot("Value"))),
				),
			)
			g.Id("capabilityStatement.Rest").Index(Lit(0)).Dot("Resource").Op("=").Id("resourcesList")

			g.Return(Id("capabilityStatement"), Nil())
		})
}

// generateOperationDefinitionsFn emits helpers that discover concrete
// operation definition methods and return maps:
//   - by code: code -> (base name, OperationDefinition)
//   - by id:   id  -> (base name, OperationDefinition)
func generateOperationDefinitionsFn(f *File, release string) {
	entryType := Struct(
		Id("Base").String(),
		Id("Def").Qual(moduleName+"/model/gen/"+strings.ToLower(release), "OperationDefinition"),
	)
	f.Func().Id("operationDefinitionsByCode").
		Params(
			Id("ctx").Qual("context", "Context"),
			Id("api").Any(),
		).
		Params(
			Map(String()).Add(entryType),
		).
		BlockFunc(func(g *Group) {
			g.Id("defs").Op(":=").Make(Map(String()).Add(entryType))
			// reflect type for context.Context
			g.Id("ctxT").Op(":=").Qual("reflect", "TypeOf").Call(Parens(Op("*").Qual("context", "Context")).Parens(Nil())).Dot("Elem").Call()
			g.Id("t").Op(":=").Qual("reflect", "TypeOf").Call(Id("api"))
			g.Id("v").Op(":=").Qual("reflect", "ValueOf").Call(Id("api"))
			g.For(Id("i").Op(":=").Lit(0), Id("i").Op("<").Id("t").Dot("NumMethod").Call(), Id("i").Op("++")).BlockFunc(func(g *Group) {
				g.Id("m").Op(":=").Id("t").Dot("Method").Call(Id("i"))
				g.Id("name").Op(":=").Id("m").Dot("Name")
				g.If(Op("!").Qual("strings", "HasSuffix").Call(Id("name"), Lit("Definition")).Op("&&").Op("!").Qual("strings", "HasSuffix").Call(Id("name"), Lit("OperationDefinition"))).Block(Continue())
				g.Id("mv").Op(":=").Id("v").Dot("MethodByName").Call(Id("name"))
				g.Id("mt").Op(":=").Id("mv").Dot("Type").Call()
				// Allow optional context.Context argument
				g.If(Parens(Id("mt").Dot("NumIn").Call().Op("!=").Lit(0)).Op("&&").Parens(Id("mt").Dot("NumIn").Call().Op("!=").Lit(1))).Block(Continue())
				g.If(Id("mt").Dot("NumOut").Call().Op("!=").Lit(1)).Block(Continue())
				g.If(Id("mt").Dot("Out").Call(Lit(0)).Op("!=").Qual("reflect", "TypeOf").Call(Qual(moduleName+"/model/gen/"+strings.ToLower(release), "OperationDefinition").Values())).Block(Continue())
				// Build call args depending on signature
				g.Id("args").Op(":=").Index().Qual("reflect", "Value").Values()
				g.If(Id("mt").Dot("NumIn").Call().Op("==").Lit(1)).BlockFunc(func(g *Group) {
					// Verify the single argument is context.Context
					g.If(Id("mt").Dot("In").Call(Lit(0)).Op("!=").Id("ctxT")).Block(Continue())
					g.Id("args").Op("=").Append(Id("args"), Qual("reflect", "ValueOf").Call(Id("ctx")))
				})
				g.List(Id("out")).Op(":=").Id("mv").Dot("Call").Call(Id("args"))
				g.Id("od").Op(":=").Id("out").Index(Lit(0)).Dot("Interface").Call().Assert(Qual(moduleName+"/model/gen/"+strings.ToLower(release), "OperationDefinition"))
				g.Id("base").Op(":=").Id("name")
				g.Id("base").Op("=").Qual("strings", "TrimSuffix").Call(Id("base"), Lit("OperationDefinition"))
				g.Id("base").Op("=").Qual("strings", "TrimSuffix").Call(Id("base"), Lit("Definition"))
				g.Id("code").Op(":=").Qual("strings", "ToLower").Call(Id("base"))
				g.If(Id("od").Dot("Code").Dot("Value").Op("!=").Nil()).Block(
					Id("code").Op("=").Qual("strings", "ToLower").Call(Op("*").Id("od").Dot("Code").Dot("Value")),
				)
				g.Id("defs").Index(Id("code")).Op("=").Add(entryType).Values(Dict{Id("Base"): Id("base"), Id("Def"): Id("od")})
			})
			g.Return(Id("defs"))
		})

	// Index by ID (prefers explicit Id.value, falls back to code)
	f.Func().Id("operationDefinitionsByID").
		Params(
			Id("ctx").Qual("context", "Context"),
			Id("api").Any(),
			Id("baseUrl").String(),
		).
		Params(
			Map(String()).Qual(moduleName+"/model/gen/"+strings.ToLower(release), "OperationDefinition"),
			Error(),
		).
		BlockFunc(func(g *Group) {
			g.Id("defs").Op(":=").Make(Map(String()).Qual(moduleName+"/model/gen/"+strings.ToLower(release), "OperationDefinition"))
			// reflect type for context.Context
			g.Id("ctxT").Op(":=").Qual("reflect", "TypeOf").Call(Parens(Op("*").Qual("context", "Context")).Parens(Nil())).Dot("Elem").Call()
			g.Id("t").Op(":=").Qual("reflect", "TypeOf").Call(Id("api"))
			g.Id("v").Op(":=").Qual("reflect", "ValueOf").Call(Id("api"))
			g.For(Id("i").Op(":=").Lit(0), Id("i").Op("<").Id("t").Dot("NumMethod").Call(), Id("i").Op("++")).BlockFunc(func(g *Group) {
				g.Id("m").Op(":=").Id("t").Dot("Method").Call(Id("i"))
				g.Id("name").Op(":=").Id("m").Dot("Name")
				g.If(Op("!").Qual("strings", "HasSuffix").Call(Id("name"), Lit("Definition")).Op("&&").Op("!").Qual("strings", "HasSuffix").Call(Id("name"), Lit("OperationDefinition"))).Block(Continue())
				g.Id("mv").Op(":=").Id("v").Dot("MethodByName").Call(Id("name"))
				g.Id("mt").Op(":=").Id("mv").Dot("Type").Call()
				// Allow 0 or 1 argument (context.Context)
				g.If(Parens(Id("mt").Dot("NumIn").Call().Op("!=").Lit(0)).Op("&&").Parens(Id("mt").Dot("NumIn").Call().Op("!=").Lit(1))).Block(Continue())
				g.If(Id("mt").Dot("NumOut").Call().Op("!=").Lit(1)).Block(Continue())
				g.If(Id("mt").Dot("Out").Call(Lit(0)).Op("!=").Qual("reflect", "TypeOf").Call(Qual(moduleName+"/model/gen/"+strings.ToLower(release), "OperationDefinition").Values())).Block(Continue())
				// Build call args depending on signature
				g.Id("args").Op(":=").Index().Qual("reflect", "Value").Values()
				g.If(Id("mt").Dot("NumIn").Call().Op("==").Lit(1)).BlockFunc(func(g *Group) {
					g.If(Id("mt").Dot("In").Call(Lit(0)).Op("!=").Id("ctxT")).Block(Continue())
					g.Id("args").Op("=").Append(Id("args"), Qual("reflect", "ValueOf").Call(Id("ctx")))
				})
				g.List(Id("out")).Op(":=").Id("mv").Dot("Call").Call(Id("args"))
				g.Id("od").Op(":=").Id("out").Index(Lit(0)).Dot("Interface").Call().Assert(Qual(moduleName+"/model/gen/"+strings.ToLower(release), "OperationDefinition"))
				g.Id("base").Op(":=").Id("name")
				g.Id("base").Op("=").Qual("strings", "TrimSuffix").Call(Id("base"), Lit("OperationDefinition"))
				g.Id("base").Op("=").Qual("strings", "TrimSuffix").Call(Id("base"), Lit("Definition"))
				g.Id("code").Op(":=").Qual("strings", "ToLower").Call(Id("base"))
				g.If(Id("od").Dot("Code").Dot("Value").Op("!=").Nil()).Block(
					Id("code").Op("=").Qual("strings", "ToLower").Call(Op("*").Id("od").Dot("Code").Dot("Value")),
				)
				g.Id("id").Op(":=").Id("code")
				g.If(Id("od").Dot("Id").Op("!=").Nil().Op("&&").Id("od").Dot("Id").Dot("Value").Op("!=").Nil()).Block(
					Id("id").Op("=").Op("*").Id("od").Dot("Id").Dot("Value"),
				).Else().Block(
					Id("od.Id").Op("=").Op("&").Qual(moduleName+"/model/gen/"+strings.ToLower(release), "Id").Values(Dict{Id("Value"): Qual(moduleName+"/utils/ptr", "To").Call(Id("id"))}),
				)
				g.If(Id("baseUrl").Op("!=").Lit("")).Block(
					Id("canonical").Op(":=").Id("baseUrl").Op("+").Lit("/OperationDefinition/").Op("+").Id("id"),
					Id("od.Url").Op("=").Op("&").Qual(moduleName+"/model/gen/"+strings.ToLower(release), "Uri").Values(Dict{Id("Value"): Qual(moduleName+"/utils/ptr", "To").Call(Id("canonical"))}),
				)
				g.Id("defs").Index(Id("id")).Op("=").Id("od")
			})
			g.Return(Id("defs"), Nil())
		})

}

func generateConcreteWrapperStruct(f *File, release string) {
	f.Type().Id(concreteWrapperName).Struct(
		Id("Generic").Qual(moduleName+"/capabilities", "GenericCapabilities"),
	)
}

func generateConcreteCapabilityBase(f *File, release string) {
	f.Func().Params(Id("w").Id(concreteWrapperName)).Id("CapabilityBase").
		Params(Id("ctx").Qual("context", "Context")).
		Params(
			Qual(moduleName+"/model", "CapabilityStatement"),
			Error(),
		).
		Block(
			Comment("// Delegate to the generic CapabilityStatement method"),
			Return(Id("w.Generic.CapabilityStatement").Params(Id("ctx"))),
		)
}

func generateConcrete(f *File, release string, resources []ir.ResourceOrType, interaction string) {
	interactionName := strcase.ToCamel(interaction)

	for _, r := range resources {
		params := []Code{Id("ctx").Qual("context", "Context")}
		passParams := []Code{Id("ctx")}
		var returnType *Statement
		switch interaction {
		case "create":
			params = append(params, Id("resource").Qual(moduleName+"/model/gen/"+strings.ToLower(release), r.Name))
			passParams = append(passParams, Id("resource"))
			returnType = Qual(moduleName+"/model/gen/"+strings.ToLower(release), r.Name)
		case "read":
			params = append(params, Id("id").String())
			passParams = append(passParams, Lit(r.Name), Id("id"))
			returnType = Qual(moduleName+"/model/gen/"+strings.ToLower(release), r.Name)
		case "update":
			params = append(params, Id("resource").Qual(moduleName+"/model/gen/"+strings.ToLower(release), r.Name))
			passParams = append(passParams, Id("resource"))
			returnType = Qual(moduleName+"/capabilities/update", "Result").Index(Qual(moduleName+"/model/gen/"+strings.ToLower(release), r.Name))
		case "delete":
			params = append(params, Id("id").String())
			passParams = append(passParams, Lit(r.Name), Id("id"))
		case "search":
			params = append(params, Id("parameters").Qual(moduleName+"/capabilities/search", "Parameters"), Id("options").Qual(moduleName+"/capabilities/search", "Options"))
			passParams = append(passParams, Lit(r.Name), Id("parameters"), Id("options"))
			returnType = Qual(moduleName+"/capabilities/search", "Result").Index(Qual(moduleName+"/model/gen/"+strings.ToLower(release), r.Name))
		}

		var returns []Code
		if returnType != nil {
			returns = append(returns, returnType)
		}
		returns = append(returns, Error())

		f.Func().Params(Id("w").Id(concreteWrapperName)).Id(interactionName + r.Name).
			Params(params...).
			Params(returns...).
			BlockFunc(func(g *Group) {
				g.List(Id("g"), Id("ok")).Op(":=").Id("w.Generic").Assert(Qual(moduleName+"/capabilities", "Generic"+interactionName))
				g.If(Id("!ok")).BlockFunc(func(g *Group) {
					if interaction == "delete" {
						g.Return(notImplementedError(release, interactionName, r.Name))
					} else {
						g.Return(returnType.Clone().Block(), notImplementedError(release, interactionName, r.Name))
					}
				})

				switch interaction {
				case "create", "read":
					g.List(Id("v"), Id("err")).Op(":=").Id("g." + interactionName).Params(passParams...)
					g.If(Id("err").Op("!=").Nil()).Block(Return(returnType.Clone().Block(), Id("err")))

					g.List(Id("contained"), Id("ok")).Op(":=").Id("v").Assert(
						Qual(moduleName+"/model/gen/"+strings.ToLower(release), "ContainedResource"),
					)
					g.If(Id("ok")).Block(
						Id("v").Op("=").Id("contained.Resource"),
					)
					g.List(Id("r"), Id("ok")).Op(":=").Id("v").Assert(returnType)
					g.If(Id("!ok")).Block(
						Return(returnType.Clone().Block(), unexpectedResourceTypeError(release, Lit(r.Name), Id("v").Dot("ResourceType").Call())),
					)
					g.Return(Id("r"), Nil())
				case "update":
					g.List(Id("result"), Id("err")).Op(":=").Id("g." + interactionName).Params(passParams...)
					g.If(Id("err").Op("!=").Nil()).Block(Return(returnType.Clone().Block(), Id("err")))

					g.Id("v").Op(":=").Id("result.Resource")
					g.List(Id("contained"), Id("ok")).Op(":=").Id("v").Assert(
						Qual(moduleName+"/model/gen/"+strings.ToLower(release), "ContainedResource"),
					)
					g.If(Id("ok")).Block(
						Id("v").Op("=").Id("contained.Resource"),
					)
					g.List(Id("r"), Id("ok")).Op(":=").Id("v").Assert(
						Qual(moduleName+"/model/gen/"+strings.ToLower(release), r.Name),
					)
					g.If(Id("!ok")).Block(
						Return(returnType.Clone().Block(), unexpectedResourceTypeError(release, Lit(r.Name), Id("v").Dot("ResourceType").Call())),
					)
					g.Return(
						returnType.Clone().Block(Dict{
							Id("Resource"): Id("r"),
							Id("Created"):  Id("result").Dot("Created"),
						}),
						Nil(),
					)
				case "delete":
					g.Return().Id("g." + interactionName).Params(passParams...)
				case "search":
					g.List(Id("result"), Id("err")).Op(":=").Id("g." + interactionName).Params(passParams...)
					g.If(Id("err").Op("!=").Nil()).Block(Return(returnType.Clone().Block(), Id("err")))

					g.Id("resources").Op(":=").Make(Index().Qual(moduleName+"/model/gen/"+strings.ToLower(release), r.Name), Len(Id("result").Dot("Resources")))
					g.For(List(Id("i"), Id("v")).Op(":=").Range().Id("result").Dot("Resources")).BlockFunc(func(g *Group) {
						g.List(Id("contained"), Id("ok")).Op(":=").Id("v").Assert(Qual(moduleName+"/model/gen/"+strings.ToLower(release), "ContainedResource"))
						g.If(Id("ok")).Block(Id("v").Op("=").Id("contained").Dot("Resource"))
						g.List(Id("r"), Id("ok")).Op(":=").Id("v").Assert(Qual(moduleName+"/model/gen/"+strings.ToLower(release), r.Name))
						g.If(Op("!").Id("ok")).Block(Return(returnType.Clone().Block(), unexpectedResourceTypeError(release, Lit(r.Name), Id("v").Dot("ResourceType").Call())))
						g.Id("resources").Index(Id("i")).Op("=").Id("r")
					})
					g.Return(returnType.Clone().Block(Dict{
						Id("Resources"): Id("resources"),
						Id("Included"):  Id("result").Dot("Included"),
						Id("Next"):      Id("result").Dot("Next"),
					}), Nil())
				}
			})

		if slices.Contains([]string{"search", "update"}, interaction) {
			f.Add(generateConcreteCapabilities(r, release, interaction))
		}
	}
}

func generateConcreteCapabilities(r ir.ResourceOrType, release, interaction string) Code {
	interactionName := strcase.ToCamel(interaction)

	// Define the return type for the function signature
	var returnType *Statement
	if interaction == "search" {
		returnType = searchCapabilitiesType(release)
	} else {
		returnType = Qual(moduleName+"/capabilities/"+interaction, "Capabilities")
	}

	return Func().Params(Id("w").Id(concreteWrapperName)).Id(interactionName+"Capabilities"+r.Name).
		Params(Id("ctx").Qual("context", "Context")).
		Params(returnType, Error()).
		BlockFunc(func(g *Group) {
			// The logic differs for "search" and "update"
			if interaction == "search" {
				// Read capabilities from the generic CapabilityStatement method
				g.List(Id("capabilityStatement"), Id("err")).Op(":=").Id("w.Generic.CapabilityStatement").Call(Id("ctx"))
				g.If(Id("err").Op("!=").Nil()).Block(
					Return(searchCapabilitiesType(release).Values(Dict{
						Id("Includes"):   Index().String().Values(),
						Id("Parameters"): Make(Map(String()).Qual(moduleName+"/model/gen/"+strings.ToLower(release), "SearchParameter")),
					}), Id("err")),
				)
				// Cast to release-specific CapabilityStatement
				g.List(Id("cs"), Id("okCs")).Op(":=").Id("capabilityStatement").Assert(Qual(moduleName+"/model/gen/"+strings.ToLower(release), "CapabilityStatement"))
				g.If(Op("!").Id("okCs")).Block(
					Return(searchCapabilitiesType(release).Values(Dict{
						Id("Includes"):   Index().String().Values(),
						Id("Parameters"): Make(Map(String()).Qual(moduleName+"/model/gen/"+strings.ToLower(release), "SearchParameter")),
					}), Qual("fmt", "Errorf").Call(Lit("CapabilityStatement type does not match release"))),
				)

				// Find the resource in the CapabilityStatement
				g.Var().Id("searchParams").Index().Qual(moduleName+"/model/gen/"+strings.ToLower(release), "CapabilityStatementRestResourceSearchParam")
				g.For(List(Id("_"), Id("rest")).Op(":=").Range().Id("cs.Rest")).Block(
					For(List(Id("_"), Id("resource")).Op(":=").Range().Id("rest.Resource")).Block(
						If(Id("resource.Type.Value").Op("!=").Nil().Op("&&").Op("*").Id("resource.Type.Value").Op("==").Lit(r.Name)).Block(
							Id("searchParams").Op("=").Id("resource.SearchParam"),
							Break(),
						),
					),
				)

				// Initialize the parameters map
				g.Id("parameters").Op(":=").Make(Map(String()).Qual(moduleName+"/model/gen/"+strings.ToLower(release), "SearchParameter"))

				// Look up actual SearchParameter resources using the generic Read method
				g.For(List(Id("_"), Id("searchParam")).Op(":=").Range().Id("searchParams")).Block(
					If(Id("searchParam.Definition").Op("!=").Nil().Op("&&").Id("searchParam.Definition.Value").Op("!=").Nil()).Block(
						// Extract SearchParameter ID from canonical URL
						Id("canonicalUrl").Op(":=").Op("*").Id("searchParam.Definition.Value"),
						Id("lastSlash").Op(":=").Qual("strings", "LastIndex").Call(Id("canonicalUrl"), Lit("/")),
						If(Id("lastSlash").Op("!=").Lit(-1).Op("&&").Id("lastSlash").Op("<").Len(Id("canonicalUrl")).Op("-").Lit(1)).Block(
							Id("searchParamId").Op(":=").Id("canonicalUrl").Index(Id("lastSlash").Op("+").Lit(1).Op(":")),

							// Read the SearchParameter resource using generic Read
							List(Id("readImpl"), Id("readOk")).Op(":=").Id("w.Generic").Assert(Qual(moduleName+"/capabilities", "GenericRead")),
							If(Id("readOk")).Block(
								List(Id("searchParamResource"), Id("readErr")).Op(":=").Id("readImpl.Read").Call(Id("ctx"), Lit("SearchParameter"), Id("searchParamId")),
								If(Id("readErr").Op("==").Nil()).Block(
									// Type assert to SearchParameter
									List(Id("sp"), Id("ok")).Op(":=").Id("searchParamResource").Assert(Qual(moduleName+"/model/gen/"+strings.ToLower(release), "SearchParameter")),
									If(Id("ok")).Block(
										If(Id("sp.Code.Value").Op("!=").Nil()).Block(
											Id("parameters").Index(Op("*").Id("sp.Code.Value")).Op("=").Id("sp"),
										),
									),
								),
							),
						),
					),
				)

				// Return the capabilities with resolved SearchParameters
				g.Return(searchCapabilitiesType(release).Values(Dict{
					Id("Includes"):   Index().String().Values(),
					Id("Parameters"): Id("parameters"),
				}), Nil())
			} else {
				// For update capabilities, use type assertion
				g.List(Id("updateImpl"), Id("ok")).Op(":=").Id("w.Generic").Assert(Id(r.Name + "UpdateCapabilities"))
				g.If(Id("ok")).Block(
					Return(Id("updateImpl.UpdateCapabilities" + r.Name).Call(Id("ctx"))),
				)

				// If not available, return default capabilities
				g.Return(Qual(moduleName+"/capabilities/update", "Capabilities").Values(), Nil())
			}
		})
}

func searchCapabilitiesType(release string) *Statement {
	return Qual(moduleName+"/model/gen/"+strings.ToLower(release), "SearchCapabilities")
}

func notImplementedError(release, interaction, resourceType string) Code {
	r := strings.ToLower(release)
	return Qual(moduleName+"/model/gen/"+r, "OperationOutcome").Values(Dict{
		Id("Issue"): Index().Qual(moduleName+"/model/gen/"+r, "OperationOutcomeIssue").Values(
			Values(Dict{
				Id("Severity"): Qual(moduleName+"/model/gen/"+r, "Code").Values(Dict{
					Id("Value"): Qual(moduleName+"/utils/ptr", "To").Call(Lit("fatal")),
				}),
				Id("Code"): Qual(moduleName+"/model/gen/"+r, "Code").Values(Dict{
					Id("Value"): Qual(moduleName+"/utils/ptr", "To").Call(Lit("not-supported")),
				}),
				Id("Diagnostics"): Op("&").Qual(moduleName+"/model/gen/"+r, "String").Values(Dict{
					Id("Value"): Qual(moduleName+"/utils/ptr", "To").Call(Lit(interaction + " not implemented for " + resourceType)),
				}),
			}),
		),
	})
}

func notFoundError(release, resourceType string, id *Statement) Code {
	r := strings.ToLower(release)
	return Qual(moduleName+"/model/gen/"+r, "OperationOutcome").Values(Dict{
		Id("Issue"): Index().Qual(moduleName+"/model/gen/"+r, "OperationOutcomeIssue").Values(
			Values(Dict{
				Id("Severity"): Qual(moduleName+"/model/gen/"+r, "Code").Values(Dict{
					Id("Value"): Qual(moduleName+"/utils/ptr", "To").Call(Lit("error")),
				}),
				Id("Code"): Qual(moduleName+"/model/gen/"+r, "Code").Values(Dict{
					Id("Value"): Qual(moduleName+"/utils/ptr", "To").Call(Lit("not-found")),
				}),
				Id("Diagnostics"): Op("&").Qual(moduleName+"/model/gen/"+r, "String").Values(Dict{
					Id("Value"): Qual(moduleName+"/utils/ptr", "To").Call(Lit(resourceType + " with ID ").Op("+").Add(id).Op("+").Lit(" not found")),
				}),
			}),
		),
	})
}

func invalidResourceTypeError(release string, resourceType Code) Code {
	r := strings.ToLower(release)
	return Qual(moduleName+"/model/gen/"+r, "OperationOutcome").Values(Dict{
		Id("Issue"): Index().Qual(moduleName+"/model/gen/"+r, "OperationOutcomeIssue").Values(
			Values(Dict{
				Id("Severity"): Qual(moduleName+"/model/gen/"+r, "Code").Values(Dict{
					Id("Value"): Qual(moduleName+"/utils/ptr", "To").Call(Lit("fatal")),
				}),
				Id("Code"): Qual(moduleName+"/model/gen/"+r, "Code").Values(Dict{
					Id("Value"): Qual(moduleName+"/utils/ptr", "To").Call(Lit("processing")),
				}),
				Id("Diagnostics"): Op("&").Qual(moduleName+"/model/gen/"+r, "String").Values(Dict{
					Id("Value"): Qual(moduleName+"/utils/ptr", "To").Call(Lit("invalid resource type: ").Op("+").Add(resourceType)),
				}),
			}),
		),
	})
}

func unexpectedResourceTypeError(release string, expectedType, gotType Code) Code {
	r := strings.ToLower(release)
	return Qual(moduleName+"/model/gen/"+r, "OperationOutcome").Values(Dict{
		Id("Issue"): Index().Qual(moduleName+"/model/gen/"+r, "OperationOutcomeIssue").Values(
			Values(Dict{
				Id("Severity"): Qual(moduleName+"/model/gen/"+r, "Code").Values(Dict{
					Id("Value"): Qual(moduleName+"/utils/ptr", "To").Call(Lit("fatal")),
				}),
				Id("Code"): Qual(moduleName+"/model/gen/"+r, "Code").Values(Dict{
					Id("Value"): Qual(moduleName+"/utils/ptr", "To").Call(Lit("processing")),
				}),
				Id("Diagnostics"): Op("&").Qual(moduleName+"/model/gen/"+r, "String").Values(Dict{
					Id("Value"): Qual(moduleName+"/utils/ptr", "To").Call(Lit("expected ").Op("+").Add(expectedType).Op("+").Lit(" but got ").Op("+").Add(gotType)),
				}),
			}),
		),
	})
}

func generatePopulateSearchParameterFn(f *File, release string) {
	// Helper function to sanitize FHIR resource IDs
	f.Func().Id("sanitizeIdentifier").
		Params(Id("input").String()).
		String().
		BlockFunc(func(g *Group) {
			// Replace underscores with hyphens for FHIR compliance
			g.Id("result").Op(":=").Qual("strings", "ReplaceAll").Call(Id("input"), Lit("_"), Lit(""))
			g.Return(Id("result"))
		})

	f.Func().Id("populateSearchParameter").
		Params(
			Id("searchParam").Qual(moduleName+"/model/gen/"+strings.ToLower(release), "SearchParameter"),
			Id("resourceType").String(),
			Id("paramName").String(),
			Id("baseUrl").String(),
		).
		Params(
			Qual(moduleName+"/model/gen/"+strings.ToLower(release), "SearchParameter"),
		).BlockFunc(func(g *Group) {

		// Auto-generate ID if not set
		g.List(Id("_"), Id("idOk"), Id("idErr")).Op(":=").Qual(moduleName+"/fhirpath", "Singleton").Index(Qual(moduleName+"/fhirpath", "String")).Call(Id("searchParam").Dot("Children").Call(Lit("id")))
		g.If(Op("!").Id("idOk").Op("||").Id("idErr").Op("!=").Nil()).Block(
			Comment("// Set auto-generated ID using pattern {resourceType}-{name} (FHIR-compliant)"),
			Id("id").Op(":=").Id("sanitizeIdentifier").Call(Id("resourceType").Op("+").Lit("-").Op("+").Id("paramName")),
			Id("searchParam").Dot("Id").Op("=").Op("&").Qual(moduleName+"/model/gen/"+strings.ToLower(release), "Id").Values(Dict{
				Id("Value"): Qual(moduleName+"/utils/ptr", "To").Call(Id("id")),
			}),
		)

		// Set canonical URL if not set
		g.List(Id("_"), Id("urlOk"), Id("urlErr")).Op(":=").Qual(moduleName+"/fhirpath", "Singleton").Index(Qual(moduleName+"/fhirpath", "String")).Call(Id("searchParam").Dot("Children").Call(Lit("url")))
		g.If(Op("!").Id("urlOk").Op("||").Id("urlErr").Op("!=").Nil()).Block(
			Comment("// Set canonical URL using sanitized ID"),
			Id("canonicalUrl").Op(":=").Id("baseUrl").Op("+").Lit("/SearchParameter/").Op("+").Id("*searchParam").Dot("Id").Dot("Value"),
			Id("searchParam").Dot("Url").Op("=").Qual(moduleName+"/model/gen/"+strings.ToLower(release), "Uri").Values(Dict{
				Id("Value"): Qual(moduleName+"/utils/ptr", "To").Call(Id("canonicalUrl")),
			}),
		)

		// Set name if not set
		g.List(Id("_"), Id("nameOk"), Id("nameErr")).Op(":=").Qual(moduleName+"/fhirpath", "Singleton").Index(Qual(moduleName+"/fhirpath", "String")).Call(Id("searchParam").Dot("Children").Call(Lit("name")))
		g.If(Op("!").Id("nameOk").Op("||").Id("nameErr").Op("!=").Nil()).Block(
			Comment("// Set name based on parameter name"),
			Id("searchParam").Dot("Name").Op("=").Qual(moduleName+"/model/gen/"+strings.ToLower(release), "String").Values(Dict{
				Id("Value"): Qual(moduleName+"/utils/ptr", "To").Call(Id("paramName")),
			}),
		)

		// Set status if not set
		g.List(Id("_"), Id("statusOk"), Id("statusErr")).Op(":=").Qual(moduleName+"/fhirpath", "Singleton").Index(Qual(moduleName+"/fhirpath", "String")).Call(Id("searchParam").Dot("Children").Call(Lit("status")))
		g.If(Op("!").Id("statusOk").Op("||").Id("statusErr").Op("!=").Nil()).Block(
			Comment("// Set default status to active"),
			Id("searchParam").Dot("Status").Op("=").Qual(moduleName+"/model/gen/"+strings.ToLower(release), "Code").Values(Dict{
				Id("Value"): Qual(moduleName+"/utils/ptr", "To").Call(Lit("active")),
			}),
		)

		// Set code if not set
		g.List(Id("_"), Id("codeOk"), Id("codeErr")).Op(":=").Qual(moduleName+"/fhirpath", "Singleton").Index(Qual(moduleName+"/fhirpath", "String")).Call(Id("searchParam").Dot("Children").Call(Lit("code")))
		g.If(Op("!").Id("codeOk").Op("||").Id("codeErr").Op("!=").Nil()).Block(
			Comment("// Set code based on parameter name"),
			Id("searchParam").Dot("Code").Op("=").Qual(moduleName+"/model/gen/"+strings.ToLower(release), "Code").Values(Dict{
				Id("Value"): Qual(moduleName+"/utils/ptr", "To").Call(Id("paramName")),
			}),
		)

		// Set base if not set or empty
		g.Id("baseElements").Op(":=").Id("searchParam").Dot("Children").Call(Lit("base"))
		g.If(Len(Id("baseElements")).Op("==").Lit(0)).Block(
			Comment("// Set base resource type"),
			Id("searchParam").Dot("Base").Op("=").Index().Qual(moduleName+"/model/gen/"+strings.ToLower(release), "Code").Values(
				Values(Dict{
					Id("Value"): Qual(moduleName+"/utils/ptr", "To").Call(Id("resourceType")),
				}),
			),
		)

		// Set description if not set
		g.List(Id("_"), Id("descOk"), Id("descErr")).Op(":=").Qual(moduleName+"/fhirpath", "Singleton").Index(Qual(moduleName+"/fhirpath", "String")).Call(Id("searchParam").Dot("Children").Call(Lit("description")))
		g.If(Op("!").Id("descOk").Op("||").Id("descErr").Op("!=").Nil()).Block(
			Comment("// Set default description"),
			Id("description").Op(":=").Lit("Search parameter ").Op("+").Id("paramName").Op("+").Lit(" for ").Op("+").Id("resourceType").Op("+").Lit(" resource"),
			Id("searchParam").Dot("Description").Op("=").Qual(moduleName+"/model/gen/"+strings.ToLower(release), "Markdown").Values(Dict{
				Id("Value"): Qual(moduleName+"/utils/ptr", "To").Call(Id("description")),
			}),
		)

		g.Return(Id("searchParam"))
	})
}

func generateSearchParametersFn(f *File, release string, resources []ir.ResourceOrType) {
	f.Func().Id("searchParameters").
		Params(
			Id("ctx").Qual("context", "Context"),
			Id("api").Any(),
			Id("baseUrl").String(),
		).
		Params(
			Map(String()).Qual(moduleName+"/model/gen/"+strings.ToLower(release), "SearchParameter"),
			Error(),
		).BlockFunc(func(g *Group) {

		// Collections for building SearchParameters
		g.Id("searchParameters").Op(":=").Make(Map(String()).Qual(moduleName+"/model/gen/"+strings.ToLower(release), "SearchParameter"))
		g.Var().Id("errs").Index().Error()

		for _, r := range resources {
			// Search
			g.If(
				List(Id("c"), Id("ok")).Op(":=").Id("api").Dot("").Call(
					Id(r.Name+"Search"),
				),
				Id("ok"),
			).Block(
				List(Id("c"), Err()).Op(":=").Id("c").Dot("SearchCapabilities"+r.Name).Call(Id("ctx")),
				If(Err().Op("!=").Nil()).Block(
					Id("errs").Op("=").Append(Id("errs"), Err()),
				).Else().Block(
					// Store SearchParameter for aggregation, indexed by ID
					For(List(Id("n"), Id("p")).Op(":=").Range().Id("c").Dot("Parameters")).Block(
						// Use helper function to populate required fields and auto-generate ID
						Id("populatedParam").Op(":=").Id("populateSearchParameter").Call(Id("p"), Lit(r.Name), Id("n"), Id("baseUrl")),

						// Extract the final ID (either existing or auto-generated)
						List(Id("fhirpathId"), Id("ok"), Id("err")).Op(":=").Qual(moduleName+"/fhirpath", "Singleton").Index(Qual(moduleName+"/fhirpath", "String")).Call(Id("populatedParam").Dot("Children").Call(Lit("id"))),
						If(Id("ok").Op("&&").Id("err").Op("==").Nil()).Block(
							Id("searchParameters").Index(String().Call(Id("fhirpathId"))).Op("=").Id("populatedParam"),
						).Else().Block(
							Comment("// Fallback: use pattern {resourceType}-{name} if ID extraction fails"),
							Id("searchParameters").Index(Lit(r.Name).Op("+").Lit("-").Op("+").Id("n")).Op("=").Id("populatedParam"),
						),
					),
				),
			)
		}

		// Check for errors before proceeding
		g.If(Len(Id("errs")).Op(">").Lit(0)).Block(
			Return(Nil(), Qual("errors", "Join").Call(Id("errs").Op("..."))),
		)

		// Add default SearchParameter-id if SearchParameterSearch is not implemented
		g.If(List(Id("_"), Id("ok")).Op(":=").Id("api").Assert(Id("SearchParameterSearch")).Op(";").Op("!").Id("ok")).Block(
			// Create default _id SearchParameter
			Id("idParam").Op(":=").Qual(moduleName+"/model/gen/"+strings.ToLower(release), "SearchParameter").Values(Dict{
				Id("Id"): Op("&").Qual(moduleName+"/model/gen/"+strings.ToLower(release), "Id").Values(Dict{
					Id("Value"): Qual(moduleName+"/utils/ptr", "To").Call(Lit("SearchParameter-id")),
				}),
				Id("Url"): Qual(moduleName+"/model/gen/"+strings.ToLower(release), "Uri").Values(Dict{
					Id("Value"): Qual(moduleName+"/utils/ptr", "To").Call(Id("baseUrl").Op("+").Lit("/SearchParameter/SearchParameter-id")),
				}),
				Id("Name"): Qual(moduleName+"/model/gen/"+strings.ToLower(release), "String").Values(Dict{
					Id("Value"): Qual(moduleName+"/utils/ptr", "To").Call(Lit("_id")),
				}),
				Id("Status"): Qual(moduleName+"/model/gen/"+strings.ToLower(release), "Code").Values(Dict{
					Id("Value"): Qual(moduleName+"/utils/ptr", "To").Call(Lit("active")),
				}),
				Id("Description"): Qual(moduleName+"/model/gen/"+strings.ToLower(release), "Markdown").Values(Dict{
					Id("Value"): Qual(moduleName+"/utils/ptr", "To").Call(Lit("Logical id of this artifact")),
				}),
				Id("Code"): Qual(moduleName+"/model/gen/"+strings.ToLower(release), "Code").Values(Dict{
					Id("Value"): Qual(moduleName+"/utils/ptr", "To").Call(Lit("_id")),
				}),
				Id("Base"): Index().Qual(moduleName+"/model/gen/"+strings.ToLower(release), "Code").Values(
					Qual(moduleName+"/model/gen/"+strings.ToLower(release), "Code").Values(Dict{
						Id("Value"): Qual(moduleName+"/utils/ptr", "To").Call(Lit("SearchParameter")),
					}),
				),
				Id("Type"): Qual(moduleName+"/model/gen/"+strings.ToLower(release), "Code").Values(Dict{
					Id("Value"): Qual(moduleName+"/utils/ptr", "To").Call(Lit("token")),
				}),
				Id("Expression"): Op("&").Qual(moduleName+"/model/gen/"+strings.ToLower(release), "String").Values(Dict{
					Id("Value"): Qual(moduleName+"/utils/ptr", "To").Call(Lit("SearchParameter.id")),
				}),
			}),
			Id("searchParameters").Index(Lit("SearchParameter-id")).Op("=").Id("idParam"),
		)

		g.Return(Id("searchParameters"), Nil())
	})
}

// generateGenericInvokeOperation generates a reflection-based implementation of
// capabilities.GenericOperation on the generic wrapper. It looks for concrete
// methods named like Invoke{Name} or Invoke{Name}Operation and dispatches based
// on system/type/instance argument shapes.
func generateGenericInvokeOperation(f *File, release string) {
	// Ensure needed imports by referencing them
	f.ImportName("context", "context")
	f.ImportName("reflect", "reflect")
	f.ImportName("strings", "strings")

	f.Func().Params(Id("w").Id(genericWrapperName)).Id("Invoke").
		Params(
			Id("ctx").Qual("context", "Context"),
			Id("resourceType").String(),
			Id("resourceID").String(),
			Id("code").String(),
			Id("parameters").Qual(moduleName+"/model", "Parameters"),
		).
		Params(
			Qual(moduleName+"/model", "Resource"),
			Error(),
		).
		BlockFunc(func(g *Group) {
			// If parameters is a ContainedResource, unwrap
			g.List(Id("crp"), Id("okCrp")).Op(":=").Id("parameters").Assert(Qual(moduleName+"/model/gen/"+strings.ToLower(release), "ContainedResource"))
			g.If(Id("okCrp")).Block(Id("parameters").Op("=").Id("crp").Dot("Resource"))
			// Try cast to release-specific Parameters
			g.List(Id("typedParams"), Id("_")).Op(":=").Id("parameters").Assert(Qual(moduleName+"/model/gen/"+strings.ToLower(release), "Parameters"))
			// Delegate immediately if concrete implements GenericOperation
			g.List(Id("op"), Id("ok")).Op(":=").Id("w").Dot("Concrete").Assert(Qual(moduleName+"/capabilities", "GenericOperation"))
			g.If(Id("ok")).Block(
				Return(Id("op").Dot("Invoke").Call(Id("ctx"), Id("resourceType"), Id("resourceID"), Id("code"), Id("parameters"))),
			)
			g.Id("t").Op(":=").Qual("reflect", "TypeOf").Call(Id("w").Dot("Concrete"))
			g.Id("v").Op(":=").Qual("reflect", "ValueOf").Call(Id("w").Dot("Concrete"))
			// Code lookup key for definitions map (definitions are indexed by lower-case code)
			g.Id("codeKey").Op(":=").Qual("strings", "ToLower").Call(Id("code"))

			// Resolve operation via helper map {code -> (base, def)}
			g.Id("matchBase").Op(":=").Lit("")
			g.Var().Id("opDef").Qual(moduleName+"/model/gen/"+strings.ToLower(release), "OperationDefinition")
			g.Id("defs").Op(":=").Id("operationDefinitionsByCode").Call(Id("ctx"), Id("w.Concrete"))
			g.List(Id("entry"), Id("found")).Op(":=").Id("defs").Index(Id("codeKey"))
			g.If(Id("found")).Block(
				Id("matchBase").Op("=").Id("entry.Base"),
				Id("opDef").Op("=").Id("entry.Def"),
			)

			// If no matching operation definition was found, do not fall back to name matching
			g.If(Op("!").Id("found")).Block(
				Return(
					Nil(),
					Qual(moduleName+"/model/gen/"+strings.ToLower(release), "OperationOutcome").Values(Dict{
						Id("Issue"): Index().Qual(moduleName+"/model/gen/"+strings.ToLower(release), "OperationOutcomeIssue").Values(
							Values(Dict{
								Id("Severity"):    Qual(moduleName+"/model/gen/"+strings.ToLower(release), "Code").Values(Dict{Id("Value"): Qual(moduleName+"/utils/ptr", "To").Call(Lit("fatal"))}),
								Id("Code"):        Qual(moduleName+"/model/gen/"+strings.ToLower(release), "Code").Values(Dict{Id("Value"): Qual(moduleName+"/utils/ptr", "To").Call(Lit("not-supported"))}),
								Id("Diagnostics"): Op("&").Qual(moduleName+"/model/gen/"+strings.ToLower(release), "String").Values(Dict{Id("Value"): Qual(moduleName+"/utils/ptr", "To").Call(Lit("OperationDefinition not found for code "))}),
							}),
						),
					}),
				),
			)

			// Validate level using OperationDefinition if available
			g.If(Id("matchBase").Op("!=").Lit("")).BlockFunc(func(g *Group) {
				// system
				g.If(Id("resourceType").Op("==").Lit("").Op("&&").Parens(Id("opDef").Dot("System").Dot("Value").Op("==").Nil().Op("||").Op("!").Op("*").Id("opDef").Dot("System").Dot("Value"))).Block(
					Return(Nil(), Qual(moduleName+"/model/gen/"+strings.ToLower(release), "OperationOutcome").Values(Dict{
						Id("Issue"): Index().Qual(moduleName+"/model/gen/"+strings.ToLower(release), "OperationOutcomeIssue").Values(
							Values(Dict{
								Id("Severity"):    Qual(moduleName+"/model/gen/"+strings.ToLower(release), "Code").Values(Dict{Id("Value"): Qual(moduleName+"/utils/ptr", "To").Call(Lit("fatal"))}),
								Id("Code"):        Qual(moduleName+"/model/gen/"+strings.ToLower(release), "Code").Values(Dict{Id("Value"): Qual(moduleName+"/utils/ptr", "To").Call(Lit("not-supported"))}),
								Id("Diagnostics"): Op("&").Qual(moduleName+"/model/gen/"+strings.ToLower(release), "String").Values(Dict{Id("Value"): Qual(moduleName+"/utils/ptr", "To").Call(Lit("operation not allowed at system level"))}),
							}),
						),
					})),
				)
				// type
				g.If(Id("resourceType").Op("!=").Lit("").Op("&&").Id("resourceID").Op("==").Lit("").Op("&&").Parens(Id("opDef").Dot("Type").Dot("Value").Op("==").Nil().Op("||").Op("!").Op("*").Id("opDef").Dot("Type").Dot("Value"))).Block(
					Return(Nil(), Qual(moduleName+"/model/gen/"+strings.ToLower(release), "OperationOutcome").Values(Dict{
						Id("Issue"): Index().Qual(moduleName+"/model/gen/"+strings.ToLower(release), "OperationOutcomeIssue").Values(
							Values(Dict{
								Id("Severity"):    Qual(moduleName+"/model/gen/"+strings.ToLower(release), "Code").Values(Dict{Id("Value"): Qual(moduleName+"/utils/ptr", "To").Call(Lit("fatal"))}),
								Id("Code"):        Qual(moduleName+"/model/gen/"+strings.ToLower(release), "Code").Values(Dict{Id("Value"): Qual(moduleName+"/utils/ptr", "To").Call(Lit("not-supported"))}),
								Id("Diagnostics"): Op("&").Qual(moduleName+"/model/gen/"+strings.ToLower(release), "String").Values(Dict{Id("Value"): Qual(moduleName+"/utils/ptr", "To").Call(Lit("operation not allowed at type level"))}),
							}),
						),
					})),
				)
				// instance
				g.If(Id("resourceType").Op("!=").Lit("").Op("&&").Id("resourceID").Op("!=").Lit("").Op("&&").Parens(Id("opDef").Dot("Instance").Dot("Value").Op("==").Nil().Op("||").Op("!").Op("*").Id("opDef").Dot("Instance").Dot("Value"))).Block(
					Return(Nil(), Qual(moduleName+"/model/gen/"+strings.ToLower(release), "OperationOutcome").Values(Dict{
						Id("Issue"): Index().Qual(moduleName+"/model/gen/"+strings.ToLower(release), "OperationOutcomeIssue").Values(
							Values(Dict{
								Id("Severity"):    Qual(moduleName+"/model/gen/"+strings.ToLower(release), "Code").Values(Dict{Id("Value"): Qual(moduleName+"/utils/ptr", "To").Call(Lit("fatal"))}),
								Id("Code"):        Qual(moduleName+"/model/gen/"+strings.ToLower(release), "Code").Values(Dict{Id("Value"): Qual(moduleName+"/utils/ptr", "To").Call(Lit("not-supported"))}),
								Id("Diagnostics"): Op("&").Qual(moduleName+"/model/gen/"+strings.ToLower(release), "String").Values(Dict{Id("Value"): Qual(moduleName+"/utils/ptr", "To").Call(Lit("operation not allowed at instance level"))}),
							}),
						),
					})),
				)
				// type/instance allowed resource list check
				g.If(Id("resourceType").Op("!=").Lit("")).BlockFunc(func(g *Group) {
					g.Id("allowed").Op(":=").Lit(false)
					g.For(List(Id("_"), Id("rt")).Op(":=").Range().Id("opDef").Dot("Resource")).Block(
						If(Id("rt").Dot("Value").Op("!=").Nil().Op("&&").Op("*").Id("rt").Dot("Value").Op("==").Id("resourceType")).Block(
							Id("allowed").Op("=").Lit(true),
							Break(),
						),
					)
					g.If(Parens(Len(Id("opDef").Dot("Resource")).Op("!=").Lit(0)).Op("&&").Op("!").Id("allowed")).Block(
						Return(Nil(), Qual(moduleName+"/model/gen/"+strings.ToLower(release), "OperationOutcome").Values(Dict{
							Id("Issue"): Index().Qual(moduleName+"/model/gen/"+strings.ToLower(release), "OperationOutcomeIssue").Values(
								Values(Dict{
									Id("Severity"):    Qual(moduleName+"/model/gen/"+strings.ToLower(release), "Code").Values(Dict{Id("Value"): Qual(moduleName+"/utils/ptr", "To").Call(Lit("fatal"))}),
									Id("Code"):        Qual(moduleName+"/model/gen/"+strings.ToLower(release), "Code").Values(Dict{Id("Value"): Qual(moduleName+"/utils/ptr", "To").Call(Lit("not-supported"))}),
									Id("Diagnostics"): Op("&").Qual(moduleName+"/model/gen/"+strings.ToLower(release), "String").Values(Dict{Id("Value"): Qual(moduleName+"/utils/ptr", "To").Call(Lit("operation not allowed for resource type"))}),
								}),
							),
						})),
					)
				})
			})

			// Determine desired arity by level (without code string in concrete signatures)
			g.Id("need").Op(":=").Lit(2) // system: ctx, parameters
			g.If(Id("resourceType").Op("!=").Lit("")).Block(
				Id("need").Op("=").Lit(3), // type: ctx, resourceType, parameters
				If(Id("resourceID").Op("!=").Lit("")).Block(
					Id("need").Op("=").Lit(4), // instance: ctx, resourceType, resourceID, parameters
				),
			)

			// reflect types
			g.Id("ctxT").Op(":=").Qual("reflect", "TypeOf").Call(Parens(Op("*").Qual("context", "Context")).Parens(Nil())).Dot("Elem").Call()
			g.Id("paramT").Op(":=").Qual("reflect", "TypeOf").Call(Qual(moduleName+"/model/gen/"+strings.ToLower(release), "Parameters").Values())

			// Build preferred arity list: exact need first, then more specific if available
			g.Var().Id("tryList").Index().Int()
			g.If(Id("need").Op("==").Lit(2)).Block(
				Id("tryList").Op("=").Index().Int().Values(Lit(2), Lit(3), Lit(4)),
			).Else().If(Id("need").Op("==").Lit(3)).Block(
				Id("tryList").Op("=").Index().Int().Values(Lit(3), Lit(4)),
			).Else().Block(
				Id("tryList").Op("=").Index().Int().Values(Lit(4)),
			)

			// Try candidates in preference order
			g.For(List(Id("_"), Id("tryN")).Op(":=").Range().Id("tryList")).Block(
				// Iterate methods and find matching Invoke{Name}[Operation] for the matched base
				For(Id("i").Op(":=").Lit(0), Id("i").Op("<").Id("t").Dot("NumMethod").Call(), Id("i").Op("++")).Block(
					Id("m").Op(":=").Id("t").Dot("Method").Call(Id("i")),
					Id("name").Op(":=").Id("m").Dot("Name"),
					If(Op("!").Qual("strings", "HasPrefix").Call(Id("name"), Lit("Invoke"))).Block(Continue()),
					Id("base").Op(":=").Qual("strings", "TrimPrefix").Call(Id("name"), Lit("Invoke")),
					Id("base").Op("=").Qual("strings", "TrimSuffix").Call(Id("base"), Lit("Operation")),
					// Require exact base match from OperationDefinition
					If(Id("base").Op("!=").Id("matchBase")).Block(Continue()),

					// Candidate found, validate signature and call. Use bound method from value to avoid name lookup.
					Id("mv").Op(":=").Id("v").Dot("Method").Call(Id("i")),
					Id("mt").Op(":=").Id("mv").Dot("Type").Call(),
					If(Id("mt").Dot("NumIn").Call().Op("!=").Id("tryN")).Block(Continue()),
					If(Id("mt").Dot("NumOut").Call().Op("!=").Lit(2)).Block(Continue()),
					// Ensure the second return value implements error to avoid panics on assertion
					Id("errorT").Op(":=").Qual("reflect", "TypeOf").Call(Parens(Op("*").Id("error")).Parens(Nil())).Dot("Elem").Call(),
					If(Op("!").Id("mt").Dot("Out").Call(Lit(1)).Dot("Implements").Call(Id("errorT"))).Block(Continue()),
					If(Id("mt").Dot("In").Call(Lit(0)).Op("!=").Id("ctxT")).Block(Continue()),
					// Validate parameter types according to arity
					If(Id("tryN").Op("==").Lit(2)).Block(
						// system: (ctx, parameters)
						If(Id("mt").Dot("In").Call(Lit(1)).Op("!=").Id("paramT")).Block(Continue()),
					).Else().If(Id("tryN").Op("==").Lit(3)).Block(
						// type: (ctx, resourceType, parameters)
						If(Id("mt").Dot("In").Call(Lit(1)).Dot("Kind").Call().Op("!=").Qual("reflect", "String")).Block(Continue()),
						If(Id("mt").Dot("In").Call(Lit(2)).Op("!=").Id("paramT")).Block(Continue()),
					).Else().Block(
						// instance: (ctx, resourceType, resourceID, parameters)
						If(Id("mt").Dot("In").Call(Lit(1)).Dot("Kind").Call().Op("!=").Qual("reflect", "String")).Block(Continue()),
						If(Id("mt").Dot("In").Call(Lit(2)).Dot("Kind").Call().Op("!=").Qual("reflect", "String")).Block(Continue()),
						If(Id("mt").Dot("In").Call(Lit(3)).Op("!=").Id("paramT")).Block(Continue()),
					),

					// Build args (no code argument)
					Id("args").Op(":=").Index().Qual("reflect", "Value").Values(Qual("reflect", "ValueOf").Call(Id("ctx"))),
					If(Id("tryN").Op("==").Lit(2)).Block(
						Id("args").Op("=").Append(Id("args"), Qual("reflect", "ValueOf").Call(Id("typedParams"))),
					).Else().If(Id("tryN").Op("==").Lit(3)).Block(
						Id("args").Op("=").Append(Id("args"), Qual("reflect", "ValueOf").Call(Id("resourceType"))),
						Id("args").Op("=").Append(Id("args"), Qual("reflect", "ValueOf").Call(Id("typedParams"))),
					).Else().Block(
						Id("args").Op("=").Append(Id("args"), Qual("reflect", "ValueOf").Call(Id("resourceType"))),
						Id("args").Op("=").Append(Id("args"), Qual("reflect", "ValueOf").Call(Id("resourceID"))),
						Id("args").Op("=").Append(Id("args"), Qual("reflect", "ValueOf").Call(Id("typedParams"))),
					),

					// Call and map result
					List(Id("out")).Op(":=").Id("mv").Dot("Call").Call(Id("args")),
					Id("rv").Op(":=").Id("out").Index(Lit(0)).Dot("Interface").Call(),
					Id("errv").Op(":=").Id("out").Index(Lit(1)).Dot("Interface").Call(),
					If(Id("errv").Op("!=").Nil()).Block(
						Return(Nil(), Id("errv").Assert(Error())),
					),
					// Prefer unwrapping ContainedResource first
					List(Id("cr"), Id("ok")).Op(":=").Id("rv").Assert(Qual(moduleName+"/model/gen/"+strings.ToLower(release), "ContainedResource")),
					If(Id("ok")).Block(
						Return(Id("cr").Dot("Resource"), Nil()),
					),
					// Then accept plain model.Resource
					List(Id("res"), Id("ok")).Op(":=").Id("rv").Assert(Qual(moduleName+"/model", "Resource")),
					If(Id("ok")).Block(
						Return(Id("res"), Nil()),
					),
				),
			)

			// Not implemented: return OperationOutcome (release-specific)
			g.Return(Nil(), Qual(moduleName+"/model/gen/"+strings.ToLower(release), "OperationOutcome").Values(Dict{
				Id("Issue"): Index().Qual(moduleName+"/model/gen/"+strings.ToLower(release), "OperationOutcomeIssue").Values(
					Values(Dict{
						Id("Severity"):    Qual(moduleName+"/model/gen/"+strings.ToLower(release), "Code").Values(Dict{Id("Value"): Qual(moduleName+"/utils/ptr", "To").Call(Lit("fatal"))}),
						Id("Code"):        Qual(moduleName+"/model/gen/"+strings.ToLower(release), "Code").Values(Dict{Id("Value"): Qual(moduleName+"/utils/ptr", "To").Call(Lit("not-supported"))}),
						Id("Diagnostics"): Op("&").Qual(moduleName+"/model/gen/"+strings.ToLower(release), "String").Values(Dict{Id("Value"): Qual(moduleName+"/utils/ptr", "To").Call(Lit("OperationDefinition but no implementation found"))}),
					}),
				),
			}))
		})
}
