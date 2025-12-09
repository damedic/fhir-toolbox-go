package generate

import (
	"github.com/damedic/fhir-toolbox-go/internal/generate/ir"
	"github.com/iancoleman/strcase"
	"sort"
	"strings"

	. "github.com/dave/jennifer/jen"
)

type ClientGenerator struct {
	NoOpGenerator
}

func (g ClientGenerator) GenerateAdditional(f func(fileName string, pkgName string) *File, release string, rt []ir.ResourceOrType) {
	resources := ir.FilterResources(rt)

	// Generate unified client that implements both generic and concrete APIs
	GenerateUnifiedClient(f("gen_client_"+strings.ToLower(release), "rest"), resources, release)
}

func GenerateUnifiedClient(f *File, resources []ir.ResourceOrType, release string) {
	releaseType := strings.ToUpper(release)
	lowerRelease := strings.ToLower(release)

	// Add build tag
	var buildTag string
	switch lowerRelease {
	case "r4":
		buildTag = "//go:build r4 || !(r4 || r4b || r5)"
	case "r4b":
		buildTag = "//go:build r4b || !(r4 || r4b || r5)"
	case "r5":
		buildTag = "//go:build r5 || !(r4 || r4b || r5)"
	}
	f.HeaderComment(buildTag)
	f.Line()

	// Add imports
	f.ImportName("context", "context")
	f.ImportName("net/http", "http")
	f.ImportName("net/url", "url")
	f.ImportName(moduleName+"/capabilities", "capabilities")
	f.ImportName(moduleName+"/capabilities/search", "search")
	f.ImportName(moduleName+"/capabilities/update", "update")
	f.ImportName(moduleName+"/model", "model")
	// no basic import for CapabilityStatement/Parameters anymore
	f.ImportName(moduleName+"/model/gen/"+lowerRelease, lowerRelease)

	// Generate client struct
	clientName := "Client" + releaseType
	f.Comment("// " + clientName + " provides both generic and resource-specific FHIR client capabilities.")
	f.Type().Id(clientName).Struct(
		Comment("// BaseURL is the base URL of the FHIR server").Line().Id("BaseURL").Op("*").Qual("net/url", "URL"),
		Comment("// Client is the HTTP client to use for requests. If nil, http.DefaultClient is used.").Line().Id("Client").Op("*").Qual("net/http", "Client"),
		Comment("// Format specifies the request/response format (JSON or XML). Defaults to JSON if not set.").Line().Id("Format").Id("Format"),
	)

	// Interface conformance checks for generic client interfaces
	f.Var().DefsFunc(func(g *Group) {
		g.Id("_").Qual(moduleName+"/capabilities", "GenericCapabilities").Op("=").Parens(Op("*").Id(clientName)).Parens(Nil())
		g.Id("_").Qual(moduleName+"/capabilities", "GenericSearch").Op("=").Parens(Op("*").Id(clientName)).Parens(Nil())
		g.Id("_").Qual(moduleName+"/capabilities", "GenericCreate").Op("=").Parens(Op("*").Id(clientName)).Parens(Nil())
		g.Id("_").Qual(moduleName+"/capabilities", "GenericRead").Op("=").Parens(Op("*").Id(clientName)).Parens(Nil())
		g.Id("_").Qual(moduleName+"/capabilities", "GenericUpdate").Op("=").Parens(Op("*").Id(clientName)).Parens(Nil())
		g.Id("_").Qual(moduleName+"/capabilities", "GenericDelete").Op("=").Parens(Op("*").Id(clientName)).Parens(Nil())
		g.Id("_").Qual(moduleName+"/capabilities", "GenericOperation").Op("=").Parens(Op("*").Id(clientName)).Parens(Nil())
	})

	// Add HTTP client helper method
	f.Comment("// httpClient returns the HTTP client, using http.DefaultClient if none is set.")
	f.Func().Params(Id("c").Op("*").Id(clientName)).Id("httpClient").Params().Op("*").Qual("net/http", "Client").Block(
		If(Id("c").Dot("Client").Op("!=").Nil()).Block(
			Return(Id("c").Dot("Client")),
		),
		Return(Qual("net/http", "DefaultClient")),
	)

	// Generate generic methods
	generateUnifiedGenericMethods(f, clientName, releaseType)

	// Generate concrete methods for each resource
	for _, resource := range resources {
		generateUnifiedConcreteResourceMethods(f, resource, release, releaseType, clientName)
	}

	// Generate operation helpers derived from the spec
	generateOperationHelpers(f, resources, release, releaseType, clientName)
}

func generateUnifiedGenericMethods(f *File, clientName, releaseType string) {
	// CapabilityStatement method
	f.Comment("// CapabilityStatement retrieves the server's CapabilityStatement.")
	f.Func().Params(Id("c").Op("*").Id(clientName)).Id("CapabilityStatement").Params(
		Id("ctx").Qual("context", "Context"),
	).Params(
		Qual(moduleName+"/model", "CapabilityStatement"),
		Error(),
	).Block(
		Id("client").Op(":=").Op("&").Id("internalClient").Index(Qual(moduleName+"/model", releaseType)).Values(
			Id("baseURL").Op(":").Id("c").Dot("BaseURL"),
			Id("client").Op(":").Id("c").Dot("httpClient").Call(),
			Id("format").Op(":").Id("c").Dot("Format"),
		),
		Return(Id("client").Dot("CapabilityStatement").Call(Id("ctx"))),
	)

	// Generic Create method
	f.Comment("// Create creates a new resource.")
	f.Func().Params(Id("c").Op("*").Id(clientName)).Id("Create").Params(
		Id("ctx").Qual("context", "Context"),
		Id("resource").Qual(moduleName+"/model", "Resource"),
	).Params(
		Qual(moduleName+"/model", "Resource"),
		Error(),
	).Block(
		Id("client").Op(":=").Op("&").Id("internalClient").Index(Qual(moduleName+"/model", releaseType)).Values(
			Id("baseURL").Op(":").Id("c").Dot("BaseURL"),
			Id("client").Op(":").Id("c").Dot("httpClient").Call(),
			Id("format").Op(":").Id("c").Dot("Format"),
		),
		Return(Id("client").Dot("Create").Call(Id("ctx"), Id("resource"))),
	)

	// Generic Read method
	f.Comment("// Read retrieves a resource by type and ID.")
	f.Func().Params(Id("c").Op("*").Id(clientName)).Id("Read").Params(
		Id("ctx").Qual("context", "Context"),
		Id("resourceType").String(),
		Id("id").String(),
	).Params(
		Qual(moduleName+"/model", "Resource"),
		Error(),
	).Block(
		Id("client").Op(":=").Op("&").Id("internalClient").Index(Qual(moduleName+"/model", releaseType)).Values(
			Id("baseURL").Op(":").Id("c").Dot("BaseURL"),
			Id("client").Op(":").Id("c").Dot("httpClient").Call(),
			Id("format").Op(":").Id("c").Dot("Format"),
		),
		Return(Id("client").Dot("Read").Call(Id("ctx"), Id("resourceType"), Id("id"))),
	)

	// Generic Update method
	f.Comment("// Update updates an existing resource.")
	f.Func().Params(Id("c").Op("*").Id(clientName)).Id("Update").Params(
		Id("ctx").Qual("context", "Context"),
		Id("resource").Qual(moduleName+"/model", "Resource"),
	).Params(
		Qual(moduleName+"/capabilities/update", "Result").Index(Qual(moduleName+"/model", "Resource")),
		Error(),
	).Block(
		Id("client").Op(":=").Op("&").Id("internalClient").Index(Qual(moduleName+"/model", releaseType)).Values(
			Id("baseURL").Op(":").Id("c").Dot("BaseURL"),
			Id("client").Op(":").Id("c").Dot("httpClient").Call(),
			Id("format").Op(":").Id("c").Dot("Format"),
		),
		Return(Id("client").Dot("Update").Call(Id("ctx"), Id("resource"))),
	)

	// Generic Delete method
	f.Comment("// Delete deletes a resource by type and ID.")
	f.Func().Params(Id("c").Op("*").Id(clientName)).Id("Delete").Params(
		Id("ctx").Qual("context", "Context"),
		Id("resourceType").String(),
		Id("id").String(),
	).Params(
		Error(),
	).Block(
		Id("client").Op(":=").Op("&").Id("internalClient").Index(Qual(moduleName+"/model", releaseType)).Values(
			Id("baseURL").Op(":").Id("c").Dot("BaseURL"),
			Id("client").Op(":").Id("c").Dot("httpClient").Call(),
			Id("format").Op(":").Id("c").Dot("Format"),
		),
		Return(Id("client").Dot("Delete").Call(Id("ctx"), Id("resourceType"), Id("id"))),
	)

	// Generic Search method
	f.Comment("// Search performs a search operation for the given resource type.")
	f.Func().Params(Id("c").Op("*").Id(clientName)).Id("Search").Params(
		Id("ctx").Qual("context", "Context"),
		Id("resourceType").String(),
		Id("parameters").Qual(moduleName+"/capabilities/search", "Parameters"),
		Id("options").Qual(moduleName+"/capabilities/search", "Options"),
	).Params(
		Qual(moduleName+"/capabilities/search", "Result").Index(Qual(moduleName+"/model", "Resource")),
		Error(),
	).Block(
		Id("client").Op(":=").Op("&").Id("internalClient").Index(Qual(moduleName+"/model", releaseType)).Values(
			Id("baseURL").Op(":").Id("c").Dot("BaseURL"),
			Id("client").Op(":").Id("c").Dot("httpClient").Call(),
			Id("format").Op(":").Id("c").Dot("Format"),
		),
		Return(Id("client").Dot("Search").Call(Id("ctx"), Id("resourceType"), Id("parameters"), Id("options"))),
	)

	// Generic Invoke method
	f.Comment("// Invoke invokes a FHIR operation at system, type, or instance level.")
	f.Func().Params(Id("c").Op("*").Id(clientName)).Id("Invoke").Params(
		Id("ctx").Qual("context", "Context"),
		Id("resourceType").String(),
		Id("resourceID").String(),
		Id("code").String(),
		Id("parameters").Qual(moduleName+"/model", "Parameters"),
	).Params(
		Qual(moduleName+"/model", "Resource"),
		Error(),
	).Block(
		Id("client").Op(":=").Op("&").Id("internalClient").Index(Qual(moduleName+"/model", releaseType)).Values(
			Id("baseURL").Op(":").Id("c").Dot("BaseURL"),
			Id("client").Op(":").Id("c").Dot("httpClient").Call(),
			Id("format").Op(":").Id("c").Dot("Format"),
		),
		Return(Id("client").Dot("Invoke").Call(Id("ctx"), Id("resourceType"), Id("resourceID"), Id("code"), Id("parameters"))),
	)

	// Thin wrappers for convenience
	f.Comment("// InvokeSystem invokes a system-level operation (/$code).")
	f.Func().Params(Id("c").Op("*").Id(clientName)).Id("InvokeSystem").Params(
		Id("ctx").Qual("context", "Context"),
		Id("code").String(),
		Id("parameters").Qual(moduleName+"/model", "Parameters"),
	).Params(
		Qual(moduleName+"/model", "Resource"),
		Error(),
	).Block(
		Return(Id("c").Dot("Invoke").Call(Id("ctx"), Lit(""), Lit(""), Id("code"), Id("parameters"))),
	)

	f.Comment("// InvokeType invokes a type-level operation (/{type}/$code).")
	f.Func().Params(Id("c").Op("*").Id(clientName)).Id("InvokeType").Params(
		Id("ctx").Qual("context", "Context"),
		Id("resourceType").String(),
		Id("code").String(),
		Id("parameters").Qual(moduleName+"/model", "Parameters"),
	).Params(
		Qual(moduleName+"/model", "Resource"),
		Error(),
	).Block(
		Return(Id("c").Dot("Invoke").Call(Id("ctx"), Id("resourceType"), Lit(""), Id("code"), Id("parameters"))),
	)

	f.Comment("// InvokeInstance invokes an instance-level operation (/{type}/{id}/$code).")
	f.Func().Params(Id("c").Op("*").Id(clientName)).Id("InvokeInstance").Params(
		Id("ctx").Qual("context", "Context"),
		Id("resourceType").String(),
		Id("id").String(),
		Id("code").String(),
		Id("parameters").Qual(moduleName+"/model", "Parameters"),
	).Params(
		Qual(moduleName+"/model", "Resource"),
		Error(),
	).Block(
		Return(Id("c").Dot("Invoke").Call(Id("ctx"), Id("resourceType"), Id("id"), Id("code"), Id("parameters"))),
	)
}

func generateUnifiedConcreteResourceMethods(f *File, resource ir.ResourceOrType, release, releaseType, clientName string) {
	resourceName := resource.Name
	lowerRelease := strings.ToLower(release)

	// Interface conformance checks for concrete capabilities of this resource
	f.Var().DefsFunc(func(g *Group) {
		g.Id("_").Qual(moduleName+"/capabilities/gen/"+lowerRelease, resourceName+"Create").Op("=").Parens(Op("*").Id(clientName)).Parens(Nil())
		g.Id("_").Qual(moduleName+"/capabilities/gen/"+lowerRelease, resourceName+"Read").Op("=").Parens(Op("*").Id(clientName)).Parens(Nil())
		g.Id("_").Qual(moduleName+"/capabilities/gen/"+lowerRelease, resourceName+"Update").Op("=").Parens(Op("*").Id(clientName)).Parens(Nil())
		g.Id("_").Qual(moduleName+"/capabilities/gen/"+lowerRelease, resourceName+"Delete").Op("=").Parens(Op("*").Id(clientName)).Parens(Nil())
		g.Id("_").Qual(moduleName+"/capabilities/gen/"+lowerRelease, resourceName+"Search").Op("=").Parens(Op("*").Id(clientName)).Parens(Nil())
	})

	// Create method
	f.Comment("// Create" + resourceName + " creates a new " + resourceName + " resource.")
	f.Func().Params(Id("c").Op("*").Id(clientName)).Id("Create"+resourceName).Params(
		Id("ctx").Qual("context", "Context"),
		Id("resource").Qual(moduleName+"/model/gen/"+lowerRelease, resourceName),
	).Params(
		Qual(moduleName+"/model/gen/"+lowerRelease, resourceName),
		Error(),
	).Block(
		Id("client").Op(":=").Op("&").Id("internalClient").Index(Qual(moduleName+"/model", releaseType)).Values(
			Id("baseURL").Op(":").Id("c").Dot("BaseURL"),
			Id("client").Op(":").Id("c").Dot("httpClient").Call(),
			Id("format").Op(":").Id("c").Dot("Format"),
		),
		Id("wrapper").Op(":=").Qual(moduleName+"/capabilities/gen/"+lowerRelease, "Concrete").Values(
			Id("Generic").Op(":").Id("client"),
		),
		Return(Id("wrapper").Dot("Create"+resourceName).Call(Id("ctx"), Id("resource"))),
	)

	// Read method
	f.Comment("// Read" + resourceName + " retrieves a " + resourceName + " resource by ID.")
	f.Func().Params(Id("c").Op("*").Id(clientName)).Id("Read"+resourceName).Params(
		Id("ctx").Qual("context", "Context"),
		Id("id").String(),
	).Params(
		Qual(moduleName+"/model/gen/"+lowerRelease, resourceName),
		Error(),
	).Block(
		Id("client").Op(":=").Op("&").Id("internalClient").Index(Qual(moduleName+"/model", releaseType)).Values(
			Id("baseURL").Op(":").Id("c").Dot("BaseURL"),
			Id("client").Op(":").Id("c").Dot("httpClient").Call(),
			Id("format").Op(":").Id("c").Dot("Format"),
		),
		Id("wrapper").Op(":=").Qual(moduleName+"/capabilities/gen/"+lowerRelease, "Concrete").Values(
			Id("Generic").Op(":").Id("client"),
		),
		Return(Id("wrapper").Dot("Read"+resourceName).Call(Id("ctx"), Id("id"))),
	)

	// Update method
	f.Comment("// Update" + resourceName + " updates an existing " + resourceName + " resource.")
	f.Func().Params(Id("c").Op("*").Id(clientName)).Id("Update"+resourceName).Params(
		Id("ctx").Qual("context", "Context"),
		Id("resource").Qual(moduleName+"/model/gen/"+lowerRelease, resourceName),
	).Params(
		Qual(moduleName+"/capabilities/update", "Result").Index(Qual(moduleName+"/model/gen/"+lowerRelease, resourceName)),
		Error(),
	).Block(
		Id("client").Op(":=").Op("&").Id("internalClient").Index(Qual(moduleName+"/model", releaseType)).Values(
			Id("baseURL").Op(":").Id("c").Dot("BaseURL"),
			Id("client").Op(":").Id("c").Dot("httpClient").Call(),
			Id("format").Op(":").Id("c").Dot("Format"),
		),
		Id("wrapper").Op(":=").Qual(moduleName+"/capabilities/gen/"+lowerRelease, "Concrete").Values(
			Id("Generic").Op(":").Id("client"),
		),
		Return(Id("wrapper").Dot("Update"+resourceName).Call(Id("ctx"), Id("resource"))),
	)

	// Delete method
	f.Comment("// Delete" + resourceName + " deletes a " + resourceName + " resource by ID.")
	f.Func().Params(Id("c").Op("*").Id(clientName)).Id("Delete"+resourceName).Params(
		Id("ctx").Qual("context", "Context"),
		Id("id").String(),
	).Params(
		Error(),
	).Block(
		Id("client").Op(":=").Op("&").Id("internalClient").Index(Qual(moduleName+"/model", releaseType)).Values(
			Id("baseURL").Op(":").Id("c").Dot("BaseURL"),
			Id("client").Op(":").Id("c").Dot("httpClient").Call(),
			Id("format").Op(":").Id("c").Dot("Format"),
		),
		Id("wrapper").Op(":=").Qual(moduleName+"/capabilities/gen/"+lowerRelease, "Concrete").Values(
			Id("Generic").Op(":").Id("client"),
		),
		Return(Id("wrapper").Dot("Delete"+resourceName).Call(Id("ctx"), Id("id"))),
	)

	// SearchCapabilities method
	f.Comment("// SearchCapabilities" + resourceName + " returns server search capabilities for " + resourceName + ".")
	f.Func().Params(Id("c").Op("*").Id(clientName)).Id("SearchCapabilities"+resourceName).Params(
		Id("ctx").Qual("context", "Context"),
	).Params(
		Qual(moduleName+"/model/gen/"+lowerRelease, "SearchCapabilities"),
		Error(),
	).Block(
		Id("client").Op(":=").Op("&").Id("internalClient").Index(Qual(moduleName+"/model", releaseType)).Values(
			Id("baseURL").Op(":").Id("c").Dot("BaseURL"),
			Id("client").Op(":").Id("c").Dot("httpClient").Call(),
			Id("format").Op(":").Id("c").Dot("Format"),
		),
		Id("wrapper").Op(":=").Qual(moduleName+"/capabilities/gen/"+lowerRelease, "Concrete").Values(
			Id("Generic").Op(":").Id("client"),
		),
		Return(Id("wrapper").Dot("SearchCapabilities"+resourceName).Call(Id("ctx"))),
	)

	// Search method
	f.Comment("// Search" + resourceName + " performs a search for " + resourceName + " resources.")
	f.Func().Params(Id("c").Op("*").Id(clientName)).Id("Search"+resourceName).Params(
		Id("ctx").Qual("context", "Context"),
		Id("parameters").Qual(moduleName+"/capabilities/search", "Parameters"),
		Id("options").Qual(moduleName+"/capabilities/search", "Options"),
	).Params(
		Qual(moduleName+"/capabilities/search", "Result").Index(Qual(moduleName+"/model/gen/"+lowerRelease, resourceName)),
		Error(),
	).Block(
		Id("client").Op(":=").Op("&").Id("internalClient").Index(Qual(moduleName+"/model", releaseType)).Values(
			Id("baseURL").Op(":").Id("c").Dot("BaseURL"),
			Id("client").Op(":").Id("c").Dot("httpClient").Call(),
			Id("format").Op(":").Id("c").Dot("Format"),
		),
		Id("wrapper").Op(":=").Qual(moduleName+"/capabilities/gen/"+lowerRelease, "Concrete").Values(
			Id("Generic").Op(":").Id("client"),
		),
		Return(Id("wrapper").Dot("Search"+resourceName).Call(Id("ctx"), Id("parameters"), Id("options"))),
	)
}

// generateOperationHelpers adds convenience wrappers for spec-defined operations.
// It derives available operations by scraping the FHIR operations list for the given release
// and emits system-level helpers (InvokeXxx) and type/instance helpers (Invoke{Resource}Xxx).
func generateOperationHelpers(f *File, resources []ir.ResourceOrType, release, releaseType, clientName string) {
	ops, err := FetchOperationsForRelease(release)
	if err != nil {
		// Fall back silently: if fetching fails, skip generating helpers
		return
	}

	// System-level helpers: one per op with system=true
	for _, op := range ops.System {
		methodSuffix := strcase.ToCamel(op)
		f.Comment("// Invoke" + methodSuffix + " invokes the system-level $" + op + " operation.")
		f.Func().Params(Id("c").Op("*").Id(clientName)).Id("Invoke"+methodSuffix).Params(
			Id("ctx").Qual("context", "Context"),
			Id("parameters").Qual(moduleName+"/model", "Parameters"),
		).Params(
			Qual(moduleName+"/model", "Resource"),
			Error(),
		).Block(
			Return(Id("c").Dot("InvokeSystem").Call(Id("ctx"), Lit("$"+op), Id("parameters"))),
		)
		f.Line()
	}

	// Build a lookup for resources present in this release
	resNames := map[string]bool{}
	var present []string
	for _, r := range resources {
		resNames[r.Name] = true
		present = append(present, r.Name)
	}
	sort.Strings(present)

	// Helper to emit one resource+op with level info
	emitResOp := func(res string, op string, lvls resOpLevels) {
		methodSuffix := strcase.ToCamel(op)
		methodName := "Invoke" + res + methodSuffix
		// Choose signature based on supported levels
		switch {
		case lvls.HasType && lvls.HasInstance:
			f.Comment("// " + methodName + " invokes $" + op + " on " + res + " at type or instance level.")
			f.Func().Params(Id("c").Op("*").Id(clientName)).Id(methodName).Params(
				Id("ctx").Qual("context", "Context"),
				Id("parameters").Qual(moduleName+"/model", "Parameters"),
				Id("id").Op("...").String(),
			).Params(
				Qual(moduleName+"/model", "Resource"),
				Error(),
			).Block(
				If(Len(Id("id")).Op(">").Lit(0)).Block(
					Return(Id("c").Dot("InvokeInstance").Call(Id("ctx"), Lit(res), Id("id").Index(Lit(0)), Lit("$"+op), Id("parameters"))),
				).Else().Block(
					Return(Id("c").Dot("InvokeType").Call(Id("ctx"), Lit(res), Lit("$"+op), Id("parameters"))),
				),
			)
			f.Line()
		case lvls.HasInstance:
			f.Comment("// " + methodName + " invokes $" + op + " on " + res + " at instance level.")
			f.Func().Params(Id("c").Op("*").Id(clientName)).Id(methodName).Params(
				Id("ctx").Qual("context", "Context"),
				Id("parameters").Qual(moduleName+"/model", "Parameters"),
				Id("id").String(),
			).Params(
				Qual(moduleName+"/model", "Resource"),
				Error(),
			).Block(
				Return(Id("c").Dot("InvokeInstance").Call(Id("ctx"), Lit(res), Id("id"), Lit("$"+op), Id("parameters"))),
			)
			f.Line()
		case lvls.HasType:
			f.Comment("// " + methodName + " invokes $" + op + " on " + res + " at type level.")
			f.Func().Params(Id("c").Op("*").Id(clientName)).Id(methodName).Params(
				Id("ctx").Qual("context", "Context"),
				Id("parameters").Qual(moduleName+"/model", "Parameters"),
			).Params(
				Qual(moduleName+"/model", "Resource"),
				Error(),
			).Block(
				Return(Id("c").Dot("InvokeType").Call(Id("ctx"), Lit(res), Lit("$"+op), Id("parameters"))),
			)
			f.Line()
		default:
			// No supported level found; skip emitting
		}
	}

	// Expand generic placeholder "*" to all resources present
	if generic, ok := ops.ByResource["*"]; ok {
		// sort operation codes for deterministic order
		var genOps []string
		for op := range generic {
			genOps = append(genOps, op)
		}
		sort.Strings(genOps)
		for _, res := range present {
			for _, op := range genOps {
				emitResOp(res, op, generic[op])
			}
		}
	}

	// Emit explicit resource mappings
	// sort resource names for deterministic order
	var explicitRes []string
	for res := range ops.ByResource {
		if res == "*" {
			continue
		}
		if !resNames[res] {
			continue
		}
		explicitRes = append(explicitRes, res)
	}
	sort.Strings(explicitRes)
	for _, res := range explicitRes {
		perRes := ops.ByResource[res]
		var opsForRes []string
		for op := range perRes {
			opsForRes = append(opsForRes, op)
		}
		sort.Strings(opsForRes)
		for _, op := range opsForRes {
			emitResOp(res, op, perRes[op])
		}
	}
}
