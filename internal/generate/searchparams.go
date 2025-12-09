package generate

import (
	"github.com/damedic/fhir-toolbox-go/internal/generate/ir"
	"github.com/damedic/fhir-toolbox-go/internal/generate/model"
	"slices"
	"strings"

	. "github.com/dave/jennifer/jen"
)

type SearchParamsGenerator struct {
	NoOpGenerator
	SearchParams map[string]model.Bundle
}

type SearchParamInfo struct {
	Code string
	Type string
	Base []string
}

func (g SearchParamsGenerator) GenerateAdditional(f func(fileName string, pkgName string) *File, release string, rt []ir.ResourceOrType) {
	searchParams := g.SearchParams[release]
	generateSearchParamsModel(f("searchparams", strings.ToLower(release)), ir.FilterResources(rt), release, searchParams)
}

func parseSearchParams(bundle model.Bundle) map[string][]SearchParamInfo {
	result := make(map[string][]SearchParamInfo)

	for _, entry := range bundle.Entry {
		if entry.Resource == nil {
			continue
		}

		// Extract search parameter details from the resource
		resourceMap, ok := entry.Resource.(map[string]interface{})
		if !ok {
			continue
		}

		resourceType, ok := resourceMap["resourceType"].(string)
		if !ok || resourceType != "SearchParameter" {
			continue
		}

		code, ok := resourceMap["code"].(string)
		if !ok {
			continue
		}

		paramType, ok := resourceMap["type"].(string)
		if !ok {
			continue
		}

		baseArray, ok := resourceMap["base"].([]interface{})
		if !ok {
			continue
		}

		var base []string
		for _, b := range baseArray {
			if baseStr, ok := b.(string); ok {
				base = append(base, baseStr)
			}
		}

		param := SearchParamInfo{
			Code: code,
			Type: paramType,
			Base: base,
		}

		// Add to each resource type in base
		for _, resourceName := range base {
			result[resourceName] = append(result[resourceName], param)
		}
	}

	return result
}

// Convert FHIR search parameter type to Go type
func fhirTypeToGoType(fhirType string) string {
	switch fhirType {
	case "string":
		return "String"
	case "token":
		return "Token"
	case "date":
		return "Date"
	case "reference":
		return "Reference"
	case "quantity":
		return "Quantity"
	case "number":
		return "Number"
	case "uri":
		return "Uri"
	case "composite":
		return "Composite"
	case "special":
		return "Special"
	default:
		return "String" // fallback
	}
}

// Convert search parameter code to Go field name
func codeToFieldName(code string) string {
	// Convert kebab-case to PascalCase
	parts := strings.Split(code, "-")
	var result strings.Builder
	for _, part := range parts {
		if len(part) > 0 {
			result.WriteString(strings.ToUpper(part[:1]))
			if len(part) > 1 {
				result.WriteString(part[1:])
			}
		}
	}
	return result.String()
}

func generateSearchParamsModel(f *File, resources []ir.ResourceOrType, release string, searchParamsBundle model.Bundle) {
	// Set the package name to match other model files
	f.PackageComment("Package " + strings.ToLower(release) + " contains FHIR " + release + " model types and search parameters.")
	// Add comment for the package
	f.Comment("// Search parameter types and interfaces for typed search")
	f.Line()

	// Parse search parameters by resource type
	searchParamsByResource := parseSearchParams(searchParamsBundle)

	// Add import for search package
	searchPkg := moduleName + "/capabilities/search"

	// Use sealed interfaces from search package that accept both typed values and search.String
	f.Comment("// Search parameters use sealed interfaces that accept both typed values and search.String")

	// Generate search parameter structs for each resource
	for _, r := range resources {
		structName := r.Name + "Params"

		f.Commentf("// %s contains typed search parameters for %s resources.", structName, r.Name)
		f.Type().Id(structName).StructFunc(func(g *Group) {
			// Get search parameters for this resource
			resourceParams := searchParamsByResource[r.Name]

			// Separate common and resource-specific parameters
			var commonParams []SearchParamInfo
			var specificParams []SearchParamInfo

			for _, param := range resourceParams {
				// Check if this parameter applies to DomainResource or Resource (common to all)
				if slices.Contains(param.Base, "DomainResource") || slices.Contains(param.Base, "Resource") {
					commonParams = append(commonParams, param)
				} else {
					specificParams = append(specificParams, param)
				}
			}

			// Add common search parameters
			if len(commonParams) > 0 {
				g.Comment("// Common search parameters")
				for _, param := range commonParams {
					fieldName := codeToFieldName(param.Code)
					interfaceType := fhirTypeToGoType(param.Type) + "OrString"
					g.Id(fieldName).Qual(searchPkg, interfaceType).Tag(map[string]string{"json": param.Code + ",omitempty"})
				}
				g.Line()
			}

			// Add resource-specific search parameters
			if len(specificParams) > 0 {
				g.Commentf("// %s-specific search parameters", r.Name)
				for _, param := range specificParams {
					fieldName := codeToFieldName(param.Code)
					interfaceType := fhirTypeToGoType(param.Type) + "OrString"
					g.Id(fieldName).Qual(searchPkg, interfaceType).Tag(map[string]string{"json": param.Code + ",omitempty"})
				}
			}
		})
		f.Line()

		// Generate the Map method for the Parameters interface
		f.Commentf("// Map implements the search.Parameters interface for %s.", structName)
		f.Func().Params(Id("p").Id(structName)).Id("Map").Params().Map(
			Qual(searchPkg, "ParameterKey"),
		).Qual(searchPkg, "MatchAll").BlockFunc(func(g *Group) {
			g.Id("m").Op(":=").Make(Map(Qual(searchPkg, "ParameterKey")).Qual(searchPkg, "MatchAll"))
			g.Line()

			// Add the parameter mapping logic for all parameters
			resourceParams := searchParamsByResource[r.Name]
			for _, param := range resourceParams {
				fieldName := codeToFieldName(param.Code)
				g.If(Id("p").Dot(fieldName).Op("!=").Nil()).Block(
					Id("m").Index(Qual(searchPkg, "ParameterKey").Values(Dict{
						Id("Name"): Lit(param.Code),
					})).Op("=").Id("p").Dot(fieldName).Dot("MatchesAll").Call(),
				)
			}

			g.Line()
			g.Return(Id("m"))
		})
		f.Line()
	}
}
