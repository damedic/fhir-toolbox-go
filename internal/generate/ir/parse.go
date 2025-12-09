package ir

import (
	"fmt"
	"github.com/damedic/fhir-toolbox-go/internal/generate/model"
	"slices"
	"strings"
)

var (
	primitives = []string{
		"base64Binary",
		"boolean",
		"canonical",
		"code",
		"date",
		"dateTime",
		"decimal",
		"id",
		"instant",
		"integer",
		"integer64",
		"markdown",
		"oid",
		"positiveInt",
		"string",
		"time",
		"unsignedInt",
		"uri",
		"url",
		"uuid",
		"xhtml",
	}
	notDomainResources = []string{"Binary", "Bundle", "Parameters"}
)

// Parse parses a FHIR Bundle into the intermediate representation.
func Parse(bundle *model.Bundle) []ResourceOrType {
	var resourcesOrTypes []ResourceOrType

	for _, s := range flattenBundle(bundle) {
		if s.Kind == "logical" {
			continue
		}
		if s.Abstract {
			continue
		}
		if s.Name == "Element" {
			continue
		}

		isResource := s.Kind == "resource"

		resourceOrType := ResourceOrType{
			Name:        s.Name,
			FileName:    toGoFileCasing(s.Name),
			IsResource:  isResource,
			IsPrimitive: slices.Contains(primitives, s.Name),
			Structs: parseStructs(
				s.Name,
				isResource,
				s.Kind == "resource" && strings.HasSuffix(s.BaseDefinition, "DomainResource"),
				s.BaseDefinition,
				s.Snapshot.Element,
				s.Type,
				fmt.Sprintf("%s\n\n%s", s.Description, s.Purpose),
			),
		}

		resourcesOrTypes = append(resourcesOrTypes, resourceOrType)
	}

	return resourcesOrTypes
}

func flattenBundle(bundle *model.Bundle) []*model.StructureDefinition {
	var definitions []*model.StructureDefinition

	for _, e := range bundle.Entry {
		if sd, ok := e.Resource.(*model.StructureDefinition); ok {
			definitions = append(definitions, sd)
		}
	}

	return definitions
}

func parseStructs(
	name string,
	isResource bool,
	isDomainResource bool,
	baseDefinition string,
	elementDefinitions []model.ElementDefinition,
	elementPathStripPrefix string,
	docComment string,
) []Struct {
	structName := toGoTypeCasing(name)

	groupedDefinitions := groupElementDefinitionsByPrefix(elementDefinitions, elementPathStripPrefix)

	// Extract base type name from baseDefinition URL (e.g., "http://hl7.org/fhir/StructureDefinition/uri" -> "uri")
	baseType := ""
	if baseDefinition != "" {
		parts := strings.Split(baseDefinition, "/")
		if len(parts) > 0 {
			baseType = parts[len(parts)-1]
		}
	}

	parsedStructs := []Struct{{
		Name:             structName,
		MarshalName:      name,
		IsResource:       isResource,
		IsDomainResource: isDomainResource,
		IsPrimitive:      slices.Contains(primitives, name),
		BaseType:         baseType,
		DocComment:       docComment,
	}}

	for _, g := range groupedDefinitions {
		if g.definitions[0].Max == "0" {
			continue
		}

		typeName := structName + toGoTypeCasing(g.fieldName)

		if len(g.definitions) > 1 {
			parsedStructs = append(
				parsedStructs, parseStructs(
					typeName,
					false,
					false,
					"", // backbone elements don't have baseDefinition
					g.definitions,
					g.definitions[0].Path,
					g.definitions[0].Definition,
				)...,
			)
		}

		parsedStructs[0].Fields = append(
			parsedStructs[0].Fields,
			parseField(
				structName,
				isResource,
				g.definitions[0],
				elementPathStripPrefix,
			),
		)
	}

	return parsedStructs
}

type definitionsGroup struct {
	fieldName   string
	definitions []model.ElementDefinition
}

func groupElementDefinitionsByPrefix(elementDefinitions []model.ElementDefinition, stripPrefix string) []definitionsGroup {
	var grouped []definitionsGroup

	for _, d := range elementDefinitions {
		if d.Path == stripPrefix {
			continue
		}

		fieldName := strings.SplitN(d.Path[len(stripPrefix)+1:], ".", 2)[0]

		if len(grouped) == 0 || grouped[len(grouped)-1].fieldName != fieldName {
			grouped = append(grouped, definitionsGroup{
				fieldName: fieldName,
			})
		}

		grouped[len(grouped)-1].definitions = append(grouped[len(grouped)-1].definitions, d)
	}

	return grouped
}

func parseField(
	structName string,
	isResource bool,
	elementDefinition model.ElementDefinition,
	elementPathStripPrefix string,
) StructField {
	fieldName := elementDefinition.Path[len(elementPathStripPrefix)+1:]
	fieldName, polymorph := strings.CutSuffix(fieldName, "[x]")

	var fieldTypes []FieldType
	if polymorph {
		for _, t := range elementDefinition.Type {
			fieldTypes = append(fieldTypes, matchFieldType(elementDefinition.Path, t.Code))
		}
	} else if isResource && fieldName == "id" {
		fieldTypes = append(fieldTypes, FieldType{
			Name:        "Id",
			IsPrimitive: true,
		})
	} else if elementDefinition.Type != nil {
		code := (elementDefinition.Type)[0].Code

		switch code {
		case "BackboneElement", "Element":
			fieldTypes = append(fieldTypes, FieldType{
				Name: structName + toGoTypeCasing(fieldName),
			})
		default:
			fieldTypes = append(fieldTypes, matchFieldType(elementDefinition.Path, code))
		}
	} else {
		// content reference
		// strip "#"
		fieldTypes = append(fieldTypes, FieldType{
			Name: toGoTypeCasing(elementDefinition.ContentReference[1:]),
		})
	}

	var binding *Binding
	if elementDefinition.Binding != nil {
		binding = &Binding{
			Strength: elementDefinition.Binding.Strength,
			ValueSet: elementDefinition.Binding.ValueSet,
		}
	}

	return StructField{
		Name:          toGoFieldCasing(fieldName),
		MarshalName:   fieldName,
		PossibleTypes: fieldTypes,
		Polymorph:     polymorph,
		Multiple:      elementDefinition.Max == "*",
		Optional:      elementDefinition.Min == 0,
		DocComment:    elementDefinition.Definition,
		Binding:       binding,
	}
}

func matchFieldType(path string, code string) FieldType {
	var fieldType FieldType

	// type like http://hl7.org/fhirpath/System.String
	switch t := code[strings.LastIndex(code, "/")+1:]; t {
	case "System.Boolean":
		fieldType.Name = "bool"
	case "System.Integer":
		switch path {
		case "integer64.value":
			fieldType.Name = "int64"
		default:
			fieldType.Name = "int32"
		}
	case "System.String":
		switch path {
		case "unsignedInt.value", "positiveInt.value":
			fieldType.Name = "uint32"
		default:
			fieldType.Name = "string"
		}
	case "System.Decimal":
		fieldType.Name = "string"
	case "System.Date", "System.DateTime", "System.Time":
		fieldType.Name = "string"
	case "Resource":
		fieldType.IsNestedResource = true
	default:
		fieldType.Name = toGoTypeCasing(t)
		fieldType.IsPrimitive = slices.Contains(primitives, t)
	}

	return fieldType
}
