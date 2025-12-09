package generate

import (
	"fmt"
	"github.com/iancoleman/strcase"
	"iter"
	"maps"
	"slices"
	"strings"

	"github.com/damedic/fhir-toolbox-go/internal/generate/ir"
	"github.com/damedic/fhir-toolbox-go/internal/generate/model"
	. "github.com/dave/jennifer/jen"
)

// ValueSetConstant represents a single constant in a value set
type ValueSetConstant struct {
	Name    string // Go constant name
	Value   string // FHIR code value
	Comment string
}

// ValueSetInfo represents a complete value set for generation
type ValueSetInfo struct {
	Name      string             // Value set name
	Constants []ValueSetConstant // All constants in this value set
}

var overrides = map[string]string{}

type ValueSetsGenerator struct {
	NoOpGenerator
	ValueSets model.Bundle
}

func (g ValueSetsGenerator) GenerateType(_ *File, _ ir.ResourceOrType) bool {
	// Don't generate per-type files, we generate all constants in GenerateAdditional
	return false
}

func (g ValueSetsGenerator) GenerateAdditional(f func(fileName string, pkgName string) *File, release string, rt []ir.ResourceOrType) {
	codeSystems := g.buildCodeSystemsMap()
	requiredBindings := g.collectRequiredBindings(rt, codeSystems)

	// resolve different VS cases and build value set info
	valueSets := g.resolveValueSets(maps.Values(requiredBindings))

	// generate the code
	if len(valueSets) > 0 {
		vf := f("value_sets", strings.ToLower(release))
		vf.Comment("Value set constants for required bindings")
		vf.Line()

		for _, vs := range valueSets {
			g.generateValueSetConstants(vf, vs)
		}
	}
}

func (g ValueSetsGenerator) buildCodeSystemsMap() map[string]*model.CodeSystem {
	codeSystems := make(map[string]*model.CodeSystem)
	for _, entry := range g.ValueSets.Entry {
		if cs, ok := entry.Resource.(*model.CodeSystem); ok {
			codeSystems[cs.URL] = cs
		}
	}
	return codeSystems
}

func (g ValueSetsGenerator) collectRequiredBindings(types []ir.ResourceOrType, codeSystems map[string]*model.CodeSystem) map[string]*RequiredBinding {
	bindings := map[string]*RequiredBinding{}

	for _, rt := range types {
		for _, s := range rt.Structs {
			for _, field := range s.Fields {
				b := g.findRequiredBinding(field, rt.Name, codeSystems)
				if b == nil {
					continue
				}
				bindings[b.ValueSetName] = b
			}
		}
	}

	return bindings
}

type RequiredBinding struct {
	ValueSetName string
	CodeSystem   *model.CodeSystem
	FieldName    string
	TypeName     string
}

func (g ValueSetsGenerator) findRequiredBinding(field ir.StructField, typeName string, codeSystems map[string]*model.CodeSystem) *RequiredBinding {
	if field.Binding == nil || field.Binding.Strength != "required" {
		return nil
	}

	// Remove version info from ValueSet URL if present
	valueSetURL := strings.Split(field.Binding.ValueSet, "|")[0]

	// Find the corresponding value set and its composed code systems
	for _, entry := range g.ValueSets.Entry {
		if vs, ok := entry.Resource.(*model.ValueSet); ok {
			if vs.URL == valueSetURL {
				for _, binding := range vs.Compose.Include {
					return &RequiredBinding{
						ValueSetName: vs.Name,
						CodeSystem:   codeSystems[binding.System],
						FieldName:    field.Name,
						TypeName:     typeName,
					}
				}
			}
		}
	}

	return nil
}

func (g ValueSetsGenerator) resolveValueSets(bindings iter.Seq[*RequiredBinding]) []ValueSetInfo {
	var valueSets []ValueSetInfo

	for binding := range bindings {
		if binding.CodeSystem == nil || len(binding.CodeSystem.Concept) == 0 {
			continue
		}

		vsInfo := ValueSetInfo{
			Name: binding.ValueSetName,
		}

		for _, concept := range binding.CodeSystem.Concept {
			constName := g.constantName(binding.ValueSetName, concept.Code)

			vsInfo.Constants = append(vsInfo.Constants, ValueSetConstant{
				Name:    constName,
				Value:   concept.Code,
				Comment: concept.Display,
			})
		}

		// Sort constants within each value set by name for consistent ordering
		slices.SortFunc(vsInfo.Constants, func(a, b ValueSetConstant) int {
			return strings.Compare(a.Name, b.Name)
		})

		valueSets = append(valueSets, vsInfo)
	}

	// Sort value sets by name for consistent ordering
	slices.SortFunc(valueSets, func(a, b ValueSetInfo) int {
		return strings.Compare(a.Name, b.Name)
	})

	return valueSets
}

// Step 3: Generate the code
func (g ValueSetsGenerator) generateValueSetConstants(f *File, vs ValueSetInfo) {
	f.Var().DefsFunc(func(g *Group) {
		for _, constant := range vs.Constants {
			g.Comment(fmt.Sprintf("%s %s", vs.Name, constant.Comment))
			g.Id(constant.Name).Op("=").Id("Code").Values(Dict{
				Id("Value"): Qual(moduleName+"/utils/ptr", "To").Call(Lit(constant.Value)),
			})
		}
	})

	f.Line()
}

// UpperCamelCase conversion with minimal replacements
func (g ValueSetsGenerator) constantName(valueSetName, concept string) string {
	replacer := strings.NewReplacer(
		"<=", "LessThanOrEqualTo",
		">=", "GreaterThanOrEqualTo",
		"<", "LessThan",
		">", "GreaterThan",
		"!=", "NotEqualTo",
		"=", "EqualTo",
	)

	return strcase.ToCamel(valueSetName) + strcase.ToCamel(replacer.Replace(concept))
}
