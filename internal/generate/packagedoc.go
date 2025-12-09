package generate

import (
	"fmt"
	"github.com/damedic/fhir-toolbox-go/internal/generate/ir"
	"strings"

	. "github.com/dave/jennifer/jen"
)

type ModelPkgDocGenerator struct {
	NoOpGenerator
}

func (g ModelPkgDocGenerator) GenerateAdditional(f func(fileName string, pkgName string) *File, release string, rt []ir.ResourceOrType) {
	file := f("doc", strings.ToLower(release))
	file.PackageComment(fmt.Sprintf("Package %s provides generated models for FHIR release %s.", strings.ToLower(release), release))
}

type CapabilityPkgDocGenerator struct {
	NoOpGenerator
}

func (g CapabilityPkgDocGenerator) GenerateAdditional(f func(fileName string, pkgName string) *File, release string, rt []ir.ResourceOrType) {
	file := f("doc", "capabilities"+release)
	file.PackageComment(fmt.Sprintf("Package capabilities%s provides generated capability interfaces for FHIR release %s.", release, release))
}
