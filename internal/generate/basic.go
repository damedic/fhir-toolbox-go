package generate

import (
	"fmt"
	"github.com/damedic/fhir-toolbox-go/internal/generate/ir"
	"strings"

	. "github.com/dave/jennifer/jen"
)

type BasicDocGenerator struct {
	NoOpGenerator
}

func (g BasicDocGenerator) GenerateAdditional(f func(fileName string, pkgName string) *File, release string, rt []ir.ResourceOrType) {
	file := f("basic", strings.ToLower(release))
	file.PackageComment(fmt.Sprintf("Package %s provides basic resources that are valid across FHIR versions.", strings.ToLower(release)))

	// Add RawResource type for nested resources in basic types
	file.Comment("RawResource represents a FHIR resource as raw content (JSON or XML)")
	file.Type().Id("RawResource").Struct(
		Id("Content").String(),
		Id("IsJSON").Bool(),
		Id("IsXML").Bool(),
	)

	// Implement model.Resource interface for RawResource
	file.Comment("MemSize returns the memory size of the RawResource")
	file.Func().Params(Id("r").Id("RawResource")).Id("MemSize").Params().Int().Block(
		Return(Len(Id("r.Content")).Op("+").Int().Call(Qual("reflect", "TypeOf").Call(Id("r")).Dot("Size").Call())),
	)

	file.Comment("ResourceType returns the resource type from the raw content")
	file.Func().Params(Id("r").Id("RawResource")).Id("ResourceType").Params().String().Block(
		If(Id("r.IsJSON")).Block(
			Var().Id("t").Struct(
				Id("ResourceType").String().Tag(map[string]string{"json": "resourceType"}),
			),
			Id("err").Op(":=").Qual("encoding/json", "Unmarshal").Call(
				Index().Byte().Call(Id("r.Content")),
				Op("&").Id("t"),
			),
			If(Id("err").Op("==").Nil().Op("&&").Id("t.ResourceType").Op("!=").Lit("")).Block(
				Return(Id("t.ResourceType")),
			),
		).Else().If(Id("r.IsXML")).Block(
			Id("d").Op(":=").Qual("encoding/xml", "NewDecoder").Call(
				Qual("strings", "NewReader").Call(Id("r.Content")),
			),
			For().Block(
				List(Id("token"), Id("err")).Op(":=").Id("d.Token").Call(),
				If(Id("err").Op("!=").Nil()).Block(
					Break(),
				),
				If(List(Id("start"), Id("ok")).Op(":=").Id("token").Op(".").Call(Qual("encoding/xml", "StartElement")), Id("ok")).Block(
					Return(Id("start.Name.Local")),
				),
			),
		),
		Return(Lit("RawResource")), // Default type if we can't determine it
	)

	file.Comment("ResourceId returns the resource ID if present")
	file.Func().Params(Id("r").Id("RawResource")).Id("ResourceId").Params().Params(String(), Bool()).Block(
		If(Id("r.IsJSON")).Block(
			Var().Id("t").Struct(
				Id("Id").String().Tag(map[string]string{"json": "id"}),
			),
			Id("err").Op(":=").Qual("encoding/json", "Unmarshal").Call(
				Index().Byte().Call(Id("r.Content")),
				Op("&").Id("t"),
			),
			If(Id("err").Op("==").Nil().Op("&&").Id("t.Id").Op("!=").Lit("")).Block(
				Return(Id("t.Id"), True()),
			),
		).Else().If(Id("r.IsXML")).Block(
			Id("d").Op(":=").Qual("encoding/xml", "NewDecoder").Call(
				Qual("strings", "NewReader").Call(Id("r.Content")),
			),
			For().Block(
				List(Id("token"), Id("err")).Op(":=").Id("d.Token").Call(),
				If(Id("err").Op("!=").Nil()).Block(
					Break(),
				),
				If(List(Id("start"), Id("ok")).Op(":=").Id("token").Op(".").Call(Qual("encoding/xml", "StartElement")), Id("ok")).Block(
					For(List(Id("_"), Id("attr")).Op(":=").Range().Id("start.Attr")).Block(
						If(Id("attr.Name.Local").Op("==").Lit("id").Op("&&").Id("attr.Value").Op("!=").Lit("")).Block(
							Return(Id("attr.Value"), True()),
						),
					),
				),
			),
		),
		Return(Lit(""), False()), // Default if we can't determine it
	)

	// Implement fhirpath.Element interface methods
	file.Comment("Children returns child elements for FHIRPath evaluation")
	file.Func().Params(Id("r").Id("RawResource")).Id("Children").Params(Id("name").Op("...").String()).
		Qual("github.com/damedic/fhir-toolbox-go/fhirpath", "Collection").Block(
		Return(Nil()), // Empty collection for raw resources
	)

	file.Comment("ToBoolean converts to Boolean for FHIRPath")
	file.Func().Params(Id("r").Id("RawResource")).Id("ToBoolean").Params(Id("explicit").Bool()).
		Params(Qual("github.com/damedic/fhir-toolbox-go/fhirpath", "Boolean"), Bool(), Error()).Block(
		Return(False(), False(), Qual("fmt", "Errorf").Call(Lit("cannot convert RawResource to Boolean"))),
	)

	file.Comment("ToString converts to String for FHIRPath")
	file.Func().Params(Id("r").Id("RawResource")).Id("ToString").Params(Id("explicit").Bool()).
		Params(Qual("github.com/damedic/fhir-toolbox-go/fhirpath", "String"), Bool(), Error()).Block(
		Return(Qual("github.com/damedic/fhir-toolbox-go/fhirpath", "String").Call(Id("r.Content")), True(), Nil()),
	)

	file.Comment("ToInteger converts to Integer for FHIRPath")
	file.Func().Params(Id("r").Id("RawResource")).Id("ToInteger").Params(Id("explicit").Bool()).
		Params(Qual("github.com/damedic/fhir-toolbox-go/fhirpath", "Integer"), Bool(), Error()).Block(
		Return(Lit(0), False(), Qual("fmt", "Errorf").Call(Lit("cannot convert RawResource to Integer"))),
	)

	file.Comment("ToLong converts to Long for FHIRPath")
	file.Func().Params(Id("r").Id("RawResource")).Id("ToLong").Params(Id("explicit").Bool()).
		Params(Qual("github.com/damedic/fhir-toolbox-go/fhirpath", "Long"), Bool(), Error()).Block(
		Return(Lit(0), False(), Qual("fmt", "Errorf").Call(Lit("cannot convert RawResource to Long"))),
	)

	file.Comment("ToDecimal converts to Decimal for FHIRPath")
	file.Func().Params(Id("r").Id("RawResource")).Id("ToDecimal").Params(Id("explicit").Bool()).
		Params(Qual("github.com/damedic/fhir-toolbox-go/fhirpath", "Decimal"), Bool(), Error()).Block(
		Return(Qual("github.com/damedic/fhir-toolbox-go/fhirpath", "Decimal").Values(), False(),
			Qual("fmt", "Errorf").Call(Lit("cannot convert RawResource to Decimal"))),
	)

	file.Comment("ToDate converts to Date for FHIRPath")
	file.Func().Params(Id("r").Id("RawResource")).Id("ToDate").Params(Id("explicit").Bool()).
		Params(Qual("github.com/damedic/fhir-toolbox-go/fhirpath", "Date"), Bool(), Error()).Block(
		Return(Qual("github.com/damedic/fhir-toolbox-go/fhirpath", "Date").Values(), False(),
			Qual("fmt", "Errorf").Call(Lit("cannot convert RawResource to Date"))),
	)

	file.Comment("ToTime converts to Time for FHIRPath")
	file.Func().Params(Id("r").Id("RawResource")).Id("ToTime").Params(Id("explicit").Bool()).
		Params(Qual("github.com/damedic/fhir-toolbox-go/fhirpath", "Time"), Bool(), Error()).Block(
		Return(Qual("github.com/damedic/fhir-toolbox-go/fhirpath", "Time").Values(), False(),
			Qual("fmt", "Errorf").Call(Lit("cannot convert RawResource to Time"))),
	)

	file.Comment("ToDateTime converts to DateTime for FHIRPath")
	file.Func().Params(Id("r").Id("RawResource")).Id("ToDateTime").Params(Id("explicit").Bool()).
		Params(Qual("github.com/damedic/fhir-toolbox-go/fhirpath", "DateTime"), Bool(), Error()).Block(
		Return(Qual("github.com/damedic/fhir-toolbox-go/fhirpath", "DateTime").Values(), False(),
			Qual("fmt", "Errorf").Call(Lit("cannot convert RawResource to DateTime"))),
	)

	file.Comment("ToQuantity converts to Quantity for FHIRPath")
	file.Func().Params(Id("r").Id("RawResource")).Id("ToQuantity").Params(Id("explicit").Bool()).
		Params(Qual("github.com/damedic/fhir-toolbox-go/fhirpath", "Quantity"), Bool(), Error()).Block(
		Return(Qual("github.com/damedic/fhir-toolbox-go/fhirpath", "Quantity").Values(), False(),
			Qual("fmt", "Errorf").Call(Lit("cannot convert RawResource to Quantity"))),
	)

	file.Comment("Equal compares with another Element for FHIRPath")
	file.Func().Params(Id("r").Id("RawResource")).Id("Equal").
		Params(Id("other").Qual("github.com/damedic/fhir-toolbox-go/fhirpath", "Element")).
		Params(Bool(), Bool()).Block(
		List(Id("s"), Id("ok"), Id("_")).Op(":=").Id("r").Dot("ToString").Call(False()),
		If(Op("!").Id("ok")).Block(Return(False(), True())),
		List(Id("os"), Id("ok"), Id("_")).Op(":=").Id("other").Dot("ToString").Call(False()),
		If(Op("!").Id("ok")).Block(Return(False(), True())),
		Return(Id("s").Dot("Equal").Call(Id("os"))),
	)

	file.Comment("Equivalent checks equivalence with another Element for FHIRPath")
	file.Func().Params(Id("r").Id("RawResource")).Id("Equivalent").
		Params(Id("other").Qual("github.com/damedic/fhir-toolbox-go/fhirpath", "Element")).Bool().Block(
		List(Id("eq"), Id("ok")).Op(":=").Id("r").Dot("Equal").Call(Id("other")),
		Return(Id("eq").Op("&&").Id("ok")),
	)

	file.Comment("TypeInfo returns type information for FHIRPath")
	file.Func().Params(Id("r").Id("RawResource")).Id("TypeInfo").Params().
		Qual("github.com/damedic/fhir-toolbox-go/fhirpath", "TypeInfo").Block(
		Return(Qual("github.com/damedic/fhir-toolbox-go/fhirpath", "ClassInfo").Values(Dict{
			Id("Name"):      Lit("RawResource"),
			Id("Namespace"): Lit("FHIR"),
		})),
	)

	// Add JSON marshaling support
	file.Comment("MarshalJSON marshals the raw resource content")
	file.Func().Params(Id("r").Id("RawResource")).Id("MarshalJSON").Params().
		Params(Index().Byte(), Error()).Block(
		If(Id("r.IsJSON")).Block(
			Return(Index().Byte().Call(Id("r.Content")), Nil()),
		),
		// For XML content, we could convert to JSON, but for now just return error
		Return(Nil(), Qual("fmt", "Errorf").Call(Lit("cannot marshal XML content as JSON"))),
	)

	file.Comment("UnmarshalJSON unmarshals JSON content into the raw resource")
	file.Func().Params(Id("r").Op("*").Id("RawResource")).Id("UnmarshalJSON").Params(Id("data").Index().Byte()).
		Error().Block(
		Id("r.Content").Op("=").String().Call(Id("data")),
		Id("r.IsJSON").Op("=").True(),
		Id("r.IsXML").Op("=").False(),
		Return(Nil()),
	)

	file.Comment("String returns the raw resource content as a string")
	file.Func().Params(Id("r").Id("RawResource")).Id("String").Params().String().Block(
		Return(Id("r.Content")),
	)

	// Add XML marshaling support
	file.Comment("MarshalXML marshals the raw resource content as XML")
	file.Func().Params(Id("r").Id("RawResource")).Id("MarshalXML").
		Params(Id("e").Op("*").Qual("encoding/xml", "Encoder"), Id("start").Qual("encoding/xml", "StartElement")).
		Error().Block(
		If(Id("r.IsXML")).Block(
			// Parse the XML content and re-encode it
			Var().Id("elem").Interface(),
			Id("err").Op(":=").Qual("encoding/xml", "Unmarshal").Call(Index().Byte().Call(Id("r.Content")), Op("&").Id("elem")),
			If(Err().Op("!=").Nil()).Block(
				Return(Id("err")),
			),
			Return(Id("e.Encode").Call(Id("elem"))),
		),
		// For JSON content, we could convert to XML, but for now just return error
		Return(Qual("fmt", "Errorf").Call(Lit("cannot marshal JSON content as XML"))),
	)

	file.Comment("UnmarshalXML unmarshals XML content into the raw resource")
	file.Func().Params(Id("r").Op("*").Id("RawResource")).Id("UnmarshalXML").
		Params(Id("d").Op("*").Qual("encoding/xml", "Decoder"), Id("start").Qual("encoding/xml", "StartElement")).
		Error().Block(
		// Decode as generic interface and then encode back to string
		Var().Id("elem").Interface(),
		Id("err").Op(":=").Id("d.DecodeElement").Call(Op("&").Id("elem"), Op("&").Id("start")),
		If(Err().Op("!=").Nil()).Block(
			Return(Id("err")),
		),
		Var().Id("raw").Qual("bytes", "Buffer"),
		Id("err").Op("=").Qual("encoding/xml", "NewEncoder").Call(Op("&").Id("raw")).Dot("Encode").Call(Id("elem")),
		If(Err().Op("!=").Nil()).Block(
			Return(Id("err")),
		),
		Id("r.Content").Op("=").Id("raw.String").Call(),
		Id("r.IsJSON").Op("=").False(),
		Id("r.IsXML").Op("=").True(),
		Return(Nil()),
	)
}
