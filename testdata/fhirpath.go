package testdata

import (
	"archive/zip"
	"bytes"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"log"
	"path"
	"regexp"
	"strconv"
	"strings"

	"github.com/cockroachdb/apd/v3"
	"github.com/damedic/fhir-toolbox-go/fhirpath"
	"github.com/damedic/fhir-toolbox-go/model"
	"github.com/damedic/fhir-toolbox-go/model/gen/r4"
	"github.com/damedic/fhir-toolbox-go/model/gen/r4b"
	"github.com/damedic/fhir-toolbox-go/model/gen/r5"
)

type resourceDecoder func(io.Reader) (model.Resource, error)

type fhirPathReleaseConfig struct {
	testsBasePath      string
	testsFile          string
	decodeXMLResource  resourceDecoder
	decodeJSONResource resourceDecoder
	inputBases         []string
}

var fhirPathReleaseConfigs = map[string]fhirPathReleaseConfig{
	model.R4{}.String(): {
		testsBasePath:      path.Join("r4", "fhirpath"),
		testsFile:          "tests-fhir-r4.xml",
		decodeXMLResource:  decodeR4ResourceXML,
		decodeJSONResource: decodeR4ResourceJSON,
		inputBases: []string{
			"r4",
			path.Join("r4", "examples"),
			path.Join("r4", "fhirpath", "input"),
		},
	},
	model.R4B{}.String(): {
		testsBasePath:      path.Join("r4b", "fhirpath"),
		testsFile:          "tests-fhir-r4b.xml",
		decodeXMLResource:  decodeR4BResourceXML,
		decodeJSONResource: decodeR4BResourceJSON,
		inputBases: []string{
			"r4b",
			path.Join("r4b", "examples"),
			path.Join("r4b", "fhirpath", "input"),
		},
	},
	model.R5{}.String(): {
		testsBasePath:      path.Join("r5", "fhirpath"),
		testsFile:          "tests-fhir-r5.xml",
		decodeXMLResource:  decodeR5ResourceXML,
		decodeJSONResource: decodeR5ResourceJSON,
		inputBases: []string{
			"r5",
			path.Join("r5", "examples"),
			path.Join("r5", "fhirpath", "input"),
		},
	},
}

func GetFHIRPathTests(release model.Release) FHIRPathTests {
	releaseKey := release.String()
	cfg, ok := fhirPathReleaseConfigs[releaseKey]
	if !ok {
		log.Fatalf("unsupported FHIRPath release %q", releaseKey)
	}

	downloadFHIRPathTests()
	filePath := fhirPathTestsFilePath()

	log.Println("opening zip archive...")
	zipReader, err := zip.OpenReader(filePath)
	if err != nil {
		log.Fatal(err)
	}
	defer zipReader.Close()

	archive := newZipArchive(zipReader)

	testFilePath := path.Join(cfg.testsBasePath, cfg.testsFile)
	testFile, err := archive.open(testFilePath)
	if err != nil {
		log.Fatal(err)
	}
	testsXml := readZipFile(testFile)

	testsXml = bytes.ReplaceAll(testsXml, []byte("< "), []byte("&lt; "))
	testsXml = bytes.ReplaceAll(testsXml, []byte("<="), []byte("&lt;="))

	var tests FHIRPathTests
	err = xml.NewDecoder(bytes.NewReader(testsXml)).Decode(&tests)
	if err != nil {
		log.Fatal(err)
	}

	for i, g := range tests.Groups {
		for j, t := range g.Tests {
			for k := range t.Output {
				t.Output[k].inferTypeFromValue()
			}
			if strings.TrimSpace(t.InputFile) == "" {
				continue
			}
			if strings.EqualFold(t.Mode, "cda") {
				g.Tests[j] = t
				continue
			}
			inputFile := openInputFile(archive, cfg, t.InputFile)
			t.InputResource = decodeInputResource(inputFile, cfg, t.InputFile)
			g.Tests[j] = t
		}
		tests.Groups[i] = g
	}

	return tests
}

type zipArchive struct {
	root  string
	files map[string]*zip.File
}

var errZipFileNotFound = errors.New("file not found in archive")

func newZipArchive(z *zip.ReadCloser) zipArchive {
	return zipArchive{
		root:  detectZipRoot(z.File),
		files: indexZipFiles(z.File),
	}
}

func detectZipRoot(files []*zip.File) string {
	for _, f := range files {
		name := strings.TrimSuffix(f.Name, "/")
		if name == "" {
			continue
		}
		if idx := strings.Index(name, "/"); idx > 0 {
			return name[:idx]
		}
	}
	return ""
}

func indexZipFiles(files []*zip.File) map[string]*zip.File {
	index := make(map[string]*zip.File, len(files))
	for _, f := range files {
		if f.FileInfo().IsDir() {
			continue
		}
		index[f.Name] = f
	}
	return index
}

func (a zipArchive) open(relPath string) (*zip.File, error) {
	relPath = path.Clean(relPath)
	if strings.HasPrefix(relPath, "..") {
		return nil, fmt.Errorf("invalid relative path %q", relPath)
	}
	fullPath := relPath
	if a.root != "" {
		fullPath = path.Join(a.root, relPath)
	}
	f, ok := a.files[fullPath]
	if !ok {
		return nil, fmt.Errorf("%w: %s (full path %s)", errZipFileNotFound, relPath, fullPath)
	}
	return f, nil
}

func openInputFile(archive zipArchive, cfg fhirPathReleaseConfig, filename string) *zip.File {
	var lastErr error
	for _, base := range cfg.inputBases {
		candidate := path.Join(base, filename)
		f, err := archive.open(candidate)
		if err == nil {
			return f
		}
		if errors.Is(err, errZipFileNotFound) {
			lastErr = err
			continue
		}
		log.Fatal(err)
	}
	if lastErr != nil {
		log.Fatal(lastErr)
	}
	log.Fatalf("input file %q not found in archive for any configured base", filename)
	return nil
}

func readZipFile(f *zip.File) []byte {
	rc, err := f.Open()
	if err != nil {
		log.Fatal(err)
	}
	defer rc.Close()

	data, err := io.ReadAll(rc)
	if err != nil {
		log.Fatal(err)
	}
	return data
}

func decodeInputResource(file *zip.File, cfg fhirPathReleaseConfig, filename string) model.Resource {
	ext := strings.ToLower(path.Ext(filename))
	switch ext {
	case ".json":
		if cfg.decodeJSONResource == nil {
			log.Fatalf("no JSON decoder configured for %s input %s", filename, cfg.testsFile)
		}
		return decodeResourceFromZip(file, cfg.decodeJSONResource, false)
	case ".xml":
		fallthrough
	default:
		if cfg.decodeXMLResource == nil {
			log.Fatalf("no XML decoder configured for %s input %s", filename, cfg.testsFile)
		}
		return decodeResourceFromZip(file, cfg.decodeXMLResource, true)
	}
}

var (
	prefixedXMLNSAttr = regexp.MustCompile(`\s+xmlns:[A-Za-z0-9_.-]+="[^"]*"`)
	xsiAttr           = regexp.MustCompile(`\s+xsi:[A-Za-z0-9_.-]+="[^"]*"`)
)

func sanitizeXMLNamespaces(data []byte) []byte {
	data = prefixedXMLNSAttr.ReplaceAll(data, []byte{})
	data = xsiAttr.ReplaceAll(data, []byte{})
	return data
}

func decodeResourceFromZip(f *zip.File, decode resourceDecoder, sanitizeXML bool) model.Resource {
	rc, err := f.Open()
	if err != nil {
		log.Fatal(err)
	}
	defer rc.Close()

	var reader io.Reader = rc
	if sanitizeXML {
		data, err := io.ReadAll(rc)
		if err != nil {
			log.Fatal(err)
		}
		data = sanitizeXMLNamespaces(data)
		reader = bytes.NewReader(data)
	}

	res, err := decode(reader)
	if err != nil {
		log.Fatalf("failed to decode %s: %v", f.Name, err)
	}

	return res
}

func decodeR4ResourceXML(r io.Reader) (model.Resource, error) {
	var resource r4.ContainedResource
	err := xml.NewDecoder(r).Decode(&resource)
	return resource.Resource, err
}

func decodeR4ResourceJSON(r io.Reader) (model.Resource, error) {
	var resource r4.ContainedResource
	err := json.NewDecoder(r).Decode(&resource)
	return resource.Resource, err
}

func decodeR4BResourceXML(r io.Reader) (model.Resource, error) {
	var resource r4b.ContainedResource
	err := xml.NewDecoder(r).Decode(&resource)
	return resource.Resource, err
}

func decodeR4BResourceJSON(r io.Reader) (model.Resource, error) {
	var resource r4b.ContainedResource
	err := json.NewDecoder(r).Decode(&resource)
	return resource.Resource, err
}

func decodeR5ResourceXML(r io.Reader) (model.Resource, error) {
	var resource r5.ContainedResource
	err := xml.NewDecoder(r).Decode(&resource)
	return resource.Resource, err
}

func decodeR5ResourceJSON(r io.Reader) (model.Resource, error) {
	var resource r5.ContainedResource
	err := json.NewDecoder(r).Decode(&resource)
	return resource.Resource, err
}

type FHIRPathTests struct {
	Name        string               `xml:"name,attr"`
	Description string               `xml:"description,attr"`
	Groups      []*FHIRPathTestGroup `xml:"group"`
}

type FHIRPathTestGroup struct {
	Name        string         `xml:"name,attr"`
	Description string         `xml:"description,attr"`
	Tests       []FHIRPathTest `xml:"test"`
}

type FHIRPathTest struct {
	Name          string `xml:"name,attr"`
	Description   string `xml:"description,attr"`
	InputFile     string `xml:"inputfile,attr"`
	Mode          string `xml:"mode,attr"`
	InputResource model.Resource
	Predicate     bool                   `xml:"predicate,attr"`
	Invalid       string                 `xml:"invalid,attr"`
	Expression    FHIRPathTestExpression `xml:"expression"`
	Output        []FHIRPathTestOutput   `xml:"output"`
}

func (t FHIRPathTest) OutputCollection() fhirpath.Collection {
	var c fhirpath.Collection
	for _, o := range t.Output {
		c = append(c, o)
	}
	return c
}

type FHIRPathTestExpression struct {
	Invalid    string `xml:"invalid,attr"`
	Expression string `xml:",chardata"`
}

type FHIRPathTestOutput struct {
	Type   string `xml:"type,attr"`
	Output string `xml:",chardata"`
}

func (o *FHIRPathTestOutput) inferTypeFromValue() {
	if o.Type != "" {
		return
	}

	value := strings.TrimSpace(o.Output)
	if value == "" {
		return
	}

	if strings.HasPrefix(value, "@T") {
		if _, err := fhirpath.ParseTime(value); err == nil {
			o.Type = "time"
			return
		}
	}

	if strings.HasPrefix(value, "@") {
		if _, err := fhirpath.ParseDateTime(value); err == nil {
			o.Type = "dateTime"
			return
		}
		if _, err := fhirpath.ParseDate(value); err == nil {
			o.Type = "date"
			return
		}
	}

	if _, err := fhirpath.ParseQuantity(value); err == nil {
		o.Type = "Quantity"
		return
	}

	switch strings.ToLower(value) {
	case "true", "false":
		o.Type = "boolean"
		return
	}

	if strings.HasPrefix(value, "'") && strings.HasSuffix(value, "'") && len(value) >= 2 {
		o.Type = "string"
		return
	}

	if strings.ContainsAny(value, ".eE") {
		o.Type = "decimal"
		return
	}

	if _, err := strconv.Atoi(value); err == nil {
		o.Type = "integer"
		return
	}

	// Fallback to string if no other type matches
	o.Type = "string"
}

func (o FHIRPathTestOutput) Children(name ...string) fhirpath.Collection {
	return nil
}

func (o FHIRPathTestOutput) ToBoolean(explicit bool) (v fhirpath.Boolean, ok bool, err error) {
	if o.Type != "boolean" {
		panic("not a boolean")
	}
	b, err := strconv.ParseBool(o.Output)
	if err != nil {
		panic(err)
	}
	return fhirpath.Boolean(b), true, nil
}

func (o FHIRPathTestOutput) ToString(explicit bool) (v fhirpath.String, ok bool, err error) {
	if o.Type != "string" && o.Type != "code" && o.Type != "id" {
		panic("not a string")
	}
	return fhirpath.String(o.Output), true, nil
}

func (o FHIRPathTestOutput) ToInteger(explicit bool) (v fhirpath.Integer, ok bool, err error) {
	if o.Type != "integer" {
		panic("not an integer")
	}
	i, err := strconv.Atoi(o.Output)
	if err != nil {
		panic(err)
	}
	return fhirpath.Integer(i), true, nil
}

func (o FHIRPathTestOutput) ToLong(explicit bool) (v fhirpath.Long, ok bool, err error) {
	if o.Type != "integer" && o.Type != "long" {
		panic("not a long")
	}
	i, err := strconv.ParseInt(o.Output, 10, 64)
	if err != nil {
		panic(err)
	}
	return fhirpath.Long(i), true, nil
}

func (o FHIRPathTestOutput) ToDecimal(explicit bool) (v fhirpath.Decimal, ok bool, err error) {
	if o.Type != "decimal" {
		panic("not a decimal")
	}
	d, _, err := apd.NewFromString(o.Output)
	if err != nil {
		panic(err)
	}
	return fhirpath.Decimal{Value: d}, true, nil
}

func (o FHIRPathTestOutput) ToDate(explicit bool) (v fhirpath.Date, ok bool, err error) {
	if o.Type != "date" {
		panic("not a date")
	}
	d, err := fhirpath.ParseDate(o.Output)
	if err != nil {
		panic(err)
	}
	return d, true, nil
}

func (o FHIRPathTestOutput) ToTime(explicit bool) (v fhirpath.Time, ok bool, err error) {
	if o.Type != "time" {
		panic("not a time")
	}
	t, err := fhirpath.ParseTime(o.Output)
	if err != nil {
		panic(err)
	}
	return t, true, nil
}

func (o FHIRPathTestOutput) ToDateTime(explicit bool) (v fhirpath.DateTime, ok bool, err error) {
	if o.Type != "dateTime" {
		panic("not a dateTime")
	}
	dt, err := fhirpath.ParseDateTime(o.Output)
	if err != nil {
		panic(err)
	}
	return dt, true, nil
}

func (o FHIRPathTestOutput) ToQuantity(explicit bool) (v fhirpath.Quantity, ok bool, err error) {
	if o.Type != "Quantity" {
		panic("not a Quantity")
	}
	q, err := fhirpath.ParseQuantity(o.Output)
	if err != nil {
		panic(err)
	}
	return q, true, nil
}

func (o FHIRPathTestOutput) Equal(other fhirpath.Element) (eq bool, ok bool) {
	e, ok, err := o.toElement()
	if err != nil || !ok {
		return false, true
	}
	eq, ok = e.Equal(other)
	if ok && eq {
		return true, true
	}
	if _, isBool := e.(*fhirpath.Boolean); isBool {
		return other != nil, true
	}
	return false, ok
}

func (o FHIRPathTestOutput) Equivalent(other fhirpath.Element) bool {
	e, ok, err := o.toElement()
	if err != nil || !ok {
		return false
	}
	return e.Equivalent(other)
}

func (o FHIRPathTestOutput) TypeInfo() fhirpath.TypeInfo {
	return fhirpath.SimpleTypeInfo{
		Namespace: "System",
		Name:      o.typeName(),
		BaseType:  fhirpath.TypeSpecifier{Namespace: "System", Name: "Any"},
	}
}

func (o FHIRPathTestOutput) MarshalJSON() (out []byte, err error) {
	e, _, _ := o.toElement()
	return json.Marshal(e)
}

func (o FHIRPathTestOutput) String() string {
	e, _, _ := o.toElement()
	return e.String()
}

func (o FHIRPathTestOutput) toElement() (v fhirpath.Element, ok bool, err error) {
	switch o.Type {
	case "boolean":
		return o.ToBoolean(false)
	case "string", "code", "id":
		return o.ToString(false)
	case "integer":
		return o.ToInteger(false)
	case "decimal":
		return o.ToDecimal(false)
	case "date":
		return o.ToDate(false)
	case "time":
		return o.ToTime(false)
	case "dateTime":
		return o.ToDateTime(false)
	case "Quantity":
		return o.ToQuantity(false)
	}
	panic(fmt.Sprintf("invalid type: %s", o.Type))
}

func (o FHIRPathTestOutput) typeName() string {
	switch o.Type {
	case "boolean":
		return "Boolean"
	case "string":
		return "String"
	case "code":
		return "Code"
	case "id":
		return "Id"
	case "integer":
		return "Integer"
	case "decimal":
		return "Decimal"
	case "date":
		return "Date"
	case "time":
		return "Time"
	case "dateTime":
		return "DateTime"
	case "Quantity":
		return "Quantity"
	default:
		return strings.Title(o.Output)
	}
}
