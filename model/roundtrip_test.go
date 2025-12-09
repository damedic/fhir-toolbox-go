package model_test

import (
	"bytes"
	"encoding/json"
	"encoding/xml"
	"slices"
	"strings"
	"testing"

	"github.com/damedic/fhir-toolbox-go/model"
	"github.com/damedic/fhir-toolbox-go/model/gen/r4"
	"github.com/damedic/fhir-toolbox-go/model/gen/r4b"
	"github.com/damedic/fhir-toolbox-go/model/gen/r5"
	"github.com/damedic/fhir-toolbox-go/testdata/assert"

	"github.com/damedic/fhir-toolbox-go/testdata"
)

func TestRoundtripJSON(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	for _, release := range testdata.TestReleases {
		release := release
		releaseName := release.String()
		t.Run(releaseName, func(t *testing.T) {
			jsonExamples := testdata.GetExamples(release, "json")

			for name, jsonIn := range jsonExamples {
				t.Run(name, func(t *testing.T) {
					t.Parallel()

					switch release.(type) {
					case model.R4:
						if strings.HasSuffix(name, "-questionnaire.json") {
							t.Skip("R4 questionnaires are missing linkIds")
						}
					case model.R4B:
						if strings.HasPrefix(name, "activitydefinition-") {
							t.Skip("R4B activity definitions are lacking a null in event array")
						}
						if strings.HasPrefix(name, "plandefinition-") {
							t.Skip("R4B plan definitions are lacking a null in event array")
						}
						if slices.Contains([]string{
							"codesystem-catalogType.json",
							"valueset-catalogType.json",
							"valuesets.json",
						}, name) {
							t.Skip("R4B codesystem or valueset is lacking required status")
						}
					case model.R5:
						if name == "questionnaireresponse-example-f201-lifelines.json" {
							t.Skip("R5 questionnaire response is missing the questionnaire")
						}
					}

					var (
						r   model.Resource
						err error
					)
					switch release.(type) {
					case model.R4:
						var r4 r4.ContainedResource
						err = json.Unmarshal(jsonIn, &r4)
						r = r4
					case model.R4B:
						var r4b r4b.ContainedResource
						err = json.Unmarshal(jsonIn, &r4b)
						r = r4b
					case model.R5:
						var r5 r5.ContainedResource
						err = json.Unmarshal(jsonIn, &r5)
						r = r5
					}
					if err != nil {
						t.Fatalf("Failed to unmarshal JSON: %v", err)
					}

					jsonOut, err := json.Marshal(r)
					if err != nil {
						t.Fatalf("Failed to marshal JSON: %v", err)
					}

					assert.JSONEqual(t, string(jsonIn), string(jsonOut))
				})
			}
		})
	}
}

func TestRoundtripXML(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	for _, release := range testdata.TestReleases {
		release := release
		releaseName := release.String()
		t.Run(releaseName, func(t *testing.T) {
			xmlExamples := testdata.GetExamples(release, "xml")

			for name, xmlIn := range xmlExamples {
				t.Run(name, func(t *testing.T) {
					t.Parallel()

					switch release.(type) {
					case model.R4B:
						if name == "valuesets.xml" {
							t.Skip("R4B valuesets is lacking required status")
						}
					}

					var (
						r   model.Resource
						err error
					)
					switch release.(type) {
					case model.R4:
						var r4 r4.ContainedResource
						err = xml.Unmarshal(xmlIn, &r4)
						r = r4
					case model.R4B:
						var r4b r4b.ContainedResource
						err = xml.Unmarshal(xmlIn, &r4b)
						r = r4b
					case model.R5:
						var r5 r5.ContainedResource
						err = xml.Unmarshal(xmlIn, &r5)
						r = r5
					}
					if err != nil {
						t.Fatalf("Failed to unmarshal XML: %v", err)
					}

					xmlOut, err := xml.Marshal(r)
					if err != nil {
						t.Fatalf("Failed to marshal XML: %v", err)
					}

					// marshalled decimals look a bit different, but are semantically identical
					if name == "observation-decimal(decimal).xml" {
						xmlIn = bytes.ReplaceAll(xmlIn, []byte("1.0e0"), []byte("1.0"))
						xmlIn = bytes.ReplaceAll(xmlIn, []byte("0.00000000000000001"), []byte("1E-17"))
						xmlIn = bytes.ReplaceAll(xmlIn, []byte("0.0000000000000000000001"), []byte("1E-22"))
						xmlIn = bytes.ReplaceAll(xmlIn, []byte("e-24"), []byte("E-24"))
						xmlIn = bytes.ReplaceAll(xmlIn, []byte("e-245"), []byte("E-245"))
						xmlIn = bytes.ReplaceAll(xmlIn, []byte("e245"), []byte("E+245"))
					}

					assert.XMLEqual(t, string(xmlIn), xml.Header+string(xmlOut))
				})
			}
		})
	}
}
