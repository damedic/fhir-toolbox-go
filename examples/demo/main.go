// Short demo that serves a mock Observation resource (only read interaction).
package main

import (
	"context"
	"github.com/cockroachdb/apd/v3"
	"github.com/damedic/fhir-toolbox-go/model/gen/r5"
	"github.com/damedic/fhir-toolbox-go/utils/ptr"
	"log"
	"net/http"
	"time"

	"github.com/damedic/fhir-toolbox-go/model"
	"github.com/damedic/fhir-toolbox-go/rest"
)

// 1. Define our backend
type demoBackend struct{}

// 2. Implement CapabilityBase
func (b *demoBackend) CapabilityBase(ctx context.Context) (r5.CapabilityStatement, error) {
	return r5.CapabilityStatement{
		Status:      r5.Code{Value: ptr.To("active")},
		Date:        r5.DateTime{Value: ptr.To(time.Now().Format(time.RFC3339))},
		Kind:        r5.Code{Value: ptr.To("instance")},
		FhirVersion: r5.Code{Value: ptr.To("5.0")},
		Format: []r5.Code{
			{Value: ptr.To("xml")},
			{Value: ptr.To("json")},
		},
		Software: &r5.CapabilityStatementSoftware{
			Name:    r5.String{Value: ptr.To("fhir-toolbox-go demo")},
			Version: &r5.String{Value: ptr.To("1.0.0")},
		},
		Implementation: &r5.CapabilityStatementImplementation{
			Description: r5.Markdown{Value: ptr.To("Demo FHIR server built with fhir-toolbox-go")},
			Url:         &r5.Url{Value: ptr.To("http://localhost")},
		},
	}, nil
}

// 3. Implement your desired capabilities (interactions)
func (b *demoBackend) ReadObservation(ctx context.Context, id string) (r5.Observation, error) {
	return r5.Observation{
		Id:     &r5.Id{Value: &id},
		Status: r5.Code{Value: ptr.To("final")},
		Code: r5.CodeableConcept{
			Coding: []r5.Coding{{
				System:  &r5.Uri{Value: ptr.To("http://loinc.org")},
				Code:    &r5.Code{Value: ptr.To("8480-6")},
				Display: &r5.String{Value: ptr.To("Systolic blood pressure")},
			}},
		},
		Effective: &r5.DateTime{Value: ptr.To(time.Now().Format(time.RFC3339))},
		Value:     &r5.Quantity{Value: &r5.Decimal{Value: apd.New(120, 0)}, Unit: &r5.String{Value: ptr.To("mmHg")}},
	}, nil
}

func main() {
	// 4. Instantiate your backend
	backend := demoBackend{}

	// 5. Start your server!
	server := &rest.Server[model.R5]{
		Backend: &backend,
	}

	log.Println("listening on http://localhost")
	log.Fatal(http.ListenAndServe(":80", server))

	// 6. Visit http://localhost/Observation/1234
}
