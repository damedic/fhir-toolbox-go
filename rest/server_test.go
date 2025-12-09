package rest_test

import (
	"context"
	"fmt"

	"github.com/damedic/fhir-toolbox-go/capabilities/search"
	"github.com/damedic/fhir-toolbox-go/capabilities/update"
	"github.com/damedic/fhir-toolbox-go/model"

	// basic types have been removed; use r4 types directly
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/damedic/fhir-toolbox-go/model/gen/r4"
	"github.com/damedic/fhir-toolbox-go/rest"
	"github.com/damedic/fhir-toolbox-go/testdata/assert"
	"github.com/damedic/fhir-toolbox-go/utils/ptr"
)

// testCase represents a common structure for HTTP handler tests
type testCase struct {
	name             string
	method           string
	format           string
	resourceType     string
	resourceID       string
	queryString      string
	requestBody      string
	backend          any
	expectedStatus   int
	expectedLocation string
	expectedBody     string
}

// runTest executes a common test pattern for HTTP handlers
func runTest(t *testing.T, tc testCase) {
	server := &rest.Server[model.R4]{
		Backend: tc.backend,
	}

	var requestURL string
	if tc.queryString != "" {
		// For search requests
		requestURL = fmt.Sprintf("http://example.com/%s?%s", tc.resourceType, tc.queryString)
	} else if tc.resourceID != "" {
		// For resource-specific requests
		requestURL = fmt.Sprintf("http://example.com/%s/%s", tc.resourceType, tc.resourceID)
	} else {
		// For resource type-level requests
		requestURL = fmt.Sprintf("http://example.com/%s", tc.resourceType)
	}

	var req *http.Request
	if tc.requestBody != "" {
		req = httptest.NewRequest(tc.method, requestURL, strings.NewReader(tc.requestBody))
		req.Header.Set("Content-Type", tc.format)
		req.Header.Set("Accept", tc.format)
	} else {
		req = httptest.NewRequest(tc.method, requestURL, nil)
		if tc.format != "" {
			req.Header.Set("Accept", tc.format)
		}
	}

	// For search requests, add the test to the context
	if tc.queryString != "" {
		req = req.WithContext(context.WithValue(req.Context(), "t", t))
	}

	rr := httptest.NewRecorder()
	server.ServeHTTP(rr, req)

	if rr.Code != tc.expectedStatus {
		t.Errorf("Expected status code %d, got %d", tc.expectedStatus, rr.Code)
	}

	// Check Location header for successful creation or update
	if tc.expectedLocation != "" {
		location := rr.Header().Get("Location")
		if location != tc.expectedLocation {
			t.Errorf("Expected Location header %s, got %s", tc.expectedLocation, location)
		}
	}

	// Skip content type and body checks if expected body is empty
	if tc.expectedBody == "" {
		return
	}

	assertResponse(t, tc.format, tc.expectedBody, rr)
}

func assertResponse(t *testing.T, format, expectedBody string, rr *httptest.ResponseRecorder) {
	if strings.Contains(format, "json") {
		contentType := rr.Header().Get("Content-Type")
		if contentType != "application/fhir+json" {
			t.Errorf("Expected Content-Type %s, got %s", "application/fhir+json", contentType)
		}
		if expectedBody != "" {
			assert.JSONEqual(t, expectedBody, rr.Body.String())
		}
	} else if strings.Contains(format, "xml") {
		contentType := rr.Header().Get("Content-Type")
		if contentType != "application/fhir+xml" {
			t.Errorf("Expected Content-Type %s, got %s", "application/fhir+xml", contentType)
		}
		if expectedBody != "" {
			assert.XMLEqual(t, expectedBody, rr.Body.String())
		}
	}
}

func TestCapabilityStatement(t *testing.T) {
	var tests = []struct {
		name           string
		format         string
		date           string
		expectedStatus int
		expectedBody   string
	}{
		{
			name:           "CapabilityStatement JSON",
			format:         "application/fhir+json",
			date:           "2024-11-28T11:25:27+01:00",
			expectedStatus: http.StatusOK,
			expectedBody: `{
			  "date": "2024-11-28T11:25:27+01:00",
			  "fhirVersion": "4.0",
			  "format": [
				"xml",
				"json"
			  ],
			  "implementation": {
				"description": "a simple FHIR service built with fhir-toolbox-go",
				"url": "http://example.com"
			  },
			  "kind": "instance",
			  "resourceType": "CapabilityStatement",
            "rest": [
                {
                  "mode": "server",
                  "operation": [
                    {
                      "definition": "http://example.com/OperationDefinition/ping",
                      "name": "ping"
                    }
                  ],
                  "resource": [
					{
					  "interaction": [
						{
						  "code": "search-type"
						}
					  ],
					  "searchInclude": [
						"Observation:patient"
					  ],
					  "searchParam": [
						{
						  "definition": "http://example.com/SearchParameter/Observation-id",
						  "name": "_id",
						  "type": "token"
						}
					  ],
					  "type": "Observation"
					},
					{
					  "interaction": [
						{
						  "code": "create"
						},
						{
						  "code": "read"
						},
						{
						  "code": "update"
						},
						{
						  "code": "delete"
						},
						{
						  "code": "search-type"
						}
					  ],
					  "searchParam": [
						{
						  "definition": "http://example.com/SearchParameter/Patient-id",
						  "name": "_id",
						  "type": "token"
						},
						{
						  "definition": "http://example.com/SearchParameter/Patient-date",
						  "name": "date",
						  "type": "date"
						},
						{
						  "definition": "http://example.com/SearchParameter/Patient-eb8",
						  "name": "eb8",
						  "type": "token"
						},
						{
						  "definition": "http://example.com/SearchParameter/Patient-eq1",
						  "name": "eq1",
						  "type": "token"
						},
						{
						  "definition": "http://example.com/SearchParameter/Patient-ge5",
						  "name": "ge5",
						  "type": "token"
						},
						{
						  "definition": "http://example.com/SearchParameter/Patient-gt3",
						  "name": "gt3",
						  "type": "token"
						},
						{
						  "definition": "http://example.com/SearchParameter/Patient-le6",
						  "name": "le6",
						  "type": "token"
						},
						{
						  "definition": "http://example.com/SearchParameter/Patient-lt4",
						  "name": "lt4",
						  "type": "token"
						},
						{
						  "definition": "http://example.com/SearchParameter/Patient-ne2",
						  "name": "ne2",
						  "type": "token"
						},
						{
						  "definition": "http://example.com/SearchParameter/Patient-pre",
						  "name": "pre",
						  "type": "token"
						},
						{
						  "definition": "http://example.com/SearchParameter/Patient-sa7",
						  "name": "sa7",
						  "type": "token"
						}
					  ],
                      "type": "Patient",
                      "operation": [
                        {
                          "definition": "http://example.com/OperationDefinition/echo",
                          "name": "echo"
                        },
                        {
                          "definition": "http://example.com/OperationDefinition/hello",
                          "name": "hello"
                        }
                      ],
                      "updateCreate": false
					},
					{
					  "interaction": [
						{
						  "code": "read"
						},
						{
						  "code": "search-type"
						}
					  ],
					  "searchParam": [
						{
						  "definition": "http://example.com/SearchParameter/SearchParameter-id",
						  "name": "_id",
						  "type": "token"
						}
					  ],
					  "type": "SearchParameter"
					}
				  ]
				}
			  ],
			  "software": {
				"name": "fhir-toolbox-go"
			  },
			  "status": "active"
			}`,
		},
		{
			name:           "CapabilityStatement XML",
			format:         "application/fhir+xml",
			date:           "2024-11-28T11:25:27+01:00",
			expectedStatus: http.StatusOK,
			expectedBody: `<?xml version="1.0" encoding="UTF-8"?>
				<CapabilityStatement xmlns='http://hl7.org/fhir'>
				  <status value='active'/>
				  <date value='2024-11-28T11:25:27+01:00'/>
				  <kind value='instance'/>
				  <software>
					<name value='fhir-toolbox-go'/>
				  </software>
				  <implementation>
					<description value='a simple FHIR service built with fhir-toolbox-go'/>
					<url value='http://example.com'/>
				  </implementation>
				  <fhirVersion value='4.0'/>
				  <format value='xml'/>
				  <format value='json'/>
                  <rest>
                    <mode value='server'/>
                    <resource>
                      <type value='Observation'/>
					  <interaction>
						<code value='search-type'/>
					  </interaction>
					  <searchInclude value='Observation:patient'/>
					  <searchParam>
						<name value='_id'/>
						<definition value='http://example.com/SearchParameter/Observation-id'/>
						<type value='token'/>
					  </searchParam>
					</resource>
                    <resource>
                      <type value='Patient'/>
                      <interaction>
                        <code value='create'/>
					  </interaction>
					  <interaction>
						<code value='read'/>
					  </interaction>
					  <interaction>
						<code value='update'/>
					  </interaction>
					  <interaction>
						<code value='delete'/>
					  </interaction>
					  <interaction>
						<code value='search-type'/>
					  </interaction>
                      <updateCreate value="false"></updateCreate>
					  <searchParam>
						<name value='_id'/>
						<definition value='http://example.com/SearchParameter/Patient-id'/>
						<type value='token'/>
					  </searchParam>
					  <searchParam>
						<name value='date'/>
						<definition value='http://example.com/SearchParameter/Patient-date'/>
						<type value='date'/>
					  </searchParam>
					  <searchParam>
						<name value='eb8'/>
						<definition value='http://example.com/SearchParameter/Patient-eb8'/>
						<type value='token'/>
					  </searchParam>
					  <searchParam>
						<name value='eq1'/>
						<definition value='http://example.com/SearchParameter/Patient-eq1'/>
						<type value='token'/>
					  </searchParam>
					  <searchParam>
						<name value='ge5'/>
						<definition value='http://example.com/SearchParameter/Patient-ge5'/>
						<type value='token'/>
					  </searchParam>
					  <searchParam>
						<name value='gt3'/>
						<definition value='http://example.com/SearchParameter/Patient-gt3'/>
						<type value='token'/>
					  </searchParam>
					  <searchParam>
						<name value='le6'/>
						<definition value='http://example.com/SearchParameter/Patient-le6'/>
						<type value='token'/>
					  </searchParam>
					  <searchParam>
						<name value='lt4'/>
						<definition value='http://example.com/SearchParameter/Patient-lt4'/>
						<type value='token'/>
					  </searchParam>
					  <searchParam>
						<name value='ne2'/>
						<definition value='http://example.com/SearchParameter/Patient-ne2'/>
						<type value='token'/>
					  </searchParam>
					  <searchParam>
						<name value='pre'/>
						<definition value='http://example.com/SearchParameter/Patient-pre'/>
						<type value='token'/>
					  </searchParam>
                      <searchParam>
                        <name value='sa7'/>
                        <definition value='http://example.com/SearchParameter/Patient-sa7'/>
                        <type value='token'/>
                      </searchParam>
                      <operation>
                        <name value='echo'/>
                        <definition value='http://example.com/OperationDefinition/echo'/>
                      </operation>
                      <operation>
                        <name value='hello'/>
                        <definition value='http://example.com/OperationDefinition/hello'/>
                      </operation>
                    </resource>
					<resource>
                      <type value="SearchParameter"/>
                      <interaction>
                        <code value="read"/>
                      </interaction>
                      <interaction>
                        <code value="search-type"/>
                      </interaction>
                      <searchParam>
                        <name value="_id"/>
                        <definition value="http://example.com/SearchParameter/SearchParameter-id"/>
                        <type value="token"/>
                      </searchParam>
                  </resource>
                  <operation>
                    <name value='ping'/>
                    <definition value='http://example.com/OperationDefinition/ping'/>
                  </operation>
                  </rest>
				</CapabilityStatement>`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := &rest.Server[model.R4]{
				Backend: mockBackend{},
			}

			req := httptest.NewRequest("GET", "http://example.com/metadata", nil)
			req.Header.Set("Accept", tt.format)

			rr := httptest.NewRecorder()
			server.ServeHTTP(rr, req)

			if rr.Code != tt.expectedStatus {
				t.Errorf("Expected status code %d, got %d", tt.expectedStatus, rr.Code)
			}

			assertResponse(t, tt.format, tt.expectedBody, rr)
		})
	}
}

func TestHandleCreate(t *testing.T) {
	var tests = []testCase{
		{
			name:             "server assigned id",
			format:           "application/fhir+json",
			resourceType:     "Patient",
			requestBody:      `{ "resourceType": "Patient", "name": [{ "family": "Smith" }] }`,
			backend:          mockBackend{},
			expectedStatus:   http.StatusCreated,
			expectedLocation: "http://example.com/Patient/server-assigned-id",
			expectedBody: `{
				"resourceType": "Patient",
				"id": "server-assigned-id",
				"name": [{ "family": "Smith" }] 
			}`,
		},
		{
			name:             "client provided id ignored",
			format:           "application/fhir+json",
			resourceType:     "Patient",
			requestBody:      `{ "resourceType": "Patient", "id": "client-id", "name": [{ "family": "Jones" }] }`,
			backend:          mockBackend{},
			expectedStatus:   http.StatusCreated,
			expectedLocation: "http://example.com/Patient/server-assigned-id",
			expectedBody: `{
				"resourceType": "Patient",
				"id": "server-assigned-id",
				"name": [{ "family": "Jones" }] 
			}`,
		},
		{
			name:             "xml format",
			format:           "application/fhir+xml",
			resourceType:     "Patient",
			requestBody:      `<?xml version="1.0" encoding="UTF-8"?><Patient xmlns="http://hl7.org/fhir"><name><family value="Smith"/></name></Patient>`,
			backend:          mockBackend{},
			expectedStatus:   http.StatusCreated,
			expectedLocation: "http://example.com/Patient/server-assigned-id",
			expectedBody: `<?xml version="1.0" encoding="UTF-8"?>
				<Patient xmlns="http://hl7.org/fhir">
					<id value="server-assigned-id"/>
					<name>
						<family value="Smith"/>
					</name>
				</Patient>`,
		},
		{
			name:           "not implemented resource type",
			format:         "application/fhir+json",
			resourceType:   "Observation",
			requestBody:    `{ "resourceType": "Observation", "status": "final", "code": {} }`,
			backend:        mockBackend{},
			expectedStatus: http.StatusNotImplemented,
			expectedBody: `{
				"resourceType": "OperationOutcome",
				"issue": [
					{
						"severity": "fatal",
						"code": "not-supported",
						"diagnostics": "create not implemented for Observation"
					}
				]
			}`,
		},
		{
			name:           "invalid resource",
			format:         "application/fhir+json",
			resourceType:   "UnknownType",
			requestBody:    `{ "resourceType": "UnknownType" }`,
			backend:        mockBackend{},
			expectedStatus: http.StatusBadRequest,
			expectedBody: `{
				"resourceType": "OperationOutcome",
				"issue": [
					{
						"severity": "fatal",
						"code": "processing",
						"diagnostics": "error parsing json body"
					}
				]
			}`,
		},
		{
			name:           "invalid json body",
			format:         "application/fhir+json",
			resourceType:   "Patient",
			requestBody:    `{ "resourceType": "Patient", "name": [{ "family": "Smith" }] `,
			backend:        mockBackend{},
			expectedStatus: http.StatusBadRequest,
			expectedBody: `{
				"resourceType": "OperationOutcome",
				"issue": [
					{
						"severity": "fatal",
						"code": "processing",
						"diagnostics": "error parsing json body"
					}
				]
			}`,
		},
		{
			name:           "mismatched resource type",
			format:         "application/fhir+json",
			resourceType:   "Patient",
			requestBody:    `{ "resourceType": "Observation", "status": "final", "code": {} }`,
			backend:        mockBackend{},
			expectedStatus: http.StatusBadRequest,
			expectedBody: `{
				"resourceType": "OperationOutcome",
				"issue": [
					{
						"severity": "fatal",
						"code": "processing",
						"diagnostics": "unexpected resource: expected Patient, got Observation"
					}
				]
			}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.method = "POST"
			runTest(t, tt)
		})
	}
}

func TestHandleRead(t *testing.T) {
	var tests = []testCase{
		{
			name:           "valid JSON resource",
			format:         "application/fhir+json",
			resourceType:   "Patient",
			resourceID:     "1",
			backend:        mockBackend{mockPatients: []r4.Patient{{Id: &r4.Id{Value: ptr.To("1")}}}},
			expectedStatus: http.StatusOK,
			expectedBody: `{
				"resourceType": "Patient",
				"id": "1"
			}`,
		},
		{
			name:           "valid JSON resource (incomplete format I)",
			format:         "application/json",
			resourceType:   "Patient",
			resourceID:     "1",
			backend:        mockBackend{mockPatients: []r4.Patient{{Id: &r4.Id{Value: ptr.To("1")}}}},
			expectedStatus: http.StatusOK,
			expectedBody: `{
				"resourceType": "Patient",
				"id": "1"
			}`,
		},
		{
			name:           "valid JSON resource (incomplete format II)",
			format:         "text/json",
			resourceType:   "Patient",
			resourceID:     "1",
			backend:        mockBackend{mockPatients: []r4.Patient{{Id: &r4.Id{Value: ptr.To("1")}}}},
			expectedStatus: http.StatusOK,
			expectedBody: `{
				"resourceType": "Patient",
				"id": "1"
			}`,
		},
		{
			name:           "valid JSON resource (incomplete format III)",
			format:         "json",
			resourceType:   "Patient",
			resourceID:     "1",
			backend:        mockBackend{mockPatients: []r4.Patient{{Id: &r4.Id{Value: ptr.To("1")}}}},
			expectedStatus: http.StatusOK,
			expectedBody: `{
				"resourceType": "Patient",
				"id": "1"
			}`,
		},
		{
			name:           "valid XML resource",
			format:         "application/fhir+xml",
			resourceType:   "Patient",
			resourceID:     "1",
			backend:        mockBackend{mockPatients: []r4.Patient{{Id: &r4.Id{Value: ptr.To("1")}}}},
			expectedStatus: http.StatusOK,
			expectedBody: `<?xml version="1.0" encoding="UTF-8"?>
				<Patient xmlns="http://hl7.org/fhir">
					<id value="1"/>
            	</Patient>`,
		},
		{
			name:           "valid XML resource (incomplete format I)",
			format:         "application/xml",
			resourceType:   "Patient",
			resourceID:     "1",
			backend:        mockBackend{mockPatients: []r4.Patient{{Id: &r4.Id{Value: ptr.To("1")}}}},
			expectedStatus: http.StatusOK,
			expectedBody: `<?xml version="1.0" encoding="UTF-8"?>
				<Patient xmlns="http://hl7.org/fhir">
					<id value="1"/>
            	</Patient>`,
		},
		{
			name:           "valid XML resource (incomplete format II)",
			format:         "text/xml",
			resourceType:   "Patient",
			resourceID:     "1",
			backend:        mockBackend{mockPatients: []r4.Patient{{Id: &r4.Id{Value: ptr.To("1")}}}},
			expectedStatus: http.StatusOK,
			expectedBody: `<?xml version="1.0" encoding="UTF-8"?>
				<Patient xmlns="http://hl7.org/fhir">
					<id value="1"/>
            	</Patient>`,
		},
		{
			name:           "valid XML resource (incomplete format III)",
			format:         "xml",
			resourceType:   "Patient",
			resourceID:     "1",
			backend:        mockBackend{mockPatients: []r4.Patient{{Id: &r4.Id{Value: ptr.To("1")}}}},
			expectedStatus: http.StatusOK,
			expectedBody: `<?xml version="1.0" encoding="UTF-8"?>
				<Patient xmlns="http://hl7.org/fhir">
					<id value="1"/>
            	</Patient>`,
		},
		{
			name:           "invalid resource type",
			format:         "application/fhir+json",
			resourceType:   "UnknownType",
			resourceID:     "1",
			backend:        mockBackend{},
			expectedStatus: http.StatusBadRequest,
			expectedBody: `{
				"resourceType": "OperationOutcome",
				"issue": [
					{
						"severity": "fatal",
						"code": "processing",
						"diagnostics":"invalid resource type: UnknownType"
					}
				]
			}`,
		},
		{
			name:           "invalid resource id",
			format:         "application/fhir+json",
			resourceType:   "Patient",
			resourceID:     "unknown",
			backend:        mockBackend{},
			expectedStatus: http.StatusNotFound,
			expectedBody: `{
			    "resourceType": "OperationOutcome",
			    "issue": [
				  {
					  "severity": "error",
					  "code": "not-found",
					  "diagnostics": "Patient with ID unknown not found"
				  }
			    ]
			}`,
		},
		{
			name:           "invalid resource id",
			format:         "application/fhir+json",
			resourceType:   "Patient",
			resourceID:     "unknown",
			backend:        mockBackend{},
			expectedStatus: http.StatusNotFound,
			expectedBody: `{
			  	"resourceType": "OperationOutcome",
			  	"issue": [
					{
						"severity": "error",
					  	"code": "not-found",
					  	"diagnostics": "Patient with ID unknown not found"
					}
			  	]
			}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.method = "GET"
			runTest(t, tt)
		})
	}
}

func TestHandleUpdate(t *testing.T) {
	var tests = []testCase{
		{
			name:             "successful update",
			format:           "application/fhir+json",
			resourceType:     "Patient",
			resourceID:       "1",
			requestBody:      `{ "resourceType": "Patient", "id": "1", "name": [{ "family": "Smith" }] }`,
			backend:          mockBackend{},
			expectedStatus:   http.StatusOK,
			expectedLocation: "http://example.com/Patient/1",
			expectedBody: `{
				"resourceType": "Patient",
				"id": "1",
				"name": [{ "family": "Smith" }] 
			}`,
		},
		{
			name:             "resource creation via update",
			format:           "application/fhir+json",
			resourceType:     "Patient",
			resourceID:       "new-resource",
			requestBody:      `{ "resourceType": "Patient", "id": "new-resource", "name": [{ "family": "Jones" }] }`,
			backend:          mockBackend{},
			expectedStatus:   http.StatusCreated,
			expectedLocation: "http://example.com/Patient/new-resource",
			expectedBody: `{
				"resourceType": "Patient",
				"id": "new-resource",
				"name": [{ "family": "Jones" }] 
			}`,
		},
		{
			name:           "id mismatch",
			format:         "application/fhir+json",
			resourceType:   "Patient",
			resourceID:     "1",
			requestBody:    `{ "resourceType": "Patient", "id": "2", "name": [{ "family": "Smith" }] }`,
			backend:        mockBackend{},
			expectedStatus: http.StatusBadRequest,
			expectedBody: `{
				"resourceType": "OperationOutcome",
				"issue": [
					{
						"severity": "fatal",
						"code": "processing",
						"diagnostics": "resource ID in URL (1) does not match resource ID in body (2)"
					}
				]
			}`,
		},
		{
			name:           "missing id in body",
			format:         "application/fhir+json",
			resourceType:   "Patient",
			resourceID:     "1",
			requestBody:    `{ "resourceType": "Patient", "name": [{ "family": "Smith" }] }`,
			backend:        mockBackend{},
			expectedStatus: http.StatusBadRequest,
			expectedBody: `{
				"resourceType": "OperationOutcome",
				"issue": [
					{
						"severity": "fatal",
						"code": "processing",
						"diagnostics": "resource ID in URL (1) does not match resource ID in body ()"
					}
				]
			}`,
		},
		{
			name:           "not implemented resource type",
			format:         "application/fhir+json",
			resourceType:   "Observation",
			resourceID:     "1",
			requestBody:    `{ "resourceType": "Observation", "id": "1", "status": "final", "code": {} }`,
			backend:        mockBackend{},
			expectedStatus: http.StatusNotImplemented,
			expectedBody: `{
				"resourceType": "OperationOutcome",
				"issue": [
					{
						"severity": "fatal",
						"code": "not-supported",
						"diagnostics": "update not implemented for Observation"
					}
				]
			}`,
		},
		{
			name:           "invalid resource",
			format:         "application/fhir+json",
			resourceType:   "UnknownType",
			resourceID:     "1",
			requestBody:    `{ "resourceType": "UnknownType", "id": "1" }`,
			backend:        mockBackend{},
			expectedStatus: http.StatusBadRequest,
			expectedBody: `{
				"resourceType": "OperationOutcome",
				"issue": [
					{
						"severity": "fatal",
						"code": "processing",
						"diagnostics": "error parsing json body"
					}
				]
			}`,
		},
		{
			name:           "mismatched resource type",
			format:         "application/fhir+json",
			resourceType:   "Patient",
			resourceID:     "1",
			requestBody:    `{ "resourceType": "Observation", "id": "1", "status": "final", "code": {} }`,
			backend:        mockBackend{},
			expectedStatus: http.StatusBadRequest,
			expectedBody: `{
				"resourceType": "OperationOutcome",
				"issue": [
					{
						"severity": "fatal",
						"code": "processing",
						"diagnostics": "unexpected resource: expected Patient, got Observation"
					}
				]
			}`,
		},
		{
			name:             "xml format",
			format:           "application/fhir+xml",
			resourceType:     "Patient",
			resourceID:       "1",
			requestBody:      `<?xml version="1.0" encoding="UTF-8"?><Patient xmlns="http://hl7.org/fhir"><id value="1"/><name><family value="Smith"/></name></Patient>`,
			backend:          mockBackend{},
			expectedStatus:   http.StatusOK,
			expectedLocation: "http://example.com/Patient/1",
			expectedBody: `<?xml version="1.0" encoding="UTF-8"?>
				<Patient xmlns="http://hl7.org/fhir">
					<id value="1"/>
					<name>
						<family value="Smith"/>
					</name>
				</Patient>`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.method = "PUT"
			runTest(t, tt)
		})
	}
}

func TestHandleDelete(t *testing.T) {
	var tests = []testCase{
		{
			name:           "successful delete",
			format:         "application/fhir+json",
			resourceType:   "Patient",
			resourceID:     "1",
			backend:        mockBackend{},
			expectedStatus: http.StatusNoContent,
			expectedBody:   "",
		},
		{
			name:           "successful delete with XML format",
			format:         "application/fhir+xml",
			resourceType:   "Patient",
			resourceID:     "1",
			backend:        mockBackend{},
			expectedStatus: http.StatusNoContent,
			expectedBody:   "",
		},
		{
			name:           "resource not found",
			format:         "application/fhir+json",
			resourceType:   "Patient",
			resourceID:     "nonexistent",
			backend:        mockBackend{deleteErrorMode: "not-found"},
			expectedStatus: http.StatusNotFound,
			expectedBody: `{
				"resourceType": "OperationOutcome",
				"issue": [
					{
						"severity": "error",
						"code": "not-found",
						"diagnostics": "Patient with ID nonexistent not found"
					}
				]
			}`,
		},
		{
			name:           "invalid resource type",
			format:         "application/fhir+json",
			resourceType:   "UnknownType",
			resourceID:     "1",
			backend:        mockBackend{deleteErrorMode: "invalid-type"},
			expectedStatus: http.StatusBadRequest,
			expectedBody: `{
				"resourceType": "OperationOutcome",
				"issue": [
					{
						"severity": "error",
						"code": "invalid",
						"diagnostics": "invalid resource type: UnknownType"
					}
				]
			}`,
		},
		{
			name:           "server error",
			format:         "application/fhir+json",
			resourceType:   "Patient",
			resourceID:     "1",
			backend:        mockBackend{deleteErrorMode: "server-error"},
			expectedStatus: http.StatusInternalServerError,
			expectedBody: `{
				"resourceType": "OperationOutcome",
				"issue": [
					{
						"severity": "error",
						"code": "exception",
						"diagnostics": "internal server error"
					}
				]
			}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.method = "DELETE"
			runTest(t, tt)
		})
	}
}

func TestHandleSearch(t *testing.T) {
	var tests = []testCase{
		{
			name:         "valid bundle single entry",
			format:       "application/fhir+json",
			resourceType: "Patient",
			queryString:  "_id=1",
			backend: mockBackend{
				mockPatients: []r4.Patient{
					{Id: &r4.Id{Value: ptr.To("1")}},
				},
			},
			expectedStatus: http.StatusOK,
			expectedBody: `{
				"resourceType":"Bundle",
				"type":"searchset",
				"entry":[
					{
						"fullUrl":"http://example.com/Patient/1",
						"resource":{
							"resourceType":"Patient",
							"id":"1"
						},
						"search":{
							"mode":"match"
						}
					}
				],
				"link":[
					{
						"relation":"self",
						"url":"http://example.com/Patient?_id=1&_count=500"
					}
				]
			}`,
		},
		{
			name:         "valid bundle two entries with or parameter",
			format:       "application/fhir+json",
			resourceType: "Patient",
			queryString:  "_id=1,2",
			backend: mockBackend{
				mockPatients: []r4.Patient{
					{Id: &r4.Id{Value: ptr.To("1")}},
					{Id: &r4.Id{Value: ptr.To("2")}},
				},
			},
			expectedStatus: http.StatusOK,
			expectedBody: `{
				"resourceType":"Bundle",
				"type":"searchset",
				"entry":[
					{
						"fullUrl":"http://example.com/Patient/1",
						"resource":{
							"resourceType":"Patient",
							"id":"1"
						},
						"search":{
							"mode":"match"
						}
					},
					{
						"fullUrl":"http://example.com/Patient/2",
						"resource":{
							"resourceType":"Patient",
							"id":"2"
						},
						"search":{
							"mode":"match"
						}
					}
				],
				"link":[
					{
						"relation":"self",
						"url":"http://example.com/Patient?_id=1%2C2&_count=500"
					}
				]
			}`,
		},
		{
			name:         "valid bundle two entries with and parameter",
			format:       "application/fhir+json",
			resourceType: "Patient",
			queryString:  "_id=1&_id=2",
			backend: mockBackend{
				mockPatients: []r4.Patient{
					{Id: &r4.Id{Value: ptr.To("1")}},
					{Id: &r4.Id{Value: ptr.To("2")}},
				},
			},
			expectedStatus: http.StatusOK,
			expectedBody: `{
				"resourceType":"Bundle",
				"type":"searchset",
				"entry":[
					{
						"fullUrl":"http://example.com/Patient/1",
						"resource":{
							"resourceType":"Patient",
							"id":"1"
						},
						"search":{
							"mode":"match"
						}
					},
					{
						"fullUrl":"http://example.com/Patient/2",
						"resource":{
							"resourceType":"Patient",
							"id":"2"
						},
						"search":{
							"mode":"match"
						}
					}
				],
				"link":[
					{
						"relation":"self",
						"url":"http://example.com/Patient?_id=1&_id=2&_count=500"
					}
				]
			}`,
		},
		{
			name:         "valid bundle with unknown parameter",
			format:       "application/fhir+json",
			resourceType: "Patient",
			queryString:  "_id=1&unknown=x",
			backend: mockBackend{
				mockPatients: []r4.Patient{
					{Id: &r4.Id{Value: ptr.To("1")}},
				},
			},
			expectedStatus: http.StatusOK,
			expectedBody: `{
				"resourceType":"Bundle",
				"type":"searchset",
				"entry":[
					{
						"fullUrl":"http://example.com/Patient/1",
						"resource":{
							"resourceType":"Patient",
							"id":"1"
						},
						"search":{
							"mode":"match"
						}
					}
				],
				"link":[
					{
						"relation":"self",
						"url":"http://example.com/Patient?_id=1&_count=500"
					}
				]
			}`,
		},
		{
			name:         "valid bundle with include parameter",
			format:       "application/fhir+json",
			resourceType: "Observation",
			queryString:  "_include=Observation:patient&_id=1",
			backend: mockBackend{
				mockObservations: []r4.Observation{
					{Id: &r4.Id{Value: ptr.To("1")}, Status: r4.Code{Value: ptr.To("final")}},
				},
				mockObservationIncludes: []model.Resource{
					r4.Patient{Id: &r4.Id{Value: ptr.To("1")}},
				},
			},
			expectedStatus: http.StatusOK,
			expectedBody: `{
				"resourceType":"Bundle",
				"type":"searchset",
				"entry":[
					{
						"fullUrl":"http://example.com/Observation/1",
						"resource":{
							"resourceType":"Observation",
							"id":"1",
							"status":"final",
							"code":{}
						},
						"search":{
							"mode":"match"
						}
					},
					{
						"fullUrl":"http://example.com/Patient/1",
						"resource":{
							"resourceType":"Patient",
							"id":"1"
						},
						"search":{
							"mode":"include"
						}
					}
				],
				"link":[
					{
						"relation":"self",
						"url":"http://example.com/Observation?_id=1&_include=Observation%3Apatient&_count=500"
					}
				]
			}`,
		},
		{
			name:           "invalid ResourceType",
			format:         "application/fhir+json",
			resourceType:   "UnknownType",
			queryString:    "_id=1",
			backend:        mockBackend{},
			expectedStatus: http.StatusBadRequest,
			expectedBody: `{
				"resourceType":"OperationOutcome",
				"issue":[
					{
						"severity":"fatal",
						"code":"processing",
						"diagnostics":"invalid resource type: UnknownType"
					}
				]
			}`,
		},
		{
			name:         "search with prefix",
			format:       "application/fhir+json",
			resourceType: "Patient",
			queryString:  "_id=ge1&date=ge2024-06-03",
			backend: mockBackend{
				mockPatients: []r4.Patient{
					{Id: &r4.Id{Value: ptr.To("1")}},
				},
			},
			expectedStatus: http.StatusOK,
			expectedBody: `{
				"resourceType":"Bundle",
				"type":"searchset",
				"entry":[
					{
						"fullUrl":"http://example.com/Patient/1",
						"resource":{
							"resourceType":"Patient",
							"id":"1"
						},
						"search":{
							"mode":"match"
						}
					}
				],
				"link":[
					{
						"relation":"self",
						"url":"http://example.com/Patient?_id=ge1&date=ge2024-06-03&_count=500"
					}
				]
			}`,
		},
		{
			name:         "search with all supported prefixes",
			format:       "application/fhir+json",
			resourceType: "Patient",
			queryString:  "eq1=eq1&ne2=ne2&gt3=gt3&lt4=lt4&ge5=ge5&le6=le6&sa7=sa7&eb8=eb8",
			backend: mockBackend{
				mockPatients: []r4.Patient{
					{Id: &r4.Id{Value: ptr.To("1")}},
				},
			},
			expectedStatus: http.StatusOK,
			expectedBody: `{
				"resourceType":"Bundle",
				"type":"searchset",
				"entry":[
					{
						"fullUrl":"http://example.com/Patient/1",
						"resource":{
							"resourceType":"Patient",
							"id":"1"
						},
						"search":{
							"mode":"match"
						}
					}
				],
				"link":[
					{
						"relation":"self",
						"url":"http://example.com/Patient?eb8=eb8&eq1=eq1&ge5=ge5&gt3=gt3&le6=le6&lt4=lt4&ne2=ne2&sa7=sa7&_count=500"
					}
				]
			}`,
		},
		{
			name:         "search date up to minute",
			format:       "application/fhir+json",
			resourceType: "Patient",
			queryString:  "date=ge2024-06-03T16:53Z",
			backend: mockBackend{
				mockPatients: []r4.Patient{
					{Id: &r4.Id{Value: ptr.To("1")}},
				},
			},
			expectedStatus: http.StatusOK,
			expectedBody: `{
				"resourceType":"Bundle",
				"type":"searchset",
				"entry":[
					{
						"fullUrl":"http://example.com/Patient/1",
						"resource":{
							"resourceType":"Patient",
							"id":"1"
						},
						"search":{
							"mode":"match"
						}
					}
				],
				"link":[
					{
						"relation":"self",
						"url":"http://example.com/Patient?date=ge2024-06-03T16%3A53Z&_count=500"
					}
				]
			}`,
		},
		{
			name:         "search date up to second",
			format:       "application/fhir+json",
			resourceType: "Patient",
			queryString:  "date=ge2024-06-03T16:53:23Z",
			backend: mockBackend{
				mockPatients: []r4.Patient{
					{Id: &r4.Id{Value: ptr.To("1")}},
				},
			},
			expectedStatus: http.StatusOK,
			expectedBody: `{
				"resourceType":"Bundle",
				"type":"searchset",
				"entry":[
					{
						"fullUrl":"http://example.com/Patient/1",
						"resource":{
							"resourceType":"Patient",
							"id":"1"
						},
						"search":{
							"mode":"match"
						}
					}
				],
				"link":[
					{
						"relation":"self",
						"url":"http://example.com/Patient?date=ge2024-06-03T16%3A53%3A23Z&_count=500"
					}
				]
			}`,
		},
		{
			name:         "search date full precision and timezone",
			format:       "application/fhir+json",
			resourceType: "Patient",
			queryString:  "date=ge2024-06-03T16:53:24.444%2b02:00",
			backend: mockBackend{
				mockPatients: []r4.Patient{
					{Id: &r4.Id{Value: ptr.To("1")}},
				},
			},
			expectedStatus: http.StatusOK,
			expectedBody: `{
				"resourceType":"Bundle",
				"type":"searchset",
				"entry":[
					{
						"fullUrl":"http://example.com/Patient/1",
						"resource":{
							"resourceType":"Patient",
							"id":"1"
						},
						"search":{
							"mode":"match"
						}
					}
				],
				"link":[
					{
						"relation":"self",
						"url":"http://example.com/Patient?date=ge2024-06-03T16%3A53%3A24.444%2B02%3A00&_count=500"
					}
				]
			}`,
		},
		{
			name:         "search with count and cursor",
			format:       "application/fhir+json",
			resourceType: "Patient",
			queryString:  "_count=1&_cursor=2",
			backend: mockBackend{
				mockPatients: []r4.Patient{
					{Id: &r4.Id{Value: ptr.To("1")}},
				},
			},
			expectedStatus: http.StatusOK,
			expectedBody: `{
				"resourceType":"Bundle",
				"type":"searchset",
				"entry":[
					{
						"fullUrl":"http://example.com/Patient/1",
						"resource":{
							"resourceType":"Patient",
							"id":"1"
						},
						"search":{
							"mode":"match"
						}
					}
				],
				"link":[
					{
						"relation":"self",
						"url":"http://example.com/Patient?_cursor=2&_count=1"
					}
				]
			}`,
		},
		{
			name:         "search with next link",
			format:       "application/fhir+json",
			resourceType: "Patient",
			queryString:  "_count=1",
			backend: mockBackend{
				nextCursor: "2",
				mockPatients: []r4.Patient{
					{Id: &r4.Id{Value: ptr.To("1")}},
				},
			},
			expectedStatus: http.StatusOK,
			expectedBody: `{
				"resourceType":"Bundle",
				"type":"searchset",
				"entry":[
					{
						"fullUrl":"http://example.com/Patient/1",
						"resource":{
							"resourceType":"Patient",
							"id":"1"
						},
						"search":{
							"mode":"match"
						}
					}
				],
				"link":[
					{
						"relation":"self",
						"url":"http://example.com/Patient?_count=1"
					},
					{
						"relation":"next",
						"url":"http://example.com/Patient?_cursor=2&_count=1"
					}
				]
			}`,
		},
		{
			name:         "search with modifier",
			format:       "application/fhir+json",
			resourceType: "Patient",
			queryString:  "_count=1&pre:above=x",
			backend: mockBackend{
				nextCursor: "2",
				mockPatients: []r4.Patient{
					{Id: &r4.Id{Value: ptr.To("1")}},
				},
			},
			expectedStatus: http.StatusOK,
			expectedBody: `{
				"resourceType":"Bundle",
				"type":"searchset",
				"entry":[
					{
						"fullUrl":"http://example.com/Patient/1",
						"resource":{
							"resourceType":"Patient",
							"id":"1"
						},
						"search":{
							"mode":"match"
						}
					}
				],
				"link":[
					{
						"relation":"self",
						"url":"http://example.com/Patient?pre%3Aabove=x&_count=1"
					},
					{
						"relation":"next",
						"url":"http://example.com/Patient?pre%3Aabove=x&_cursor=2&_count=1"
					}
				]
			}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.method = "GET"
			runTest(t, tt)
		})
	}
}

type mockBackend struct {
	nextCursor              search.Cursor
	mockPatients            []r4.Patient
	mockObservations        []r4.Observation
	mockObservationIncludes []model.Resource
	// Error behavior control
	deleteErrorMode string // Can be "not-found", "invalid-type", "server-error", or empty for success
}

// Implement ConcreteCapabilities interface
func (m mockBackend) CapabilityBase(ctx context.Context) (r4.CapabilityStatement, error) {
	return r4.CapabilityStatement{
		Status:      r4.Code{Value: ptr.To("active")},
		Date:        r4.DateTime{Value: ptr.To("2024-11-28T11:25:27+01:00")},
		Kind:        r4.Code{Value: ptr.To("instance")},
		FhirVersion: r4.Code{Value: ptr.To("4.0")},
		Format: []r4.Code{
			{Value: ptr.To("xml")},
			{Value: ptr.To("json")},
		},
		Software: &r4.CapabilityStatementSoftware{
			Name: r4.String{Value: ptr.To("fhir-toolbox-go")},
		},
		Implementation: &r4.CapabilityStatementImplementation{
			Description: r4.String{Value: ptr.To("a simple FHIR service built with fhir-toolbox-go")},
			Url:         &r4.Url{Value: ptr.To("http://example.com")},
		},
	}, nil
}

func (m mockBackend) CreatePatient(ctx context.Context, patient r4.Patient) (r4.Patient, error) {
	patient.Id = &r4.Id{Value: ptr.To("server-assigned-id")}
	return patient, nil
}

func (m mockBackend) ReadPatient(ctx context.Context, id string) (r4.Patient, error) {
	if len(m.mockPatients) == 0 {
		return r4.Patient{}, r4.OperationOutcome{Issue: []r4.OperationOutcomeIssue{
			{
				Severity:    r4.Code{Value: ptr.To("error")},
				Code:        r4.Code{Value: ptr.To("not-found")},
				Diagnostics: &r4.String{Value: ptr.To(fmt.Sprintf("Patient with ID %s not found", id))},
			},
		}}
	}
	return m.mockPatients[0], nil
}

func (m mockBackend) UpdatePatient(ctx context.Context, patient r4.Patient) (update.Result[r4.Patient], error) {
	// If the patient ID is "new-resource", mark it as created
	if id, ok := patient.ResourceId(); ok && id == "new-resource" {
		return update.Result[r4.Patient]{
			Resource: patient,
			Created:  true,
		}, nil
	}

	// Otherwise, it's an update
	return update.Result[r4.Patient]{
		Resource: patient,
		Created:  false,
	}, nil
}

func (m mockBackend) UpdateCapabilitiesPatient(ctx context.Context) (update.Capabilities, error) {
	return update.Capabilities{
		UpdateCreate: false,
	}, nil
}

func (m mockBackend) DeletePatient(ctx context.Context, id string) error {
	return nil
}

func (m mockBackend) SearchCapabilitiesPatient(ctx context.Context) (r4.SearchCapabilities, error) {
	return r4.SearchCapabilities{
		Parameters: map[string]r4.SearchParameter{
			"_id":  {Type: r4.SearchParamTypeToken},
			"date": {Type: r4.SearchParamTypeDate},
			"eq1":  {Type: r4.SearchParamTypeToken},
			"ne2":  {Type: r4.SearchParamTypeToken},
			"gt3":  {Type: r4.SearchParamTypeToken},
			"lt4":  {Type: r4.SearchParamTypeToken},
			"ge5":  {Type: r4.SearchParamTypeToken},
			"le6":  {Type: r4.SearchParamTypeToken},
			"sa7":  {Type: r4.SearchParamTypeToken},
			"eb8":  {Type: r4.SearchParamTypeToken},
			"pre":  {Type: r4.SearchParamTypeToken, Modifier: []r4.Code{r4.SearchModifierCodeAbove}},
		},
	}, nil
}

func (m mockBackend) SearchPatient(ctx context.Context, parameters search.Parameters, options search.Options) (search.Result[r4.Patient], error) {
	result := search.Result[r4.Patient]{}

	for _, p := range m.mockPatients {
		result.Resources = append(result.Resources, p)
	}

	if m.nextCursor != "" {
		result.Next = m.nextCursor
	}

	return result, nil
}

func (m mockBackend) SearchCapabilitiesObservation(ctx context.Context) (r4.SearchCapabilities, error) {
	return r4.SearchCapabilities{
		Parameters: map[string]r4.SearchParameter{
			"_id": {Type: r4.SearchParamTypeToken},
		},
		Includes: []string{"Observation:patient"},
	}, nil
}

func (m mockBackend) SearchObservation(ctx context.Context, parameters search.Parameters, options search.Options) (search.Result[r4.Observation], error) {
	var result search.Result[r4.Observation]

	for _, p := range m.mockObservations {
		result.Resources = append(result.Resources, p)
	}
	for _, p := range m.mockObservationIncludes {
		result.Included = append(result.Included, p)
	}

	return result, nil
}

// Operations on mockBackend
func (m mockBackend) PingOperationDefinition() r4.OperationDefinition {
	return r4.OperationDefinition{Id: &r4.Id{Value: ptr.To("ping")}, Code: r4.Code{Value: ptr.To("ping")}, System: r4.Boolean{Value: ptr.To(true)}}
}
func (m mockBackend) InvokePing(ctx context.Context, params r4.Parameters) (r4.Parameters, error) {
	return r4.Parameters{}, nil
}
func (m mockBackend) EchoOperationDefinition() r4.OperationDefinition {
	return r4.OperationDefinition{Id: &r4.Id{Value: ptr.To("echo")}, Code: r4.Code{Value: ptr.To("echo")}, Type: r4.Boolean{Value: ptr.To(true)}, Resource: []r4.Code{{Value: ptr.To("Patient")}}}
}
func (m mockBackend) InvokeEcho(ctx context.Context, resourceType string, params r4.Parameters) (r4.Patient, error) {
	return r4.Patient{}, nil
}
func (m mockBackend) HelloOperationDefinition() r4.OperationDefinition {
	return r4.OperationDefinition{Id: &r4.Id{Value: ptr.To("hello")}, Code: r4.Code{Value: ptr.To("hello")}, Instance: r4.Boolean{Value: ptr.To(true)}, Resource: []r4.Code{{Value: ptr.To("Patient")}}}
}
func (m mockBackend) InvokeHello(ctx context.Context, resourceType, id string, params r4.Parameters) (r4.Parameters, error) {
	return r4.Parameters{}, nil
}

// Implement GenericDelete interface
func (m mockBackend) Delete(ctx context.Context, resourceType, id string) error {
	switch m.deleteErrorMode {
	case "not-found":
		return r4.OperationOutcome{Issue: []r4.OperationOutcomeIssue{
			{
				Severity:    r4.Code{Value: ptr.To("error")},
				Code:        r4.Code{Value: ptr.To("not-found")},
				Diagnostics: &r4.String{Value: ptr.To(fmt.Sprintf("%s with ID %s not found", resourceType, id))},
			},
		}}
	case "invalid-type":
		return r4.OperationOutcome{Issue: []r4.OperationOutcomeIssue{
			{
				Severity:    r4.Code{Value: ptr.To("error")},
				Code:        r4.Code{Value: ptr.To("invalid")},
				Diagnostics: &r4.String{Value: ptr.To(fmt.Sprintf("invalid resource type: %s", resourceType))},
			},
		}}
	case "server-error":
		return r4.OperationOutcome{Issue: []r4.OperationOutcomeIssue{
			{
				Severity:    r4.Code{Value: ptr.To("error")},
				Code:        r4.Code{Value: ptr.To("exception")},
				Diagnostics: &r4.String{Value: ptr.To("internal server error")},
			},
		}}
	default:
		return nil
	}
}

func TestHandleOperation(t *testing.T) {
	backend := &mockBackend{}
	server := &rest.Server[model.R4]{Backend: backend}

	tests := []struct {
		name       string
		method     string
		path       string
		wantStatus int
		wantDiag   string
	}{
		{name: "system_ok", method: http.MethodGet, path: "/$ping", wantStatus: http.StatusOK},
		{name: "type_ok", method: http.MethodGet, path: "/Patient/$echo", wantStatus: http.StatusOK},
		{name: "instance_ok", method: http.MethodGet, path: "/Patient/123/$hello", wantStatus: http.StatusOK},
		{name: "missing_system", method: http.MethodGet, path: "/$missing", wantStatus: http.StatusBadRequest, wantDiag: "operation 'missing' not defined on system level"},
		{name: "missing_type", method: http.MethodGet, path: "/Observation/$echo", wantStatus: http.StatusBadRequest, wantDiag: "operation 'echo' not defined on type level"},
		{name: "missing_instance", method: http.MethodGet, path: "/Observation/1/$hello", wantStatus: http.StatusBadRequest, wantDiag: "operation 'hello' not defined on instance level"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(tt.method, "http://example.com"+tt.path, nil)
			req.Header.Set("Accept", string(rest.FormatJSON))
			rr := httptest.NewRecorder()
			server.ServeHTTP(rr, req)

			if rr.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d; body=%s", rr.Code, tt.wantStatus, rr.Body.String())
			}

			if tt.wantDiag != "" {
				if !strings.Contains(rr.Body.String(), tt.wantDiag) {
					t.Fatalf("expected diagnostics to contain %q, got %s", tt.wantDiag, rr.Body.String())
				}
			}
		})
	}
}
