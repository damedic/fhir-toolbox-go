package rest

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/damedic/fhir-toolbox-go/capabilities/search"
	"github.com/damedic/fhir-toolbox-go/model"
	"github.com/damedic/fhir-toolbox-go/model/gen/r4"
	"github.com/damedic/fhir-toolbox-go/utils/ptr"
)

// mockSearchClient implements GenericSearch for testing
type mockSearchClient struct {
	pages       []search.Result[model.Resource]
	calls       int
	lastOptions search.Options // Track the options from the last call
}

func (m *mockSearchClient) CapabilityStatement(ctx context.Context) (model.CapabilityStatement, error) {
	// Return an empty R4 capability statement to satisfy the interface
	return r4.CapabilityStatement{}, nil
}

func (m *mockSearchClient) Search(ctx context.Context, resourceType string, parameters search.Parameters, options search.Options) (search.Result[model.Resource], error) {
	// Store the options for verification in tests
	m.lastOptions = options

	// If cursor is provided, use it to determine which page to return
	if options.Cursor != "" {
		// Parse cursor as page number for simplicity
		pageNum := int(options.Cursor[0] - '0') // Simple cursor parsing for test
		if pageNum >= 0 && pageNum < len(m.pages) {
			m.calls++
			return m.pages[pageNum], nil
		}
	}

	// Return first page by default
	if len(m.pages) > 0 {
		m.calls++
		return m.pages[0], nil
	}

	return search.Result[model.Resource]{}, nil
}

func TestClientCapabilityStatement(t *testing.T) {
	tests := []struct {
		name            string
		serverResponse  string
		statusCode      int
		expectedError   bool
		validateRequest func(t *testing.T, r *http.Request)
	}{
		{
			name:       "successful_capability_statement",
			statusCode: http.StatusOK,
			serverResponse: `{
				"resourceType": "CapabilityStatement"
			}`,
			expectedError: false,
			validateRequest: func(t *testing.T, r *http.Request) {
				if r.Method != "GET" {
					t.Errorf("Expected method GET, got %s", r.Method)
				}
				if r.URL.Path != "/metadata" {
					t.Errorf("Expected path /metadata, got %s", r.URL.Path)
				}
				if r.Header.Get("Accept") != "application/fhir+json" {
					t.Errorf("Expected Accept header application/fhir+json, got %s", r.Header.Get("Accept"))
				}
			},
		},
		{
			name:          "server_error_500",
			statusCode:    http.StatusInternalServerError,
			expectedError: true,
		},
		{
			name:          "not_found_404",
			statusCode:    http.StatusNotFound,
			expectedError: true,
		},
		{
			name:           "invalid_json_response",
			statusCode:     http.StatusOK,
			serverResponse: `{invalid json}`,
			expectedError:  true,
		},
		{
			name:          "service_unavailable_503",
			statusCode:    http.StatusServiceUnavailable,
			expectedError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if tt.validateRequest != nil {
					tt.validateRequest(t, r)
				}
				w.WriteHeader(tt.statusCode)
				if tt.serverResponse != "" {
					w.Write([]byte(tt.serverResponse))
				}
			}))
			defer server.Close()

			baseURL, _ := url.Parse(server.URL)
			client := ClientR4{
				BaseURL: baseURL,
				Client:  server.Client(),
			}

			_, err := client.CapabilityStatement(context.Background())

			if tt.expectedError {
				if err == nil {
					t.Error("Expected error, but got none")
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
				}
			}
		})
	}
}

func TestClientCreate(t *testing.T) {
	tests := []struct {
		name               string
		inputResource      model.Resource
		serverResponse     string
		statusCode         int
		expectedError      bool
		expectedResourceID string
		validateRequest    func(t *testing.T, r *http.Request)
	}{
		{
			name: "successful_create",
			inputResource: &r4.Patient{
				Name: []r4.HumanName{
					{Given: []r4.String{{Value: ptr.To("John")}}},
				},
			},
			statusCode: http.StatusCreated,
			serverResponse: `{
				"resourceType": "Patient",
				"id": "123",
				"name": [{"given": ["John"]}]
			}`,
			expectedError:      false,
			expectedResourceID: "123",
			validateRequest: func(t *testing.T, r *http.Request) {
				if r.Method != "POST" {
					t.Errorf("Expected method POST, got %s", r.Method)
				}
				if r.Header.Get("Content-Type") != "application/fhir+json" {
					t.Errorf("Expected Content-Type application/fhir+json, got %s", r.Header.Get("Content-Type"))
				}
				if r.URL.Path != "/Patient" {
					t.Errorf("Expected path /Patient, got %s", r.URL.Path)
				}
			},
		},
		{
			name: "create_observation",
			inputResource: &r4.Observation{
				Status: r4.Code{Value: ptr.To("final")},
				Code: r4.CodeableConcept{
					Text: &r4.String{Value: ptr.To("Blood pressure")},
				},
			},
			statusCode: http.StatusCreated,
			serverResponse: `{
				"resourceType": "Observation",
				"id": "obs-456",
				"status": "final",
				"code": {"text": "Blood pressure"}
			}`,
			expectedError:      false,
			expectedResourceID: "obs-456",
			validateRequest: func(t *testing.T, r *http.Request) {
				if r.URL.Path != "/Observation" {
					t.Errorf("Expected path /Observation, got %s", r.URL.Path)
				}
			},
		},
		{
			name: "server_error_500",
			inputResource: &r4.Patient{
				Name: []r4.HumanName{
					{Given: []r4.String{{Value: ptr.To("John")}}},
				},
			},
			statusCode:    http.StatusInternalServerError,
			expectedError: true,
		},
		{
			name: "bad_request_400",
			inputResource: &r4.Patient{
				Name: []r4.HumanName{
					{Given: []r4.String{{Value: ptr.To("John")}}},
				},
			},
			statusCode:    http.StatusBadRequest,
			expectedError: true,
		},
		{
			name: "invalid_response_json",
			inputResource: &r4.Patient{
				Name: []r4.HumanName{
					{Given: []r4.String{{Value: ptr.To("John")}}},
				},
			},
			statusCode:     http.StatusCreated,
			serverResponse: `{invalid json}`,
			expectedError:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if tt.validateRequest != nil {
					tt.validateRequest(t, r)
				}
				w.WriteHeader(tt.statusCode)
				if tt.serverResponse != "" {
					w.Write([]byte(tt.serverResponse))
				}
			}))
			defer server.Close()

			baseURL, _ := url.Parse(server.URL)
			client := ClientR4{
				BaseURL: baseURL,
				Client:  server.Client(),
			}

			result, err := client.Create(context.Background(), tt.inputResource)

			if tt.expectedError {
				if err == nil {
					t.Error("Expected error, but got none")
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
				}
				if result != nil {
					if id, hasID := result.ResourceId(); hasID && id != tt.expectedResourceID {
						t.Errorf("Expected resource ID %s, got %s", tt.expectedResourceID, id)
					}
				}
			}
		})
	}
}

func TestClientRead(t *testing.T) {
	tests := []struct {
		name               string
		resourceType       string
		resourceID         string
		serverResponse     string
		statusCode         int
		expectedError      bool
		expectedResourceID string
		validateRequest    func(t *testing.T, r *http.Request)
	}{
		{
			name:         "successful_read_patient",
			resourceType: "Patient",
			resourceID:   "123",
			statusCode:   http.StatusOK,
			serverResponse: `{
				"resourceType": "Patient",
				"id": "123",
				"name": [{"given": ["John"]}]
			}`,
			expectedError:      false,
			expectedResourceID: "123",
			validateRequest: func(t *testing.T, r *http.Request) {
				if r.Method != "GET" {
					t.Errorf("Expected method GET, got %s", r.Method)
				}
				if r.URL.Path != "/Patient/123" {
					t.Errorf("Expected path /Patient/123, got %s", r.URL.Path)
				}
			},
		},
		{
			name:         "successful_read_observation",
			resourceType: "Observation",
			resourceID:   "obs-456",
			statusCode:   http.StatusOK,
			serverResponse: `{
				"resourceType": "Observation",
				"id": "obs-456",
				"status": "final"
			}`,
			expectedError:      false,
			expectedResourceID: "obs-456",
			validateRequest: func(t *testing.T, r *http.Request) {
				if r.URL.Path != "/Observation/obs-456" {
					t.Errorf("Expected path /Observation/obs-456, got %s", r.URL.Path)
				}
			},
		},
		{
			name:          "not_found_404",
			resourceType:  "Patient",
			resourceID:    "999",
			statusCode:    http.StatusNotFound,
			expectedError: true,
		},
		{
			name:          "server_error_500",
			resourceType:  "Patient",
			resourceID:    "123",
			statusCode:    http.StatusInternalServerError,
			expectedError: true,
		},
		{
			name:           "invalid_response_json",
			resourceType:   "Patient",
			resourceID:     "123",
			statusCode:     http.StatusOK,
			serverResponse: `{invalid json}`,
			expectedError:  true,
		},
		{
			name:          "forbidden_403",
			resourceType:  "Patient",
			resourceID:    "secret-123",
			statusCode:    http.StatusForbidden,
			expectedError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if tt.validateRequest != nil {
					tt.validateRequest(t, r)
				}
				w.WriteHeader(tt.statusCode)
				if tt.serverResponse != "" {
					w.Write([]byte(tt.serverResponse))
				}
			}))
			defer server.Close()

			baseURL, _ := url.Parse(server.URL)
			client := ClientR4{
				BaseURL: baseURL,
				Client:  server.Client(),
			}

			result, err := client.Read(context.Background(), tt.resourceType, tt.resourceID)

			if tt.expectedError {
				if err == nil {
					t.Error("Expected error, but got none")
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
				}
				if result != nil {
					if id, hasID := result.ResourceId(); hasID && id != tt.expectedResourceID {
						t.Errorf("Expected resource ID %s, got %s", tt.expectedResourceID, id)
					}
				}
			}
		})
	}
}

func TestClientUpdate(t *testing.T) {
	tests := []struct {
		name               string
		inputResource      model.Resource
		serverResponse     string
		statusCode         int
		expectedError      bool
		expectedCreated    bool
		expectedResourceID string
		validateRequest    func(t *testing.T, r *http.Request)
	}{
		{
			name: "successful_update",
			inputResource: &r4.Patient{
				Id:   &r4.Id{Value: ptr.To("123")},
				Name: []r4.HumanName{{Given: []r4.String{{Value: ptr.To("John")}}}},
			},
			statusCode: http.StatusOK,
			serverResponse: `{
				"resourceType": "Patient",
				"id": "123",
				"name": [{"given": ["John"]}]
			}`,
			expectedError:      false,
			expectedCreated:    false,
			expectedResourceID: "123",
			validateRequest: func(t *testing.T, r *http.Request) {
				if r.Method != "PUT" {
					t.Errorf("Expected method PUT, got %s", r.Method)
				}
				if r.URL.Path != "/Patient/123" {
					t.Errorf("Expected path /Patient/123, got %s", r.URL.Path)
				}
			},
		},
		{
			name: "successful_create_via_update",
			inputResource: &r4.Patient{
				Id:   &r4.Id{Value: ptr.To("456")},
				Name: []r4.HumanName{{Given: []r4.String{{Value: ptr.To("Jane")}}}},
			},
			statusCode: http.StatusCreated,
			serverResponse: `{
				"resourceType": "Patient",
				"id": "456",
				"name": [{"given": ["Jane"]}]
			}`,
			expectedError:      false,
			expectedCreated:    true,
			expectedResourceID: "456",
		},
		{
			name: "resource_without_id",
			inputResource: &r4.Patient{
				Name: []r4.HumanName{{Given: []r4.String{{Value: ptr.To("John")}}}},
			},
			expectedError: true,
		},
		{
			name: "server_error_500",
			inputResource: &r4.Patient{
				Id:   &r4.Id{Value: ptr.To("123")},
				Name: []r4.HumanName{{Given: []r4.String{{Value: ptr.To("John")}}}},
			},
			statusCode:    http.StatusInternalServerError,
			expectedError: true,
		},
		{
			name: "conflict_409",
			inputResource: &r4.Patient{
				Id:   &r4.Id{Value: ptr.To("123")},
				Name: []r4.HumanName{{Given: []r4.String{{Value: ptr.To("John")}}}},
			},
			statusCode:    http.StatusConflict,
			expectedError: true,
		},
		{
			name: "invalid_response_json",
			inputResource: &r4.Patient{
				Id:   &r4.Id{Value: ptr.To("123")},
				Name: []r4.HumanName{{Given: []r4.String{{Value: ptr.To("John")}}}},
			},
			statusCode:     http.StatusOK,
			serverResponse: `{invalid json}`,
			expectedError:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if tt.validateRequest != nil {
					tt.validateRequest(t, r)
				}
				w.WriteHeader(tt.statusCode)
				if tt.serverResponse != "" {
					w.Write([]byte(tt.serverResponse))
				}
			}))
			defer server.Close()

			baseURL, _ := url.Parse(server.URL)
			client := ClientR4{
				BaseURL: baseURL,
				Client:  server.Client(),
			}

			result, err := client.Update(context.Background(), tt.inputResource)

			if tt.expectedError {
				if err == nil {
					t.Error("Expected error, but got none")
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
				}
				if result.Created != tt.expectedCreated {
					t.Errorf("Expected created=%v, got %v", tt.expectedCreated, result.Created)
				}
				if result.Resource != nil {
					if id, hasID := result.Resource.ResourceId(); hasID && id != tt.expectedResourceID {
						t.Errorf("Expected resource ID %s, got %s", tt.expectedResourceID, id)
					}
				}
			}
		})
	}
}

func TestClientDelete(t *testing.T) {
	tests := []struct {
		name            string
		resourceType    string
		resourceID      string
		statusCode      int
		expectedError   bool
		validateRequest func(t *testing.T, r *http.Request)
	}{
		{
			name:          "successful_delete_ok",
			resourceType:  "Patient",
			resourceID:    "123",
			statusCode:    http.StatusOK,
			expectedError: false,
			validateRequest: func(t *testing.T, r *http.Request) {
				if r.Method != "DELETE" {
					t.Errorf("Expected method DELETE, got %s", r.Method)
				}
				if r.URL.Path != "/Patient/123" {
					t.Errorf("Expected path /Patient/123, got %s", r.URL.Path)
				}
			},
		},
		{
			name:          "successful_delete_no_content",
			resourceType:  "Patient",
			resourceID:    "123",
			statusCode:    http.StatusNoContent,
			expectedError: false,
		},
		{
			name:          "successful_delete_observation",
			resourceType:  "Observation",
			resourceID:    "obs-456",
			statusCode:    http.StatusOK,
			expectedError: false,
			validateRequest: func(t *testing.T, r *http.Request) {
				if r.URL.Path != "/Observation/obs-456" {
					t.Errorf("Expected path /Observation/obs-456, got %s", r.URL.Path)
				}
			},
		},
		{
			name:          "not_found_404",
			resourceType:  "Patient",
			resourceID:    "999",
			statusCode:    http.StatusNotFound,
			expectedError: true,
		},
		{
			name:          "server_error_500",
			resourceType:  "Patient",
			resourceID:    "123",
			statusCode:    http.StatusInternalServerError,
			expectedError: true,
		},
		{
			name:          "forbidden_403",
			resourceType:  "Patient",
			resourceID:    "secret-123",
			statusCode:    http.StatusForbidden,
			expectedError: true,
		},
		{
			name:          "method_not_allowed_405",
			resourceType:  "Patient",
			resourceID:    "123",
			statusCode:    http.StatusMethodNotAllowed,
			expectedError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if tt.validateRequest != nil {
					tt.validateRequest(t, r)
				}
				w.WriteHeader(tt.statusCode)
			}))
			defer server.Close()

			baseURL, _ := url.Parse(server.URL)
			client := ClientR4{
				BaseURL: baseURL,
				Client:  server.Client(),
			}

			err := client.Delete(context.Background(), tt.resourceType, tt.resourceID)

			if tt.expectedError {
				if err == nil {
					t.Error("Expected error, but got none")
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
				}
			}
		})
	}
}

func TestClientSearchResourceIncludedBehavior(t *testing.T) {
	tests := []struct {
		name                  string
		resourceType          string
		parameters            search.Parameters
		options               search.Options
		serverResponse        string
		statusCode            int
		expectedError         bool
		expectedResourceCount int
		expectedIncludedCount int
		expectedNextCursor    string
	}{
		{
			name:         "search_with_matches_and_includes",
			resourceType: "Patient",
			parameters:   search.GenericParams{"name": search.String("John"), "_include": search.String("Patient:organization")},
			options:      search.Options{},
			statusCode:   http.StatusOK,
			serverResponse: `{
				"resourceType": "Bundle",
				"type": "searchset",
				"entry": [
					{
						"resource": {
							"resourceType": "Patient",
							"id": "123",
							"name": [{"given": ["John"]}]
						},
						"search": {
							"mode": "match"
						}
					},
					{
						"resource": {
							"resourceType": "Organization",
							"id": "org1",
							"name": "Test Hospital"
						},
						"search": {
							"mode": "include"
						}
					},
					{
						"resource": {
							"resourceType": "Organization",
							"id": "org2",
							"name": "Another Hospital"
						},
						"search": {
							"mode": "include"
						}
					}
				]
			}`,
			expectedError:         false,
			expectedResourceCount: 1,
			expectedIncludedCount: 2,
			expectedNextCursor:    "",
		},
		{
			name:         "entries_without_search_mode_default_to_match",
			resourceType: "Patient",
			parameters:   search.GenericParams{"name": search.String("John")},
			options:      search.Options{},
			statusCode:   http.StatusOK,
			serverResponse: `{
				"resourceType": "Bundle",
				"type": "searchset",
				"entry": [
					{
						"resource": {
							"resourceType": "Patient",
							"id": "123",
							"name": [{"given": ["John"]}]
						}
					},
					{
						"resource": {
							"resourceType": "Patient",
							"id": "456",
							"name": [{"given": ["Jane"]}]
						},
						"search": {
							"mode": "match"
						}
					}
				]
			}`,
			expectedError:         false,
			expectedResourceCount: 2,
			expectedIncludedCount: 0,
			expectedNextCursor:    "",
		},
		{
			name:         "next_link_url_parsing",
			resourceType: "Patient",
			parameters:   search.GenericParams{"birthdate": search.String("ge2000-01-01")},
			options:      search.Options{Count: 5},
			statusCode:   http.StatusOK,
			serverResponse: `{
				"resourceType": "Bundle",
				"id": "example-search-result",
				"type": "searchset",
				"link": [
					{
						"relation": "self",
						"url": "https://server.fire.ly/Patient?birthdate=ge2000-01-01&_count=5"
					},
					{
						"relation": "next",
						"url": "https://server.fire.ly/?q=CfDJ8LjzO31FTPZ7oJjS9LaN61lmkTfAnInTGj2O1K-gN64JUkzbjV9Btdtq7ilIc5Z06kpu7l9wjDm3KdPbJ4-w5Ebwov3KvrmuWNo4dLr3nv2BEGxnXAjPiGX_ymfHRVmtwuR2NPDDFuY7vw8uweBWktTYEJFRaM2rPpRpiS7GBM1p6EjYGFTiBJua1BBJYIteMI4-VALy5e3e4m3lR4pBqCqfpWJ0BwdcwZQv1HPnDt9e8nVKi6HixZVx4j3psjceo5GaAe2csWKEpeRz5GsDLLV0Q_X48MS38BXd6H9z6FB9"
					}
				],
				"entry": [
					{
						"resource": {
							"resourceType": "Patient",
							"id": "patient-1"
						},
						"search": {
							"mode": "match"
						}
					}
				]
			}`,
			expectedError:         false,
			expectedResourceCount: 1,
			expectedIncludedCount: 0,
			expectedNextCursor:    "https://server.fire.ly/?q=CfDJ8LjzO31FTPZ7oJjS9LaN61lmkTfAnInTGj2O1K-gN64JUkzbjV9Btdtq7ilIc5Z06kpu7l9wjDm3KdPbJ4-w5Ebwov3KvrmuWNo4dLr3nv2BEGxnXAjPiGX_ymfHRVmtwuR2NPDDFuY7vw8uweBWktTYEJFRaM2rPpRpiS7GBM1p6EjYGFTiBJua1BBJYIteMI4-VALy5e3e4m3lR4pBqCqfpWJ0BwdcwZQv1HPnDt9e8nVKi6HixZVx4j3psjceo5GaAe2csWKEpeRz5GsDLLV0Q_X48MS38BXd6H9z6FB9",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				expectedPath := "/" + tt.resourceType
				if r.URL.Path != expectedPath {
					t.Errorf("Expected path %s, got %s", expectedPath, r.URL.Path)
				}
				if r.Method != "GET" {
					t.Errorf("Expected method GET, got %s", r.Method)
				}
				w.WriteHeader(tt.statusCode)
				if tt.serverResponse != "" {
					w.Write([]byte(tt.serverResponse))
				}
			}))
			defer server.Close()

			baseURL, _ := url.Parse(server.URL)
			client := ClientR4{
				BaseURL: baseURL,
				Client:  server.Client(),
			}

			result, err := client.Search(context.Background(), tt.resourceType, tt.parameters, tt.options)

			if tt.expectedError {
				if err == nil {
					t.Error("Expected error, but got none")
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
				}
				if len(result.Resources) != tt.expectedResourceCount {
					t.Errorf("Expected %d resources, got %d", tt.expectedResourceCount, len(result.Resources))
				}
				if len(result.Included) != tt.expectedIncludedCount {
					t.Errorf("Expected %d included resources, got %d", tt.expectedIncludedCount, len(result.Included))
				}
				if string(result.Next) != tt.expectedNextCursor {
					t.Errorf("Expected next cursor '%s', got '%s'", tt.expectedNextCursor, string(result.Next))
				}
			}
		})
	}
}

func TestClientInvoke(t *testing.T) {
	type reqCheck func(t *testing.T, r *http.Request)
	makeParams := func(k, v string) r4.Parameters {
		vv := v
		name := k
		return r4.Parameters{Parameter: []r4.ParametersParameter{{
			Name:  r4.String{Value: &name},
			Value: r4.String{Value: &vv},
		}}}
	}

	tests := []struct {
		name         string
		setupServer  func(t *testing.T) *httptest.Server
		call         func(c ClientR4) (model.Resource, error)
		expectNilRes bool
		expectErr    bool
	}{
		{
			name: "system_invoke_post_with_body",
			setupServer: func(t *testing.T) *httptest.Server {
				return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.Method != http.MethodPost {
						t.Fatalf("expected POST, got %s", r.Method)
					}
					if r.URL.Path != "/$echo" {
						t.Fatalf("expected path /$echo, got %s", r.URL.Path)
					}
					if r.Header.Get("Accept") != string(FormatJSON) {
						t.Fatalf("expected Accept %s", FormatJSON)
					}
					if r.Header.Get("Content-Type") != string(FormatJSON) {
						t.Fatalf("expected Content-Type %s", FormatJSON)
					}
					w.Header().Set("Content-Type", string(FormatJSON))
					w.WriteHeader(http.StatusOK)
					io.WriteString(w, `{"resourceType":"Patient","id":"p1"}`)
				}))
			},
			call: func(c ClientR4) (model.Resource, error) {
				return c.InvokeSystem(context.Background(), "echo", makeParams("in", "hello"))
			},
		},
		{
			name: "type_invoke_path_and_decoding",
			setupServer: func(t *testing.T) *httptest.Server {
				return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.URL.Path != "/Patient/$validate" {
						t.Fatalf("expected path /Patient/$validate, got %s", r.URL.Path)
					}
					w.Header().Set("Content-Type", string(FormatJSON))
					w.WriteHeader(http.StatusOK)
					io.WriteString(w, `{"resourceType":"OperationOutcome"}`)
				}))
			},
			call: func(c ClientR4) (model.Resource, error) {
				return c.InvokeType(context.Background(), "Patient", "validate", r4.Parameters{})
			},
		},
		{
			name: "instance_invoke_no_content",
			setupServer: func(t *testing.T) *httptest.Server {
				return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.URL.Path != "/Patient/123/$everything" {
						t.Fatalf("expected path /Patient/123/$everything, got %s", r.URL.Path)
					}
					w.WriteHeader(http.StatusNoContent)
				}))
			},
			call: func(c ClientR4) (model.Resource, error) {
				return c.InvokeInstance(context.Background(), "Patient", "123", "everything", r4.Parameters{})
			},
			expectNilRes: true,
		},
		{
			name: "invoke_error_outcome",
			setupServer: func(t *testing.T) *httptest.Server {
				return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.Header().Set("Content-Type", string(FormatJSON))
					w.WriteHeader(http.StatusBadRequest)
					io.WriteString(w, `{"resourceType":"OperationOutcome"}`)
				}))
			},
			call: func(c ClientR4) (model.Resource, error) {
				return c.InvokeSystem(context.Background(), "broken", r4.Parameters{})
			},
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := tt.setupServer(t)
			defer server.Close()

			baseURL, _ := url.Parse(server.URL)
			client := ClientR4{BaseURL: baseURL, Client: server.Client()}

			res, err := tt.call(client)
			if tt.expectErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tt.expectNilRes {
				if res != nil {
					t.Fatalf("expected nil resource, got %+v", res)
				}
			} else if res == nil {
				t.Fatalf("expected non-nil resource")
			}
		})
	}
}

func TestIterator(t *testing.T) {
	tests := []struct {
		name              string
		setupTest         func() (interface{}, *mockSearchClient)
		expectedCalls     []iteratorCall
		expectedMockCalls int
	}{
		{
			name: "single_page",
			setupTest: func() (interface{}, *mockSearchClient) {
				patient1 := &r4.Patient{Id: &r4.Id{Value: ptr.To("patient1")}}
				patient2 := &r4.Patient{Id: &r4.Id{Value: ptr.To("patient2")}}

				initialResult := search.Result[r4.Patient]{
					Resources: []r4.Patient{*patient1, *patient2},
					Next:      "", // No next page
				}

				mockClient := &mockSearchClient{
					pages: []search.Result[model.Resource]{},
				}

				return initialResult, mockClient
			},
			expectedCalls: []iteratorCall{
				{expectError: false, expectedResourceCount: 2},
				{expectError: true, expectedErr: io.EOF},
			},
			expectedMockCalls: 0,
		},
		{
			name: "multiple_pages",
			setupTest: func() (interface{}, *mockSearchClient) {
				initialResult := search.Result[model.Resource]{
					Resources: []model.Resource{&r4.Patient{Id: &r4.Id{Value: ptr.To("patient1")}}},
					Next:      "1", // Points to second page
				}

				page2 := search.Result[model.Resource]{
					Resources: []model.Resource{&r4.Patient{Id: &r4.Id{Value: ptr.To("patient2")}}},
					Next:      "2", // Points to third page
				}
				page3 := search.Result[model.Resource]{
					Resources: []model.Resource{&r4.Patient{Id: &r4.Id{Value: ptr.To("patient3")}}},
					Next:      "", // No more pages
				}

				mockClient := &mockSearchClient{
					pages: []search.Result[model.Resource]{
						{},    // page 0 (not used)
						page2, // page 1
						page3, // page 2
					},
				}

				return initialResult, mockClient
			},
			expectedCalls: []iteratorCall{
				{expectError: false, expectedResourceCount: 1},
				{expectError: false, expectedResourceCount: 1},
				{expectError: false, expectedResourceCount: 1},
				{expectError: true, expectedErr: io.EOF},
			},
			expectedMockCalls: 2,
		},
		{
			name: "empty_result",
			setupTest: func() (interface{}, *mockSearchClient) {
				initialResult := search.Result[r4.Patient]{
					Resources: []r4.Patient{},
					Next:      "", // No next page
				}

				mockClient := &mockSearchClient{
					pages: []search.Result[model.Resource]{},
				}

				return initialResult, mockClient
			},
			expectedCalls: []iteratorCall{
				{expectError: false, expectedResourceCount: 0},
				{expectError: true, expectedErr: io.EOF},
			},
			expectedMockCalls: 0,
		},
		{
			name: "multiple_eof_calls",
			setupTest: func() (interface{}, *mockSearchClient) {
				patient1 := &r4.Patient{Id: &r4.Id{Value: ptr.To("patient1")}}

				initialResult := search.Result[r4.Patient]{
					Resources: []r4.Patient{*patient1},
					Next:      "", // No next page
				}

				mockClient := &mockSearchClient{
					pages: []search.Result[model.Resource]{},
				}

				return initialResult, mockClient
			},
			expectedCalls: []iteratorCall{
				{expectError: false, expectedResourceCount: 1},
				{expectError: true, expectedErr: io.EOF},
				{expectError: true, expectedErr: io.EOF},
				{expectError: true, expectedErr: io.EOF},
			},
			expectedMockCalls: 0,
		},
		{
			name: "pointer_patients",
			setupTest: func() (interface{}, *mockSearchClient) {
				patient1 := &r4.Patient{Id: &r4.Id{Value: ptr.To("patient1")}}
				patient2 := &r4.Patient{Id: &r4.Id{Value: ptr.To("patient2")}}

				initialResult := search.Result[*r4.Patient]{
					Resources: []*r4.Patient{patient1},
					Next:      "1", // Points to second page
				}

				mockClient := &mockSearchClient{
					pages: []search.Result[model.Resource]{
						{}, // page 0 (not used)
						{ // page 1
							Resources: []model.Resource{patient2},
							Next:      "", // No more pages
						},
					},
				}

				return initialResult, mockClient
			},
			expectedCalls: []iteratorCall{
				{expectError: false, expectedResourceCount: 1},
				{expectError: false, expectedResourceCount: 1},
				{expectError: true, expectedErr: io.EOF},
			},
			expectedMockCalls: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			initialResult, mockClient := tt.setupTest()

			var iter interface{}

			// Create iterator based on the type of initial result
			switch result := initialResult.(type) {
			case search.Result[r4.Patient]:
				iter = Iterator(mockClient, result)
			case search.Result[model.Resource]:
				iter = Iterator(mockClient, result)
			case search.Result[*r4.Patient]:
				iter = Iterator(mockClient, result)
			default:
				t.Fatalf("Unsupported result type: %T", initialResult)
			}

			// Execute all expected calls
			for i, expectedCall := range tt.expectedCalls {
				var err error
				var resourceCount int

				// Call Next() based on iterator type
				switch it := iter.(type) {
				case *iterator[r4.Patient]:
					page, nextErr := it.Next(context.Background())
					err = nextErr
					if err == nil {
						resourceCount = len(page.Resources)
					}
				case *iterator[model.Resource]:
					page, nextErr := it.Next(context.Background())
					err = nextErr
					if err == nil {
						resourceCount = len(page.Resources)
					}
				case *iterator[*r4.Patient]:
					page, nextErr := it.Next(context.Background())
					err = nextErr
					if err == nil {
						resourceCount = len(page.Resources)
					}
				default:
					t.Fatalf("Unsupported iterator type: %T", iter)
				}

				// Verify results
				if expectedCall.expectError {
					if err != expectedCall.expectedErr {
						t.Errorf("Call %d: expected error %v, got %v", i+1, expectedCall.expectedErr, err)
					}
				} else {
					if err != nil {
						t.Errorf("Call %d: unexpected error: %v", i+1, err)
					}
					if resourceCount != expectedCall.expectedResourceCount {
						t.Errorf("Call %d: expected %d resources, got %d", i+1, expectedCall.expectedResourceCount, resourceCount)
					}
				}
			}

			// Verify mock calls
			if mockClient.calls != tt.expectedMockCalls {
				t.Errorf("Expected %d mock calls, got %d", tt.expectedMockCalls, mockClient.calls)
			}
		})
	}
}

type iteratorCall struct {
	expectError           bool
	expectedErr           error
	expectedResourceCount int
}

func TestIteratorCountParameter(t *testing.T) {
	// Create initial result with 3 resources
	initialResult := search.Result[r4.Patient]{
		Resources: []r4.Patient{
			{Id: &r4.Id{Value: ptr.To("patient1")}},
			{Id: &r4.Id{Value: ptr.To("patient2")}},
			{Id: &r4.Id{Value: ptr.To("patient3")}},
		},
		Next: "1", // Points to next page
	}

	// Create second page with 2 resources
	page2 := search.Result[model.Resource]{
		Resources: []model.Resource{
			&r4.Patient{Id: &r4.Id{Value: ptr.To("patient4")}},
			&r4.Patient{Id: &r4.Id{Value: ptr.To("patient5")}},
		},
		Next: "", // No more pages
	}

	mockClient := &mockSearchClient{
		pages: []search.Result[model.Resource]{
			{},    // page 0 (not used)
			page2, // page 1
		},
	}

	iter := Iterator(mockClient, initialResult)

	// First call should return initial result without calling Search
	result1, err := iter.Next(context.Background())
	if err != nil {
		t.Fatalf("First call failed: %v", err)
	}
	if len(result1.Resources) != 3 {
		t.Errorf("Expected 3 resources in first result, got %d", len(result1.Resources))
	}
	if mockClient.calls != 0 {
		t.Errorf("Expected 0 mock calls after first Next(), got %d", mockClient.calls)
	}

	// Second call should fetch next page and pass Count=3 (length of initial result)
	result2, err := iter.Next(context.Background())
	if err != nil {
		t.Fatalf("Second call failed: %v", err)
	}
	if len(result2.Resources) != 2 {
		t.Errorf("Expected 2 resources in second result, got %d", len(result2.Resources))
	}
	if mockClient.calls != 1 {
		t.Errorf("Expected 1 mock call after second Next(), got %d", mockClient.calls)
	}

	// Verify that Count was set to the length of the previous page (3)
	if mockClient.lastOptions.Count != 3 {
		t.Errorf("Expected Count=3 in search options, got Count=%d", mockClient.lastOptions.Count)
	}

	// Verify cursor was passed correctly
	if mockClient.lastOptions.Cursor != "1" {
		t.Errorf("Expected Cursor='1' in search options, got Cursor='%s'", mockClient.lastOptions.Cursor)
	}

	// Third call should return EOF
	_, err = iter.Next(context.Background())
	if err != io.EOF {
		t.Errorf("Expected EOF on third call, got: %v", err)
	}
}

func TestIteratorCountParameterMultiplePages(t *testing.T) {
	// Test that Count is maintained across multiple pages
	initialResult := search.Result[r4.Patient]{
		Resources: []r4.Patient{
			{Id: &r4.Id{Value: ptr.To("patient1")}},
			{Id: &r4.Id{Value: ptr.To("patient2")}},
		},
		Next: "1", // Points to page 1
	}

	page1 := search.Result[model.Resource]{
		Resources: []model.Resource{
			&r4.Patient{Id: &r4.Id{Value: ptr.To("patient3")}},
			&r4.Patient{Id: &r4.Id{Value: ptr.To("patient4")}},
		},
		Next: "2", // Points to page 2
	}

	page2 := search.Result[model.Resource]{
		Resources: []model.Resource{
			&r4.Patient{Id: &r4.Id{Value: ptr.To("patient5")}},
		},
		Next: "", // No more pages
	}

	mockClient := &mockSearchClient{
		pages: []search.Result[model.Resource]{
			{},    // page 0 (not used)
			page1, // page 1
			page2, // page 2
		},
	}

	iter := Iterator(mockClient, initialResult)

	// First call returns initial result (2 resources)
	result1, err := iter.Next(context.Background())
	if err != nil {
		t.Fatalf("First call failed: %v", err)
	}
	if len(result1.Resources) != 2 {
		t.Errorf("Expected 2 resources in first result, got %d", len(result1.Resources))
	}

	// Second call fetches page 1, Count should be 2
	result2, err := iter.Next(context.Background())
	if err != nil {
		t.Fatalf("Second call failed: %v", err)
	}
	if len(result2.Resources) != 2 {
		t.Errorf("Expected 2 resources in second result, got %d", len(result2.Resources))
	}
	if mockClient.lastOptions.Count != 2 {
		t.Errorf("Expected Count=2 for page 1, got Count=%d", mockClient.lastOptions.Count)
	}
	if mockClient.lastOptions.Cursor != "1" {
		t.Errorf("Expected Cursor='1' for page 1, got Cursor='%s'", mockClient.lastOptions.Cursor)
	}

	// Third call fetches page 2, Count should still be 2 (from page 1)
	result3, err := iter.Next(context.Background())
	if err != nil {
		t.Fatalf("Third call failed: %v", err)
	}
	if len(result3.Resources) != 1 {
		t.Errorf("Expected 1 resource in third result, got %d", len(result3.Resources))
	}
	if mockClient.lastOptions.Count != 2 {
		t.Errorf("Expected Count=2 for page 2, got Count=%d", mockClient.lastOptions.Count)
	}
	if mockClient.lastOptions.Cursor != "2" {
		t.Errorf("Expected Cursor='2' for page 2, got Cursor='%s'", mockClient.lastOptions.Cursor)
	}

	// Fourth call should return EOF
	_, err = iter.Next(context.Background())
	if err != io.EOF {
		t.Errorf("Expected EOF on fourth call, got: %v", err)
	}
}

func TestIteratorCountParameterEmptyInitialResult(t *testing.T) {
	// Test Count parameter when initial result is empty but has next page
	initialResult := search.Result[r4.Patient]{
		Resources: []r4.Patient{}, // Empty initial result
		Next:      "1",            // But has next page
	}

	page1 := search.Result[model.Resource]{
		Resources: []model.Resource{
			&r4.Patient{Id: &r4.Id{Value: ptr.To("patient1")}},
		},
		Next: "", // No more pages
	}

	mockClient := &mockSearchClient{
		pages: []search.Result[model.Resource]{
			{},    // page 0 (not used)
			page1, // page 1
		},
	}

	iter := Iterator(mockClient, initialResult)

	// First call returns empty initial result
	result1, err := iter.Next(context.Background())
	if err != nil {
		t.Fatalf("First call failed: %v", err)
	}
	if len(result1.Resources) != 0 {
		t.Errorf("Expected 0 resources in first result, got %d", len(result1.Resources))
	}

	// Second call fetches page 1, Count should be 0 (from empty initial result)
	result2, err := iter.Next(context.Background())
	if err != nil {
		t.Fatalf("Second call failed: %v", err)
	}
	if len(result2.Resources) != 1 {
		t.Errorf("Expected 1 resource in second result, got %d", len(result2.Resources))
	}
	if mockClient.lastOptions.Count != 0 {
		t.Errorf("Expected Count=0 for page 1, got Count=%d", mockClient.lastOptions.Count)
	}
	if mockClient.lastOptions.Cursor != "1" {
		t.Errorf("Expected Cursor='1' for page 1, got Cursor='%s'", mockClient.lastOptions.Cursor)
	}

	// Third call should return EOF
	_, err = iter.Next(context.Background())
	if err != io.EOF {
		t.Errorf("Expected EOF on third call, got: %v", err)
	}
}
