package rest_test

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/damedic/fhir-toolbox-go/capabilities/search"
	"github.com/damedic/fhir-toolbox-go/model"

	// basic types removed; use r4 directly
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/damedic/fhir-toolbox-go/model/gen/r4"
	"github.com/damedic/fhir-toolbox-go/rest"
	"github.com/damedic/fhir-toolbox-go/utils/ptr"
)

func TestSearchParameterCapabilities(t *testing.T) {
	tests := []struct {
		name         string
		backend      interface{}
		expectedBody string
	}{
		{
			name:    "fallback_capabilities_without_SearchParameterSearch",
			backend: mockBackendMinimal{},
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
						"resource": [
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
			name:    "concrete_capabilities_with_SearchParameterSearch",
			backend: mockBackendWithSearchParameterSearchOnly{},
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
						"resource": [
							{
								"interaction": [
									{
										"code": "search-type"
									}
								],
								"searchParam": [
									{
										"definition": "http://example.com/SearchParameter/SearchParameter-name",
										"name": "name",
										"type": "string"
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := &rest.Server[model.R4]{
				Backend: tt.backend,
			}

			req := httptest.NewRequest("GET", "http://example.com/metadata", nil)
			req.Header.Set("Accept", "application/fhir+json")

			rr := httptest.NewRecorder()
			server.ServeHTTP(rr, req)

			if rr.Code != http.StatusOK {
				t.Errorf("Expected status code %d, got %d", http.StatusOK, rr.Code)
			}

			assertResponse(t, "application/fhir+json", tt.expectedBody, rr)
		})
	}
}

func TestSearchParameterSearch(t *testing.T) {
	tests := []struct {
		name         string
		backend      interface{}
		query        string
		expectedBody string
	}{
		{
			name:    "search_fallback_with_results",
			backend: mockBackendWithoutSearchParameterSearch{},
			query:   "_id=SearchParameter-id",
			expectedBody: `{
				"resourceType": "Bundle",
				"type": "searchset",
				"entry": [
					{
						"fullUrl": "http://example.com/SearchParameter/SearchParameter-id",
						"resource": {
							"resourceType": "SearchParameter",
							"id": "SearchParameter-id",
							"url": "http://example.com/SearchParameter/SearchParameter-id",
							"name": "_id",
							"status": "active",
							"description": "Logical id of this artifact",
							"code": "_id",
							"base": ["SearchParameter"],
							"type": "token",
							"expression": "SearchParameter.id"
						},
						"search": {
							"mode": "match"
						}
					}
				],
				"link": [
					{
						"relation": "self",
						"url": "http://example.com/SearchParameter?_id=SearchParameter-id&_count=500"
					}
				]
			}`,
		},
		{
			name:    "search_fallback_empty_result",
			backend: mockBackendWithoutSearchParameterSearch{},
			query:   "_id=unknown-id",
			expectedBody: `{
				"resourceType": "Bundle",
				"type": "searchset",
				"link": [
					{
						"relation": "self",
						"url": "http://example.com/SearchParameter?_id=unknown-id&_count=500"
					}
				]
			}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := &rest.Server[model.R4]{
				Backend: tt.backend,
			}

			req := httptest.NewRequest("GET", "http://example.com/SearchParameter?"+tt.query, nil)
			req.Header.Set("Accept", "application/fhir+json")

			rr := httptest.NewRecorder()
			server.ServeHTTP(rr, req)

			if rr.Code != http.StatusOK {
				t.Errorf("Expected status code %d, got %d", http.StatusOK, rr.Code)
			}

			assertResponse(t, "application/fhir+json", tt.expectedBody, rr)
		})
	}
}

// Mock backend with minimal capabilities (only CapabilityBase)
type mockBackendMinimal struct{}

func (m mockBackendMinimal) CapabilityBase(ctx context.Context) (r4.CapabilityStatement, error) {
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

// Mock backend WITHOUT SearchParameterSearch implementation (fallback scenario)
type mockBackendWithoutSearchParameterSearch struct{}

func (m mockBackendWithoutSearchParameterSearch) CapabilityBase(ctx context.Context) (r4.CapabilityStatement, error) {
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

// Add Patient search capabilities to generate SearchParameters
func (m mockBackendWithoutSearchParameterSearch) SearchCapabilitiesPatient(ctx context.Context) (r4.SearchCapabilities, error) {
	return r4.SearchCapabilities{
		Parameters: map[string]r4.SearchParameter{
			"_id":  {Type: r4.SearchParamTypeToken},
			"date": {Type: r4.SearchParamTypeDate},
			"name": {Type: r4.SearchParamTypeString},
		},
	}, nil
}

// Add Patient search implementation to make it part of capabilities
func (m mockBackendWithoutSearchParameterSearch) SearchPatient(ctx context.Context, parameters search.Parameters, options search.Options) (search.Result[r4.Patient], error) {
	return search.Result[r4.Patient]{}, nil
}

// Add Observation search capabilities to generate SearchParameters
func (m mockBackendWithoutSearchParameterSearch) SearchCapabilitiesObservation(ctx context.Context) (r4.SearchCapabilities, error) {
	return r4.SearchCapabilities{
		Parameters: map[string]r4.SearchParameter{
			"_id": {Type: r4.SearchParamTypeToken},
		},
	}, nil
}

// Add Observation search implementation to make it part of capabilities
func (m mockBackendWithoutSearchParameterSearch) SearchObservation(ctx context.Context, parameters search.Parameters, options search.Options) (search.Result[r4.Observation], error) {
	return search.Result[r4.Observation]{}, nil
}

// Mock backend WITH SearchParameterSearch implementation (inherits minimal base)
type mockBackendWithSearchParameterSearchOnly struct {
	mockBackendMinimal
}

func (m mockBackendWithSearchParameterSearchOnly) SearchCapabilitiesSearchParameter(ctx context.Context) (r4.SearchCapabilities, error) {
	return r4.SearchCapabilities{
		Parameters: map[string]r4.SearchParameter{
			"name": {Type: r4.SearchParamTypeString},
		},
	}, nil
}

func (m mockBackendWithSearchParameterSearchOnly) SearchSearchParameter(ctx context.Context, parameters search.Parameters, options search.Options) (search.Result[r4.SearchParameter], error) {
	result := search.Result[r4.SearchParameter]{}

	// Return a mock SearchParameter
	mockSearchParam := r4.SearchParameter{
		Id:          &r4.Id{Value: ptr.To("custom-param")},
		Url:         r4.Uri{Value: ptr.To("http://example.com/SearchParameter/custom-param")},
		Name:        r4.String{Value: ptr.To("example")},
		Status:      r4.Code{Value: ptr.To("active")},
		Description: r4.Markdown{Value: ptr.To("Custom search parameter")},
		Code:        r4.Code{Value: ptr.To("example")},
		Base: []r4.Code{
			{Value: ptr.To("Patient")},
		},
		Type:       r4.Code{Value: ptr.To("string")},
		Expression: &r4.String{Value: ptr.To("Patient.name")},
	}

	result.Resources = []r4.SearchParameter{mockSearchParam}
	return result, nil
}

// Mock backend WITH SearchParameterSearch implementation (concrete scenario with Patient/Observation)
type mockBackendWithSearchParameterSearch struct {
	mockBackendWithoutSearchParameterSearch
}

func (m mockBackendWithSearchParameterSearch) SearchCapabilitiesSearchParameter(ctx context.Context) (r4.SearchCapabilities, error) {
	return r4.SearchCapabilities{
		Parameters: map[string]r4.SearchParameter{
			"name": {Type: r4.SearchParamTypeString},
		},
	}, nil
}

func (m mockBackendWithSearchParameterSearch) SearchSearchParameter(ctx context.Context, parameters search.Parameters, options search.Options) (search.Result[r4.SearchParameter], error) {
	result := search.Result[r4.SearchParameter]{}

	// Return a mock SearchParameter
	mockSearchParam := r4.SearchParameter{
		Id:          &r4.Id{Value: ptr.To("custom-param")},
		Url:         r4.Uri{Value: ptr.To("http://example.com/SearchParameter/custom-param")},
		Name:        r4.String{Value: ptr.To("example")},
		Status:      r4.Code{Value: ptr.To("active")},
		Description: r4.Markdown{Value: ptr.To("Custom search parameter")},
		Code:        r4.Code{Value: ptr.To("example")},
		Base: []r4.Code{
			{Value: ptr.To("Patient")},
		},
		Type:       r4.Code{Value: ptr.To("string")},
		Expression: &r4.String{Value: ptr.To("Patient.name")},
	}

	result.Resources = []r4.SearchParameter{mockSearchParam}
	return result, nil
}

func TestSearchParameterRead(t *testing.T) {
	tests := []struct {
		name         string
		backend      interface{}
		url          string
		expectedBody string
	}{
		{
			name:    "read_fallback_SearchParameter_id",
			backend: mockBackendWithoutSearchParameterSearch{},
			url:     "http://example.com/SearchParameter/SearchParameter-id",
			expectedBody: `{
				"resourceType": "SearchParameter",
				"id": "SearchParameter-id",
				"url": "http://example.com/SearchParameter/SearchParameter-id",
				"name": "_id",
				"status": "active",
				"description": "Logical id of this artifact",
				"code": "_id",
				"base": ["SearchParameter"],
				"type": "token",
				"expression": "SearchParameter.id"
			}`,
		},
		{
			name:    "read_generated_from_Patient_capabilities",
			backend: mockBackendWithoutSearchParameterSearch{},
			url:     "http://example.com/SearchParameter/Patient-id",
			expectedBody: `{
				"resourceType": "SearchParameter",
				"id": "Patient-id",
				"url": "http://example.com/SearchParameter/Patient-id",
				"name": "_id",
				"status": "active",
				"description": "Search parameter _id for Patient resource",
				"code": "_id",
				"base": ["Patient"],
				"type": "token"
			}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := &rest.Server[model.R4]{
				Backend: tt.backend,
			}

			req := httptest.NewRequest("GET", tt.url, nil)
			req.Header.Set("Accept", "application/fhir+json")

			rr := httptest.NewRecorder()
			server.ServeHTTP(rr, req)

			if rr.Code != http.StatusOK {
				t.Errorf("Expected status code %d, got %d", http.StatusOK, rr.Code)
			}

			assertResponse(t, "application/fhir+json", tt.expectedBody, rr)
		})
	}
}

// Test that all SearchParameter URLs in CapabilityStatement can be resolved
func TestAllCapabilityStatementSearchParameterUrlsResolvable(t *testing.T) {
	backend := mockBackendWithoutSearchParameterSearch{}

	server := &rest.Server[model.R4]{
		Backend: backend,
	}

	// First, get the CapabilityStatement
	req := httptest.NewRequest("GET", "http://example.com/metadata", nil)
	req.Header.Set("Accept", "application/fhir+json")

	rr := httptest.NewRecorder()
	server.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("Expected status code %d, got %d", http.StatusOK, rr.Code)
	}

	// Parse the CapabilityStatement
	var capabilityStatement map[string]interface{}
	if err := json.Unmarshal(rr.Body.Bytes(), &capabilityStatement); err != nil {
		t.Fatalf("Failed to parse CapabilityStatement JSON: %v", err)
	}

	// Extract all SearchParameter definition URLs from the CapabilityStatement
	searchParamUrls := extractSearchParameterUrls(t, capabilityStatement)

	if len(searchParamUrls) == 0 {
		t.Fatal("No SearchParameter URLs found in CapabilityStatement")
	}

	t.Logf("Found %d SearchParameter URLs in CapabilityStatement", len(searchParamUrls))

	// Test each SearchParameter URL can be resolved
	for _, definitionUrl := range searchParamUrls {
		t.Run(fmt.Sprintf("resolve_%s", getLastPathSegment(definitionUrl)), func(t *testing.T) {
			// Convert the definition URL to a relative path for the test server
			relativeUrl := convertToRelativePath(definitionUrl, "http://example.com")

			req := httptest.NewRequest("GET", relativeUrl, nil)
			req.Header.Set("Accept", "application/fhir+json")

			rr := httptest.NewRecorder()
			server.ServeHTTP(rr, req)

			if rr.Code != http.StatusOK {
				t.Errorf("Failed to resolve SearchParameter at %s (converted to %s). Status: %d, Body: %s",
					definitionUrl, relativeUrl, rr.Code, rr.Body.String())
				return
			}

			// Verify it's a valid SearchParameter resource
			var searchParam map[string]interface{}
			if err := json.Unmarshal(rr.Body.Bytes(), &searchParam); err != nil {
				t.Errorf("Failed to parse SearchParameter JSON for %s: %v", definitionUrl, err)
				return
			}

			// Verify it has the expected resourceType
			if resourceType, ok := searchParam["resourceType"].(string); !ok || resourceType != "SearchParameter" {
				t.Errorf("Expected resourceType 'SearchParameter', got %v for URL %s", resourceType, definitionUrl)
				return
			}

			// Verify the URL matches the definition URL
			if urlField, ok := searchParam["url"].(string); !ok || urlField != definitionUrl {
				t.Errorf("SearchParameter URL field (%s) does not match definition URL (%s)", urlField, definitionUrl)
				return
			}

			t.Logf("Successfully resolved SearchParameter: %s", definitionUrl)
		})
	}
}

// Helper function to extract SearchParameter definition URLs from CapabilityStatement
func extractSearchParameterUrls(t *testing.T, capabilityStatement map[string]interface{}) []string {
	var urls []string

	rest, ok := capabilityStatement["rest"].([]interface{})
	if !ok {
		t.Fatal("CapabilityStatement missing 'rest' array")
	}

	for _, restItem := range rest {
		restObj, ok := restItem.(map[string]interface{})
		if !ok {
			continue
		}

		resources, ok := restObj["resource"].([]interface{})
		if !ok {
			continue
		}

		for _, resourceItem := range resources {
			resourceObj, ok := resourceItem.(map[string]interface{})
			if !ok {
				continue
			}

			searchParams, ok := resourceObj["searchParam"].([]interface{})
			if !ok {
				continue
			}

			for _, searchParamItem := range searchParams {
				searchParamObj, ok := searchParamItem.(map[string]interface{})
				if !ok {
					continue
				}

				if definition, ok := searchParamObj["definition"].(string); ok {
					urls = append(urls, definition)
				}
			}
		}
	}

	return urls
}

// Helper function to convert absolute URL to relative path for testing
func convertToRelativePath(absoluteUrl, baseUrl string) string {
	if strings.HasPrefix(absoluteUrl, baseUrl) {
		return absoluteUrl[len(baseUrl):]
	}

	// Parse the URL to get just the path
	if parsedUrl, err := url.Parse(absoluteUrl); err == nil {
		return parsedUrl.Path
	}

	return absoluteUrl
}

// Helper function to get the last path segment from a URL
func getLastPathSegment(urlStr string) string {
	if parsedUrl, err := url.Parse(urlStr); err == nil {
		segments := strings.Split(strings.Trim(parsedUrl.Path, "/"), "/")
		if len(segments) > 0 {
			return segments[len(segments)-1]
		}
	}
	return "unknown"
}

func TestStrictSearchParameters(t *testing.T) {
	tests := []struct {
		name           string
		strict         bool
		query          string
		expectedStatus int
		errorContains  string
		expectBundle   bool
	}{
		{
			name:           "strict_enabled_unsupported_param",
			strict:         true,
			query:          "unsupported_param=test",
			expectedStatus: http.StatusBadRequest,
			errorContains:  "unsupported search parameter",
			expectBundle:   false,
		},
		{
			name:           "strict_disabled_unsupported_param",
			strict:         false,
			query:          "unsupported_param=test",
			expectedStatus: http.StatusOK,
			errorContains:  "",
			expectBundle:   true,
		},
		{
			name:           "strict_enabled_supported_param",
			strict:         true,
			query:          "_id=test123",
			expectedStatus: http.StatusOK,
			errorContains:  "",
			expectBundle:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			backend := mockBackendWithoutSearchParameterSearch{}

			server := &rest.Server[model.R4]{
				Backend:                backend,
				StrictSearchParameters: tt.strict,
			}

			req := httptest.NewRequest("GET", "http://example.com/Patient?"+tt.query, nil)
			req.Header.Set("Accept", "application/fhir+json")

			rr := httptest.NewRecorder()
			server.ServeHTTP(rr, req)

			if rr.Code != tt.expectedStatus {
				t.Errorf("Expected status code %d, got %d. Response: %s", tt.expectedStatus, rr.Code, rr.Body.String())
			}

			bodyStr := rr.Body.String()

			if tt.errorContains != "" {
				if !strings.Contains(bodyStr, tt.errorContains) {
					t.Errorf("Expected error message to contain '%s'", tt.errorContains)
				}
			}

			if tt.expectBundle {
				if !strings.Contains(bodyStr, `"resourceType":"Bundle"`) {
					t.Error("Expected response to be a Bundle")
				}
				if !strings.Contains(bodyStr, `"type":"searchset"`) {
					t.Error("Expected Bundle type to be searchset")
				}
			}
		})
	}
}

func TestSearchParameterPaginationBasic(t *testing.T) {
	tests := []struct {
		name               string
		query              string
		expectedEntries    int
		maxEntries         int
		expectNextLink     bool
		expectCursorInNext bool
	}{
		{
			name:            "pagination_with_count",
			query:           "_count=2",
			expectedEntries: 2,
			maxEntries:      2,
			expectNextLink:  true,
		},
		{
			name:            "pagination_with_cursor",
			query:           "_count=2&_cursor=2",
			expectedEntries: 2,
			maxEntries:      2,
			expectNextLink:  true,
		},
		{
			name:            "pagination_with_cursor",
			query:           "_count=2&_cursor=4",
			expectedEntries: 1,
			maxEntries:      2,
			expectNextLink:  false, // should not have next link because there are only 5 params
		},
		{
			name:            "pagination_with_cursor",
			query:           "_count=1&_cursor=4",
			expectedEntries: 1,
			maxEntries:      1,
			expectNextLink:  false, // should not have next link because there are only 5 params
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			backend := mockBackendWithoutSearchParameterSearch{}

			server := &rest.Server[model.R4]{
				Backend: backend,
			}

			req := httptest.NewRequest("GET", "http://example.com/SearchParameter?"+tt.query, nil)
			req.Header.Set("Accept", "application/fhir+json")

			rr := httptest.NewRecorder()
			server.ServeHTTP(rr, req)

			if rr.Code != http.StatusOK {
				t.Errorf("Expected status code %d, got %d", http.StatusOK, rr.Code)
			}

			var bundle map[string]interface{}
			if err := json.Unmarshal(rr.Body.Bytes(), &bundle); err != nil {
				t.Fatalf("Failed to parse bundle: %v", err)
			}

			entries, ok := bundle["entry"].([]interface{})
			if !ok {
				entries = []interface{}{}
			}

			if tt.expectedEntries >= 0 && len(entries) != tt.expectedEntries {
				t.Errorf("Expected %d entries, got %d", tt.expectedEntries, len(entries))
			}

			if len(entries) > tt.maxEntries {
				t.Errorf("Expected at most %d entries, got %d", tt.maxEntries, len(entries))
			}

			links, _ := bundle["link"].([]interface{})
			var nextLink string
			if links != nil {
				for _, link := range links {
					linkObj := link.(map[string]interface{})
					if relation, ok := linkObj["relation"].(string); ok && relation == "next" {
						nextLink = linkObj["url"].(string)
						break
					}
				}
			}

			hasNextLink := nextLink != ""
			if hasNextLink != tt.expectNextLink {
				t.Errorf("Expected next link presence: %v, got: %v", tt.expectNextLink, hasNextLink)
			}

			if tt.expectCursorInNext && !strings.Contains(nextLink, "_cursor=") {
				t.Error("Next link should contain _cursor parameter")
			}
		})
	}
}

func TestSearchParameterPaginationEdgeCases(t *testing.T) {
	tests := []struct {
		name           string
		cursor         string
		count          string
		expectedError  string
		expectNextLink bool
	}{
		{
			name:           "cursor_beyond_results",
			cursor:         "100",
			count:          "10",
			expectedError:  "",
			expectNextLink: false,
		},
		{
			name:           "invalid_cursor_non_numeric",
			cursor:         "invalid",
			count:          "2",
			expectedError:  "invalid cursor",
			expectNextLink: false,
		},
		{
			name:           "last_page",
			cursor:         "4",
			count:          "10",
			expectedError:  "",
			expectNextLink: false,
		},
		{
			name:           "empty_cursor_is_valid",
			cursor:         "",
			count:          "2",
			expectedError:  "",
			expectNextLink: true,
		},
		{
			name:           "numeric_cursor_is_valid",
			cursor:         "5",
			count:          "2",
			expectedError:  "",
			expectNextLink: false,
		},
		{
			name:           "special_characters_cursor",
			cursor:         "abc123",
			count:          "2",
			expectedError:  "invalid cursor",
			expectNextLink: false,
		},
		{
			name:           "negative_number_cursor",
			cursor:         "-1",
			count:          "2",
			expectedError:  "invalid cursor: offset must be non-negative",
			expectNextLink: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			backend := mockBackendWithoutSearchParameterSearch{}

			server := &rest.Server[model.R4]{
				Backend: backend,
			}

			reqURL := "http://example.com/SearchParameter?_count=" + tt.count
			if tt.cursor != "" {
				reqURL += "&_cursor=" + tt.cursor
			}

			req := httptest.NewRequest("GET", reqURL, nil)
			req.Header.Set("Accept", "application/fhir+json")

			rr := httptest.NewRecorder()
			server.ServeHTTP(rr, req)

			if tt.expectedError != "" {
				if rr.Code == http.StatusOK {
					t.Errorf("Expected error status code, got %d", rr.Code)
				}
				bodyStr := rr.Body.String()
				if !strings.Contains(bodyStr, tt.expectedError) {
					t.Errorf("Expected error message to contain '%s', got: %s", tt.expectedError, bodyStr)
				}
				return
			}

			if rr.Code != http.StatusOK {
				t.Errorf("Expected status code %d, got %d. Response: %s", http.StatusOK, rr.Code, rr.Body.String())
			}

			var bundle map[string]interface{}
			if err := json.Unmarshal(rr.Body.Bytes(), &bundle); err != nil {
				t.Fatalf("Failed to parse bundle: %v", err)
			}

			links, _ := bundle["link"].([]interface{})
			hasNextLink := false
			if links != nil {
				for _, link := range links {
					linkObj := link.(map[string]interface{})
					if relation, ok := linkObj["relation"].(string); ok && relation == "next" {
						hasNextLink = true
						break
					}
				}
			}

			if hasNextLink != tt.expectNextLink {
				t.Errorf("Expected next link presence: %v, got: %v", tt.expectNextLink, hasNextLink)
			}
		})
	}
}

// Test SearchParameter pagination with multiple page navigation
func TestSearchParameterPaginationMultiplePages(t *testing.T) {
	backend := mockBackendWithoutSearchParameterSearch{}

	server := &rest.Server[model.R4]{
		Backend: backend,
	}

	// Test navigating through multiple pages
	pageSize := 2
	currentCursor := ""
	totalPagesVisited := 0
	maxPages := 3 // Prevent infinite loops

	for totalPagesVisited < maxPages {
		// Build request URL
		reqURL := "http://example.com/SearchParameter?_count=" + fmt.Sprintf("%d", pageSize)
		if currentCursor != "" {
			reqURL += "&_cursor=" + currentCursor
		}

		req := httptest.NewRequest("GET", reqURL, nil)
		req.Header.Set("Accept", "application/fhir+json")

		rr := httptest.NewRecorder()
		server.ServeHTTP(rr, req)

		if rr.Code != http.StatusOK {
			t.Errorf("Page %d: Expected status code %d, got %d", totalPagesVisited+1, http.StatusOK, rr.Code)
			break
		}

		// Parse the response
		var bundle map[string]interface{}
		if err := json.Unmarshal(rr.Body.Bytes(), &bundle); err != nil {
			t.Fatalf("Page %d: Failed to parse bundle: %v", totalPagesVisited+1, err)
		}

		// Check entries
		entries, ok := bundle["entry"].([]interface{})
		if !ok {
			entries = []interface{}{} // Empty entries is valid
		}

		// Should have at most pageSize entries
		if len(entries) > pageSize {
			t.Errorf("Page %d: Expected at most %d entries, got %d", totalPagesVisited+1, pageSize, len(entries))
		}

		// Find next link
		links, _ := bundle["link"].([]interface{})
		var nextLink string
		if links != nil {
			for _, link := range links {
				linkObj := link.(map[string]interface{})
				if relation, ok := linkObj["relation"].(string); ok && relation == "next" {
					nextLink = linkObj["url"].(string)
					break
				}
			}
		}

		totalPagesVisited++

		// If no next link or no entries, we've reached the end
		if nextLink == "" || len(entries) == 0 {
			t.Logf("Reached end of pagination after %d pages", totalPagesVisited)
			break
		}

		// Extract cursor from next link for next iteration
		if nextURL, err := url.Parse(nextLink); err == nil {
			currentCursor = nextURL.Query().Get("_cursor")
			if currentCursor == "" {
				t.Errorf("Page %d: Next link should contain _cursor parameter", totalPagesVisited)
				break
			}
		} else {
			t.Errorf("Page %d: Failed to parse next link URL: %v", totalPagesVisited, err)
			break
		}
	}

	// Should have visited at least 2 pages to test multi-page navigation
	if totalPagesVisited < 2 {
		t.Errorf("Expected to visit at least 2 pages for multi-page test, but only visited %d", totalPagesVisited)
	}
}

// Test SearchParameter pagination deterministic ordering
func TestSearchParameterPaginationDeterministicOrdering(t *testing.T) {
	backend := mockBackendWithoutSearchParameterSearch{}

	server := &rest.Server[model.R4]{
		Backend: backend,
	}

	// Make the same request multiple times to ensure consistent ordering
	numRuns := 5
	var responses []string

	for i := 0; i < numRuns; i++ {
		req := httptest.NewRequest("GET", "http://example.com/SearchParameter?_count=10", nil)
		req.Header.Set("Accept", "application/fhir+json")

		rr := httptest.NewRecorder()
		server.ServeHTTP(rr, req)

		if rr.Code != http.StatusOK {
			t.Errorf("Run %d: Expected status code %d, got %d", i+1, http.StatusOK, rr.Code)
		}

		responses = append(responses, rr.Body.String())
	}

	// All responses should be identical (deterministic ordering)
	firstResponse := responses[0]
	for i, response := range responses[1:] {
		if response != firstResponse {
			t.Errorf("Run %d: Response differs from first run. This indicates non-deterministic ordering.", i+2)
			t.Logf("First response: %s", firstResponse)
			t.Logf("Run %d response: %s", i+2, response)
			break
		}
	}

	t.Logf("All %d runs produced identical responses, confirming deterministic ordering", numRuns)
}
