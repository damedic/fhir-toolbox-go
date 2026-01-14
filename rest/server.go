// Package rest provides a FHIR REST API.
//
// Capabilities are detected by type assertion, a generated CapabilityStatement is served at the "/metadata" endpoint.
//
// Following interactions are currently supported:
//   - read
//   - create
//   - update
//   - delete
//   - search (parameters are passed down to the supplied backend implementation)
//
// # Base URL and routes
//
// You must pass a base URL using the config.
// This base URL is only used for building response Bundles (e.g., for generating links and self URLs).
// For supported interactions, the returned http.Handler has sub-routes installed.
// These routes are always installed at the root of this handler, regardless of the base URL.
//
// Currently, installed patterns are:
//   - capabilities: "GET /metadata"
//   - create: "POST /{type}"
//   - read: "GET /{type}/{id}"
//   - update: "PUT /{type}/{id}"
//   - delete: "DELETE /{type}/{id}"
//   - search: "GET /{type}"
//   - operations:
//   - system:   "GET /${code}",  "POST /${code}"
//   - type:     "GET /{type}/${code}",  "POST /{type}/${code}"
//   - instance: "GET /{type}/{resourceID}/${code}", "POST /{type}/{resourceID}/${code}"
//
// If you do not want your FHIR handlers installed at the root, use something like
//
//	mux.Handle("/path/", http.StripPrefix("/path", serverHandler)
//
// This allows you to implement multiple FHIR REST APIs on the same HTTP server
// (e.g. for multi-tenancy scenarios).
//
// # Pagination
//
// Cursor-based pagination can be implemented by the backend.
// Therefore the parameters "_count" and "_cursor" are passed down to the backend.
// The backend should use the cursor to determine where to continue fetching results
// and return a new cursor in the search.Result for the next page.
package rest

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/damedic/fhir-toolbox-go/capabilities"
	"github.com/damedic/fhir-toolbox-go/capabilities/search"
	"github.com/damedic/fhir-toolbox-go/capabilities/update"
	"github.com/damedic/fhir-toolbox-go/model"
	"github.com/damedic/fhir-toolbox-go/rest/internal/bundle"
	"github.com/damedic/fhir-toolbox-go/rest/internal/encoding"
	"github.com/damedic/fhir-toolbox-go/rest/internal/outcome"
	"github.com/damedic/fhir-toolbox-go/rest/internal/wrap"
)

var (
	defaultServerTimezone      = time.Local
	defaultServerMaxCount      = 500
	defaultServerDefaultCount  = 500
	defaultServerDefaultFormat = FormatJSON
)

// Server is a generic FHIR server type that registers and serves HTTP endpoints for FHIR resource interactions.
// It is parameterized with the FHIR Release version R as a type constraint.
// It supports configuration of timezone, default and maximum search result counts, and strict parameter handling.
//
// The Server uses a dynamic backend system that detects capabilities through type assertions.
// The Backend field can accept two different types of implementations:
//
// 1. Generic API implementations (resource-agnostic):
//   - capabilities.GenericCapabilities: For capability statement generation
//   - capabilities.GenericCreate: For resource creation operations
//   - capabilities.GenericRead: For resource retrieval operations
//   - capabilities.GenericUpdate: For resource update operations
//   - capabilities.GenericDelete: For resource deletion operations
//   - capabilities.GenericSearch: For resource search operations
//
// 2. Concrete API implementations (resource-specific):
//   - capabilities.ConcreteCapabilities: Required for concrete backends
//   - Resource-specific methods like PatientRead, PatientSearch, etc.
//
// The Server automatically detects which approach your backend uses and handles
// the appropriate conversions. For concrete implementations, it will wrap them
// in a generic interface adapter using wrap.Generic[R].
//
// If a backend doesn't implement a specific capability interface, the corresponding
// HTTP endpoint will return a "not-supported" OperationOutcome.
//
// Backends can implement only the capabilities they need, giving flexibility while
// maintaining type safety. The server handles validation and proper error responses.
//
// NOTE: When using the concrete API, you must implement the CapabilityBase method
// which provides the base CapabilityStatement that will be enhanced with the
// capabilities detected from your concrete implementation.
type Server[R model.Release] struct {
	// Backend is the actual concrete or generic FHIR handler.
	// This field can hold any implementation that satisfies at least one of the capability
	// interfaces from the capabilities package. The server will detect which operations
	// are supported through type assertions at runtime.
	Backend any

	// Timezone used for parsing date parameters without timezones.
	// Defaults to current server timezone.
	Timezone *time.Location
	// MaxCount of search bundle entries.
	// Defaults to 500.
	MaxCount int
	// DefaultCount of search bundle entries.
	// Defaults to 500.
	DefaultCount int
	// DefaultFormat of the server.
	// Defaults to JSON.
	DefaultFormat Format
	// StrictSearchParameters when true causes the server to return an error
	// if unsupported search parameters are used. When false (default),
	// unsupported search parameters are silently ignored.
	StrictSearchParameters bool

	// internal fields
	muxMu sync.Mutex
	mux   *http.ServeMux
}

func (s *Server[R]) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if s.mux == nil {
		s.registerRoutes()
	}
	// Intercept operation-style routes like /$code, /{type}/$code, /{type}/{id}/$code
	if _, _, _, _, ok := parseOperationRoute(request.URL.Path); ok {
		s.handleOperation(writer, request)
		return
	}
	s.mux.ServeHTTP(writer, request)
}

func (s *Server[R]) registerRoutes() {
	s.muxMu.Lock()
	defer s.muxMu.Unlock()

	// double check, mux might have been set in the background while waiting
	if s.mux != nil {
		return
	}

	s.mux = http.NewServeMux()
	s.mux.Handle("GET /metadata", http.HandlerFunc(s.handleMetadata))
	s.mux.Handle("POST /{type}", http.HandlerFunc(s.handleCreate))
	s.mux.Handle("GET /{type}/{id}", http.HandlerFunc(s.handleRead))
	s.mux.Handle("PUT /{type}/{id}", http.HandlerFunc(s.handleUpdate))
	s.mux.Handle("DELETE /{type}/{id}", http.HandlerFunc(s.handleDelete))
	s.mux.Handle("GET /{type}", http.HandlerFunc(s.handleSearch))
}

func (s *Server[R]) genericBackend() (capabilities.GenericCapabilities, error) {
	// Always wrap - wrapper handles concrete vs generic precedence
	return wrap.Generic[R](s.Backend)
}

func (s *Server[R]) handleMetadata(w http.ResponseWriter, r *http.Request) {
	responseFormat := s.detectFormat(r, "Accept")

	backend, err := s.genericBackend()
	if err != nil {
		slog.Error("error in backend configuration", "err", err)
		returnErr[R](w, err, responseFormat)
		return
	}

	capabilityStatement, err := backend.CapabilityStatement(r.Context())
	if err != nil {
		slog.Error("error getting metadata", "err", err)
		returnErr[R](w, err, responseFormat)
		return
	}

	// The CapabilityStatement comes fully configured from the backend
	// Unwrap and return the concrete resource
	returnResult(w, capabilityStatement, http.StatusOK, responseFormat)

}

func (s *Server[R]) handleCreate(w http.ResponseWriter, r *http.Request) {
	requestFormat := s.detectFormat(r, "Content-Type")
	responseFormat := s.detectFormat(r, "Accept")
	resourceType := r.PathValue("type")

	anyBackend, err := s.genericBackend()
	if err != nil {
		slog.Error("error in backend configuration", "err", err)
		returnErr[R](w, err, responseFormat)
		return
	}
	backend, impl := anyBackend.(capabilities.GenericCreate)

	if !checkInteractionImplemented[R](impl, "create", w, responseFormat) {
		return
	}

	resource, err := dispatchCreate[R](r, backend, requestFormat, resourceType)
	if err != nil {
		slog.Error("error creating resource", "resourceType", resourceType, "err", err)
		returnErr[R](w, err, responseFormat)
		return
	}

	// fall back to empty string if id is not set
	id, _ := resource.ResourceId()
	baseURL := getBaseURL(r)
	w.Header().Set("Location", baseURL.JoinPath(resourceType, id).String())

	returnResult(w, resource, http.StatusCreated, responseFormat)
}

func dispatchCreate[R model.Release](
	r *http.Request,
	backend capabilities.GenericCreate,
	requestFormat Format,
	resourceType string,
) (model.Resource, error) {
	resource, err := encoding.DecodeResource[R](r.Body, encoding.Format(requestFormat))
	if err != nil {
		return nil, err
	}

	if resourceType != resource.ResourceType() {
		return nil, unexpectedResourceError[R](resourceType, resource.ResourceType())
	}

	createdResource, err := backend.Create(r.Context(), resource)
	if err != nil {
		return nil, err
	}

	return createdResource, nil
}

func (s *Server[R]) handleRead(w http.ResponseWriter, r *http.Request) {
	responseFormat := s.detectFormat(r, "Accept")
	resourceType := r.PathValue("type")
	resourceID := r.PathValue("id")

	anyBackend, err := s.genericBackend()
	if err != nil {
		slog.Error("error in backend configuration", "err", err)
		returnErr[R](w, err, responseFormat)
		return
	}
	backend, impl := anyBackend.(capabilities.GenericRead)

	if !checkInteractionImplemented[R](impl, "read", w, responseFormat) {
		return
	}

	resource, err := dispatchRead(r.Context(), backend, resourceType, resourceID)
	if err != nil {
		slog.Error("error reading resource", "resourceType", resourceType, "err", err)
		returnErr[R](w, err, responseFormat)
		return
	}

	returnResult(w, resource, http.StatusOK, responseFormat)
}

func dispatchRead(
	ctx context.Context,
	backend capabilities.GenericRead,
	resourceType string,
	resourceID string,
) (model.Resource, error) {
	resource, err := backend.Read(ctx, resourceType, resourceID)
	if err != nil {
		return nil, err
	}
	return resource, nil
}

func (s *Server[R]) handleUpdate(w http.ResponseWriter, r *http.Request) {
	requestFormat := s.detectFormat(r, "Content-Type")
	responseFormat := s.detectFormat(r, "Accept")
	resourceType := r.PathValue("type")
	resourceID := r.PathValue("id")

	anyBackend, err := s.genericBackend()
	if err != nil {
		slog.Error("error in backend configuration", "err", err)
		returnErr[R](w, err, responseFormat)
		return
	}
	backend, impl := anyBackend.(capabilities.GenericUpdate)

	if !checkInteractionImplemented[R](impl, "update", w, responseFormat) {
		return
	}

	result, err := dispatchUpdate[R](r, backend, requestFormat, resourceType, resourceID)
	if err != nil {
		slog.Error("error updating resource", "resourceType", resourceType, "id", resourceID, "err", err)
		returnErr[R](w, err, responseFormat)
		return
	}

	// set Location header with the resource's logical ID
	// the dispatchUpdate function checks that the path id matches the id included in the resource
	baseURL := getBaseURL(r)
	w.Header().Set("Location", baseURL.JoinPath(resourceType, resourceID).String())

	status := http.StatusOK
	if result.Created {
		status = http.StatusCreated
	}

	returnResult(w, result.Resource, status, responseFormat)
}

func dispatchUpdate[R model.Release](
	r *http.Request,
	backend capabilities.GenericUpdate,
	requestFormat Format,
	resourceType string,
	resourceID string,
) (update.Result[model.Resource], error) {
	resource, err := encoding.DecodeResource[R](r.Body, encoding.Format(requestFormat))
	if err != nil {
		return update.Result[model.Resource]{}, err
	}

	if resourceType != resource.ResourceType() {
		return update.Result[model.Resource]{}, unexpectedResourceError[R](resourceType, resource.ResourceType())
	}

	// Verify that the resource ID in the URL matches the resource ID in the body
	id, ok := resource.ResourceId()
	if !ok || id != resourceID {
		return update.Result[model.Resource]{}, outcome.Error[R](
			"fatal",
			"processing",
			fmt.Sprintf("resource ID in URL (%s) does not match resource ID in body (%s)", resourceID, id),
		)
	}

	result, err := backend.Update(r.Context(), resource)
	if err != nil {
		return update.Result[model.Resource]{}, err
	}

	return result, nil
}

func (s *Server[R]) handleDelete(w http.ResponseWriter, r *http.Request) {
	responseFormat := s.detectFormat(r, "Accept")
	resourceType := r.PathValue("type")
	resourceID := r.PathValue("id")

	anyBackend, err := s.genericBackend()
	if err != nil {
		slog.Error("error in backend configuration", "err", err)
		returnErr[R](w, err, responseFormat)
		return
	}
	backend, impl := anyBackend.(capabilities.GenericDelete)

	if !checkInteractionImplemented[R](impl, "delete", w, responseFormat) {
		return
	}

	err = dispatchDelete(r.Context(), backend, resourceType, resourceID)
	if err != nil {
		slog.Error("error deleting resource", "resourceType", resourceType, "id", resourceID, "err", err)
		returnErr[R](w, err, responseFormat)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func dispatchDelete(
	ctx context.Context,
	backend capabilities.GenericDelete,
	resourceType string,
	resourceID string,
) error {
	return backend.Delete(ctx, resourceType, resourceID)
}

func (s *Server[R]) handleSearch(w http.ResponseWriter, r *http.Request) {
	responseFormat := s.detectFormat(r, "Accept")
	resourceType := r.PathValue("type")

	anyBackend, err := s.genericBackend()
	if err != nil {
		slog.Error("error in backend configuration", "err", err)
		returnErr[R](w, err, responseFormat)
		return
	}
	backend, impl := anyBackend.(capabilities.GenericSearch)

	if !checkInteractionImplemented[R](impl, "search-type", w, responseFormat) {
		return
	}

	resource, err := dispatchSearch[R](
		r,
		backend,
		resourceType,
		r.URL.Query(),
		cmp.Or(s.Timezone, defaultServerTimezone),
		cmp.Or(s.MaxCount, defaultServerMaxCount),
		cmp.Or(s.DefaultCount, defaultServerDefaultCount),
		s.StrictSearchParameters,
	)
	if err != nil {
		slog.Error("error reading searching", "resourceType", resourceType, "err", err)
		returnErr[R](w, err, responseFormat)
		return
	}

	returnResult(w, resource, http.StatusOK, responseFormat)
}

func dispatchSearch[R model.Release](
	r *http.Request,
	backend capabilities.GenericSearch,
	resourceType string,
	parameters url.Values,
	tz *time.Location,
	maxCount, defaultCount int,
	strictSearchParameters bool,
) (model.Resource, error) {
	ctx := r.Context()
	// Get CapabilityStatement to extract SearchParameter information
	csWrapper, err := backend.CapabilityStatement(ctx)
	if err != nil {
		return nil, err
	}
	capabilityStatement := csWrapper

	// Create a SearchParameter resolver function
	resolveSearchParameter := func(canonical string) (model.Element, error) {
		// Try to resolve SearchParameter using Read operation if available
		if readBackend, ok := backend.(capabilities.GenericRead); ok {
			// Extract SearchParameter ID from canonical URL
			searchParamId := extractIDFromCanonical(canonical)
			if searchParamId == "" {
				return nil, fmt.Errorf("cannot resolve SearchParameter from canonical URL: %s", canonical)
			}
			resource, err := readBackend.Read(ctx, "SearchParameter", searchParamId)
			if err != nil {
				return nil, err
			}
			// Return the SearchParameter resource as a model.Element
			return resource, nil
		}
		// Return error if SearchParameter cannot be resolved
		return nil, fmt.Errorf("cannot resolve SearchParameter from canonical URL: %s", canonical)
	}

	searchParameters, options, err := parseSearchOptions[R](
		capabilityStatement,
		resourceType,
		resolveSearchParameter,
		parameters,
		tz, maxCount, defaultCount,
		strictSearchParameters,
	)
	if err != nil {
		return nil, err
	}

	resources, err := backend.Search(ctx, resourceType, searchParameters, options)
	if err != nil {
		return nil, err
	}

	bundle, err := bundle.BuildSearchBundle[R](resourceType, resources, searchParameters, options, capabilityStatement, resolveSearchParameter)
	if err != nil {
		return nil, err
	}

	return bundle, nil
}

func parseSearchOptions[R model.Release](
	capabilityStatement model.CapabilityStatement,
	resourceType string,
	resolveSearchParameter func(canonical string) (model.Element, error),
	params url.Values,
	tz *time.Location,
	maxCount, defaultCount int,
	strict bool) (search.Parameters, search.Options, error) {
	parameters, options, err := search.ParseQuery(capabilityStatement, resourceType, resolveSearchParameter, params, tz, maxCount, defaultCount, strict)
	if err != nil {
		return nil, search.Options{}, searchError[R](err.Error())
	}
	return parameters, options, nil
}

type Format encoding.Format

const (
	FormatJSON = Format(encoding.FormatJSON)
	FormatXML  = Format(encoding.FormatXML)
)

var (
	alternateFormatsJSON = []string{"application/json", "text/json", "json"}
	alternateFormatsXML  = []string{"application/xml", "text/xml", "xml"}
)

func (s *Server[R]) detectFormat(r *http.Request, headerName string) Format {
	// url parameter overrides the Accept header
	formatQuery := r.URL.Query()["_format"]
	if len(formatQuery) > 0 {
		format := matchFormat(formatQuery[0])
		if format != "" {
			return format
		}
	}

	for _, accept := range r.Header[headerName] {
		format := matchFormat(accept)
		if format != "" {
			return format
		}
	}
	return cmp.Or(s.DefaultFormat, defaultServerDefaultFormat)
}

// UnmarshalJSON implements json.Unmarshaler for Format
func (f *Format) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}

	format := matchFormat(s)
	if format == "" {
		return fmt.Errorf("unsupported format: %s", s)
	}

	*f = Format(format)
	return nil
}

func matchFormat(contentType string) Format {
	switch {
	case contentType == string(FormatJSON) || slices.Contains(alternateFormatsJSON, contentType):
		return FormatJSON
	case contentType == string(FormatXML) || slices.Contains(alternateFormatsXML, contentType):
		return FormatXML
	}
	return ""
}

func returnErr[R model.Release](w http.ResponseWriter, err error, format Format) {
	status, oo := errToOperationOutcome[R](err)
	returnResult(w, oo, status, format)
}

func returnResult[T any](w http.ResponseWriter, r T, status int, format Format) {
	w.Header().Set("Content-Type", string(format))
	w.WriteHeader(status)

	err := encoding.Encode(w, r, encoding.Format(format))
	if err != nil {
		// we were not able to return an application level error (OperationOutcome)
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func checkInteractionImplemented[R model.Release](
	implemented bool,
	interaction string,
	w http.ResponseWriter,
	format Format,
) bool {
	if !implemented {
		slog.Error("interaction not implemented by backend", "interaction", interaction)
		returnErr[R](w, notImplementedError[R](interaction), format)
		return false
	}
	return true
}

func notImplementedError[R model.Release](interaction string) error {
	return outcome.Error[R](
		"fatal",
		"not-supported",
		fmt.Sprintf("%s interaction not implemented", interaction),
	)
}

// extractIDFromCanonical returns the last path segment of a canonical URL.
func extractIDFromCanonical(canonical string) string {
	if canonical == "" {
		return ""
	}
	parts := strings.Split(strings.TrimRight(canonical, "/"), "/")
	return parts[len(parts)-1]
}

func unexpectedResourceError[R model.Release](expectedType string, gotType string) error {
	return outcome.Error[R](
		"fatal",
		"processing",
		fmt.Sprintf("unexpected resource: expected %s, got %s", expectedType, gotType),
	)
}

func searchError[R model.Release](msg string) error {
	return outcome.Error[R]("fatal", "processing", msg)
}

// getBaseURL extracts the base URL from the request
func getBaseURL(r *http.Request) *url.URL {
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}

	// Check for X-Forwarded-Proto header (common in load balancer setups)
	if proto := r.Header.Get("X-Forwarded-Proto"); proto != "" {
		scheme = proto
	}

	host := r.Host
	if host == "" {
		host = "localhost"
	}

	return &url.URL{
		Scheme: scheme,
		Host:   host,
	}
}
