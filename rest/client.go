package rest

import (
	"bytes"
	"cmp"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/damedic/fhir-toolbox-go/capabilities"
	"github.com/damedic/fhir-toolbox-go/capabilities/search"
	"github.com/damedic/fhir-toolbox-go/capabilities/update"
	"github.com/damedic/fhir-toolbox-go/fhirpath"
	"github.com/damedic/fhir-toolbox-go/model"
	"github.com/damedic/fhir-toolbox-go/rest/internal/encoding"
)

const (
	defaultClientFormat = FormatJSON
)

type internalClient[R model.Release] struct {
	baseURL *url.URL
	client  *http.Client
	format  Format
}

// CapabilityStatement retrieves the CapabilityStatement from the server's metadata endpoint.
func (c *internalClient[R]) CapabilityStatement(ctx context.Context) (model.CapabilityStatement, error) {
	if c.baseURL == nil {
		return nil, fmt.Errorf("base URL is nil")
	}

	u := c.baseURL.JoinPath("metadata")

	req, err := http.NewRequestWithContext(ctx, "GET", u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	// Set Accept header, using configured format or default
	requestFormat := cmp.Or(c.format, defaultClientFormat)
	req.Header.Set("Accept", string(requestFormat))

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("execute request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, c.handleErrorResponse(resp)
	}

	// Determine response format from Content-Type header
	responseFormat := c.detectResponseFormat(resp)

	res, err := encoding.DecodeResource[R](resp.Body, encoding.Format(responseFormat))
	if err != nil {
		return nil, fmt.Errorf("parse response: %w", err)
	}
	cs, ok := res.(model.CapabilityStatement)
	if !ok {
		return nil, fmt.Errorf("unexpected resource: expected CapabilityStatement, got %s", res.ResourceType())
	}
	return cs, nil
}

// Create creates a new resource.
func (c *internalClient[R]) Create(ctx context.Context, resource model.Resource) (model.Resource, error) {
	if c.baseURL == nil {
		return nil, fmt.Errorf("base URL is nil")
	}

	resourceType := resource.ResourceType()
	u := c.baseURL.JoinPath(resourceType)

	// Use configured format or default
	requestFormat := cmp.Or(c.format, defaultClientFormat)

	body, err := marshalResource(resource, encoding.Format(requestFormat))
	if err != nil {
		return nil, fmt.Errorf("marshal resource: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", u.String(), body)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	req.Header.Set("Content-Type", string(requestFormat))
	// Set Accept header
	req.Header.Set("Accept", string(requestFormat))

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("execute request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		return nil, c.handleErrorResponse(resp)
	}

	// Determine response format from Content-Type header
	responseFormat := c.detectResponseFormat(resp)
	return encoding.DecodeResource[R](resp.Body, encoding.Format(responseFormat))
}

// Read retrieves a resource by type and ID.
func (c *internalClient[R]) Read(ctx context.Context, resourceType, id string) (model.Resource, error) {
	if c.baseURL == nil {
		return nil, fmt.Errorf("base URL is nil")
	}

	u := c.baseURL.JoinPath(resourceType, id)

	req, err := http.NewRequestWithContext(ctx, "GET", u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	// Set Accept header, using configured format or default
	requestFormat := cmp.Or(c.format, defaultClientFormat)
	req.Header.Set("Accept", string(requestFormat))

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("execute request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, c.handleErrorResponse(resp)
	}

	// Determine response format from Content-Type header
	responseFormat := c.detectResponseFormat(resp)
	return encoding.DecodeResource[R](resp.Body, encoding.Format(responseFormat))
}

// Update updates an existing resource.
func (c *internalClient[R]) Update(ctx context.Context, resource model.Resource) (update.Result[model.Resource], error) {
	if c.baseURL == nil {
		return update.Result[model.Resource]{}, fmt.Errorf("base URL is nil")
	}

	resourceType := resource.ResourceType()
	id, hasID := resource.ResourceId()
	if !hasID {
		return update.Result[model.Resource]{}, fmt.Errorf("resource has no ID")
	}

	u := c.baseURL.JoinPath(resourceType, id)

	// Use configured format or default
	requestFormat := cmp.Or(c.format, defaultClientFormat)

	body, err := marshalResource(resource, encoding.Format(requestFormat))
	if err != nil {
		return update.Result[model.Resource]{}, fmt.Errorf("marshal resource: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "PUT", u.String(), body)
	if err != nil {
		return update.Result[model.Resource]{}, fmt.Errorf("create request: %w", err)
	}

	req.Header.Set("Content-Type", string(requestFormat))
	// Set Accept header
	req.Header.Set("Accept", string(requestFormat))

	resp, err := c.client.Do(req)
	if err != nil {
		return update.Result[model.Resource]{}, fmt.Errorf("execute request: %w", err)
	}
	defer resp.Body.Close()

	var created bool
	switch resp.StatusCode {
	case http.StatusOK:
		created = false
	case http.StatusCreated:
		created = true
	default:
		return update.Result[model.Resource]{}, c.handleErrorResponse(resp)
	}

	// Determine response format from Content-Type header
	responseFormat := c.detectResponseFormat(resp)
	updatedResource, err := encoding.DecodeResource[R](resp.Body, encoding.Format(responseFormat))
	if err != nil {
		return update.Result[model.Resource]{}, fmt.Errorf("parse response: %w", err)
	}

	return update.Result[model.Resource]{
		Resource: updatedResource,
		Created:  created,
	}, nil
}

// Delete deletes a resource by type and ID.
func (c *internalClient[R]) Delete(ctx context.Context, resourceType, id string) error {
	if c.baseURL == nil {
		return fmt.Errorf("base URL is nil")
	}

	u := c.baseURL.JoinPath(resourceType, id)

	req, err := http.NewRequestWithContext(ctx, "DELETE", u.String(), nil)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("execute request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		return c.handleErrorResponse(resp)
	}

	return nil
}

// Invoke invokes a FHIR operation at system, type, or instance level.
// It always uses POST for safety (covers affectsState=true operations).
// Paths:
//   - /$code
//   - /{resourceType}/$code
//   - /{resourceType}/{id}/$code
func (c *internalClient[R]) Invoke(
	ctx context.Context,
	resourceType, resourceID, code string,
	parameters model.Parameters,
) (model.Resource, error) {
	if c.baseURL == nil {
		return nil, fmt.Errorf("base URL is nil")
	}
	if code == "" {
		return nil, fmt.Errorf("operation code is empty")
	}

	// Build path according to level
	var u *url.URL
	switch {
	case resourceType == "" && resourceID == "":
		u = c.baseURL.JoinPath("$" + code)
	case resourceType != "" && resourceID == "":
		u = c.baseURL.JoinPath(resourceType, "$"+code)
	case resourceType != "" && resourceID != "":
		u = c.baseURL.JoinPath(resourceType, resourceID, "$"+code)
	default:
		return nil, fmt.Errorf("invalid operation target")
	}

	// Encode parameters when present
	requestFormat := cmp.Or(c.format, defaultClientFormat)
	var body io.Reader
	if parameters != nil {
		buf := &bytes.Buffer{}
		if err := encoding.Encode(buf, parameters, encoding.Format(requestFormat)); err != nil {
			return nil, fmt.Errorf("marshal parameters: %w", err)
		}
		body = buf
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), body)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	// Content-Type only when body is present
	if body != nil {
		req.Header.Set("Content-Type", string(requestFormat))
	}
	// Accept header to negotiate response
	req.Header.Set("Accept", string(requestFormat))

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("execute request: %w", err)
	}
	defer resp.Body.Close()

	// 204 No Content means success with no resource
	if resp.StatusCode == http.StatusNoContent {
		return nil, nil
	}
	if resp.StatusCode != http.StatusOK {
		return nil, c.handleErrorResponse(resp)
	}

	// decode result resource using response Content-Type
	responseFormat := c.detectResponseFormat(resp)
	out, err := encoding.DecodeResource[R](resp.Body, encoding.Format(responseFormat))
	if err != nil {
		return nil, fmt.Errorf("parse response: %w", err)
	}
	return out, nil
}

// Search performs a search operation for the given resource type with the specified options.
func (c *internalClient[R]) Search(ctx context.Context, resourceType string, parameters search.Parameters, options search.Options) (search.Result[model.Resource], error) {
	if c.baseURL == nil {
		return search.Result[model.Resource]{}, fmt.Errorf("base URL is nil")
	}

	opts := options

	var u *url.URL

	// If cursor is provided, use it as the complete URL (ignoring other options)
	if opts.Cursor != "" {
		var err error
		u, err = url.Parse(string(opts.Cursor))
		if err != nil {
			return search.Result[model.Resource]{}, fmt.Errorf("invalid cursor URL: %w", err)
		}
	} else {
		// Build URL from base and resource type with search parameters
		u = c.baseURL.JoinPath(resourceType)

		// Add query parameters from search options
		queryString := search.BuildQuery(parameters, opts)
		if queryString != "" {
			u.RawQuery = queryString
		}
	}

	req, err := http.NewRequestWithContext(ctx, "GET", u.String(), nil)
	if err != nil {
		return search.Result[model.Resource]{}, fmt.Errorf("create request: %w", err)
	}

	// Set Accept header, using configured format or default
	requestFormat := cmp.Or(c.format, defaultClientFormat)
	req.Header.Set("Accept", string(requestFormat))

	resp, err := c.client.Do(req)
	if err != nil {
		return search.Result[model.Resource]{}, fmt.Errorf("execute request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return search.Result[model.Resource]{}, c.handleErrorResponse(resp)
	}

	return parseSearchResponse[R](c, resp)
}

// Helper functions

// marshalResource marshals a FHIR resource to the specified format.
func marshalResource(resource model.Resource, format encoding.Format) (io.Reader, error) {
	buf := &bytes.Buffer{}
	if err := encoding.Encode(buf, resource, format); err != nil {
		return nil, err
	}
	return buf, nil
}

// detectResponseFormat determines the format from the response Content-Type header.
// Falls back to the configured format if Content-Type is not recognized.
func (c *internalClient[R]) detectResponseFormat(resp *http.Response) Format {
	contentType := resp.Header.Get("Content-Type")
	if contentType != "" {
		format := matchFormat(contentType)
		if format != "" {
			return format
		}
	}

	// Fall back to configured format or default
	format := cmp.Or(c.format, defaultClientFormat)
	return format
}

// parseSearchResponse parses a search bundle response using FHIRPath expressions.
func parseSearchResponse[R model.Release](c *internalClient[R], resp *http.Response) (search.Result[model.Resource], error) {
	// Determine response format from Content-Type header
	responseFormat := c.detectResponseFormat(resp)
	// decode the bundle as a resource using the generic decode function
	bundle, err := encoding.DecodeResource[R](resp.Body, encoding.Format(responseFormat))
	if err != nil {
		return search.Result[model.Resource]{}, fmt.Errorf("parse bundle: %w", err)
	}

	return parseSearchBundle(bundle)
}

// parseSearchBundle parses a Bundle into a search result using FHIRPath expressions.
func parseSearchBundle(bundle model.Resource) (search.Result[model.Resource], error) {
	// Extract all bundle entries using FHIRPath
	entryExpr := fhirpath.MustParse("entry")
	entryResults, err := fhirpath.Evaluate(context.Background(), bundle, entryExpr)
	if err != nil {
		return search.Result[model.Resource]{}, fmt.Errorf("extract bundle entries: %w", err)
	}

	var resources []model.Resource
	var included []model.Resource

	// Process each entry to determine if it's a match or include
	for _, entryElement := range entryResults {
		// Extract the resource from this entry
		resourceExpr := fhirpath.MustParse("resource")
		resourceResults, err := fhirpath.Evaluate(context.Background(), entryElement, resourceExpr)
		if err != nil {
			continue // Skip entries without resources
		}

		if len(resourceResults) == 0 {
			continue // Skip entries without resources
		}

		resource, ok := resourceResults[0].(model.Resource)
		if !ok {
			continue // Skip if not a resource
		}

		// Extract the search mode from this entry
		searchModeExpr := fhirpath.MustParse("search.mode")
		searchModeResults, err := fhirpath.Evaluate(context.Background(), entryElement, searchModeExpr)
		if err != nil || len(searchModeResults) == 0 {
			// Default to match if no search mode specified
			resources = append(resources, resource)
			continue
		}

		// Check the search mode value - handle different FHIR types
		var searchModeStr string
		if len(searchModeResults) > 0 {
			searchModeString, ok, err := searchModeResults[0].ToString(false)
			if err != nil {
				return search.Result[model.Resource]{}, fmt.Errorf("invalid search mode: %w", err)
			}
			if !ok {
				return search.Result[model.Resource]{}, fmt.Errorf("invalid search mode: %v", searchModeResults[0])
			}
			searchModeStr = string(searchModeString)
		}

		// Sort based on search mode
		switch searchModeStr {
		case "include":
			included = append(included, resource)
		case "match":
			fallthrough
		default:
			// Default to match for any other value or if mode is not specified
			resources = append(resources, resource)
		}
	}

	// Extract next link URL using FHIRPath
	nextLinkExpr := fhirpath.MustParse("link.where(relation = 'next').url")
	nextResults, err := fhirpath.Evaluate(context.Background(), bundle, nextLinkExpr)
	if err != nil {
		return search.Result[model.Resource]{}, fmt.Errorf("extract next link: %w", err)
	}

	result := search.Result[model.Resource]{
		Resources: resources,
		Included:  included,
	}

	// Set next cursor if available
	if len(nextResults) > 0 {
		// Use ToString() to convert any FHIRPath value to string
		nextURLString, ok, err := nextResults[0].ToString(false)
		if err != nil {
			return search.Result[model.Resource]{}, fmt.Errorf("error converting next link to string: %w", err)
		}
		if !ok {
			return search.Result[model.Resource]{}, fmt.Errorf("next link cannot be converted to string: %v", nextResults[0])
		}
		result.Next = search.Cursor(nextURLString)
	}

	return result, nil
}

// handleErrorResponse attempts to unmarshal an error response into an OperationOutcome
// and returns an appropriate error. If unmarshaling fails, it returns a generic status code error.
func (c *internalClient[R]) handleErrorResponse(resp *http.Response) error {
	// Read the response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("unexpected status code: %d (failed to read response body: %w)", resp.StatusCode, err)
	}

	// Try to unmarshal as OperationOutcome
	responseFormat := c.detectResponseFormat(resp)

	res, err := encoding.DecodeResource[R](bytes.NewReader(body), encoding.Format(responseFormat))
	if err != nil {
		// If we can't unmarshal as OperationOutcome, return generic error with response body
		return fmt.Errorf("unexpected status code: %d, response: %s", resp.StatusCode, string(body))
	}
	if e, ok := res.(error); ok {
		return e
	}
	return fmt.Errorf("unexpected status code: %d, response: %s", resp.StatusCode, string(body))
}

// iterator provides pagination functionality for search results.
type iterator[T model.Resource] struct {
	client capabilities.GenericSearch
	result search.Result[T]
	done   bool
	first  bool
}

// Iterator creates a new iterator for paginating through search results.
// It starts with the provided initial result and uses the client to fetch subsequent pages.
func Iterator[T model.Resource](client capabilities.GenericSearch, initialResult search.Result[T]) *iterator[T] {
	return &iterator[T]{
		client: client,
		result: initialResult,
		done:   false,
		first:  true,
	}
}

// Next returns the next page of search results.
// It returns io.EOF when there are no more pages available.
func (it *iterator[T]) Next(ctx context.Context) (search.Result[T], error) {
	// If we are done, return EOF
	if it.done {
		return search.Result[T]{}, io.EOF
	}

	// For the first call, return the initial result
	if it.first {
		it.first = false

		// If there is no next cursor, mark as done
		if it.result.Next == "" {
			it.done = true
		}

		return it.result, nil
	}

	// If there is no next cursor, we are done
	if it.result.Next == "" {
		it.done = true
		return search.Result[T]{}, io.EOF
	}

	// Fetch the next page using the cursor
	// The cursor contains the full URL, so we use it directly
	genericResult, err := it.client.Search(ctx, "", search.GenericParams{}, search.Options{
		Count:  len(it.result.Resources),
		Cursor: it.result.Next,
	})
	if err != nil {
		it.done = true
		return search.Result[T]{}, err
	}

	// Convert the generic result to our specific type
	nextResult := search.Result[T]{
		Resources: make([]T, len(genericResult.Resources)),
		Included:  genericResult.Included,
		Next:      genericResult.Next,
	}

	// Convert each resource
	for i, resource := range genericResult.Resources {
		if typedResource, ok := resource.(T); ok {
			nextResult.Resources[i] = typedResource
		}
	}

	// Update our state with the new result
	it.result = nextResult

	// If the next result has no cursor, we are done after this
	if nextResult.Next == "" {
		it.done = true
	}

	return nextResult, nil
}
