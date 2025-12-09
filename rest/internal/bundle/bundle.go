package bundle

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/damedic/fhir-toolbox-go/capabilities/search"
	"github.com/damedic/fhir-toolbox-go/model"
	"github.com/damedic/fhir-toolbox-go/rest/internal/outcome"
)

// Links captures top-level bundle links we populate.
type Links struct {
	Self string
	Next string // empty if none
}

// Entry captures the minimal entry fields we need across releases.
type Entry struct {
	FullURL  string
	Resource model.Resource
	Mode     string // match | include
}

// BuildSearchBundle creates a release-specific search bundle from resources and paging info.
func BuildSearchBundle[R model.Release](
	resourceType string,
	result search.Result[model.Resource],
	usedParameters search.Parameters,
	usedOptions search.Options,
	capabilityStatement model.CapabilityStatement,
	resolveSearchParameter func(canonical string) (model.Element, error),
) (model.Resource, error) {
	baseURL, err := implementationBaseURL(capabilityStatement)
	if err != nil {
		return nil, outcome.Error[R]("fatal", "exception", fmt.Sprintf("invalid implementation URL in CapabilityStatement: %v", err))
	}

	// Build links
	links := Links{
		Self: relationLink(resourceType, usedParameters, usedOptions, capabilityStatement, resolveSearchParameter),
	}
	if result.Next != "" {
		nextOptions := usedOptions
		nextOptions.Cursor = result.Next
		links.Next = relationLink(resourceType, usedParameters, nextOptions, capabilityStatement, resolveSearchParameter)
	}

	// Build entries
	var entries []Entry
	for _, r := range result.Resources {
		e, err := makeEntry[R](baseURL, resourceType, r, "match")
		if err != nil {
			return nil, err
		}
		entries = append(entries, e)
	}
	for _, r := range result.Included {
		e, err := makeEntry[R](baseURL, r.ResourceType(), r, "include")
		if err != nil {
			return nil, err
		}
		entries = append(entries, e)
	}

	// Dispatch to release-specific builder
	var rel R
	switch any(rel).(type) {
	case model.R4:
		return buildR4(links, entries)
	case model.R4B:
		return buildR4B(links, entries)
	case model.R5:
		return buildR5(links, entries)
	default:
		panic("unsupported release")
	}
}

func implementationBaseURL(cs model.CapabilityStatement) (*url.URL, error) {
	var base string
	if impl := cs.Children("implementation"); len(impl) > 0 {
		if u := impl[0].Children("url"); len(u) > 0 {
			if s, ok, _ := u[0].ToString(false); ok {
				base = string(s)
			}
		}
	}
	return url.Parse(base)
}

func makeEntry[R model.Release](baseURL *url.URL, resourceType string, resource model.Resource, mode string) (Entry, error) {
	id, ok := resource.ResourceId()
	if !ok || id == "" {
		return Entry{}, outcome.Error[R]("fatal", "processing", fmt.Sprintf("missing id for resource of type '%s'", resource.ResourceType()))
	}
	path := strings.Trim(baseURL.Path, "/ ")
	var fullPath string
	if path == "" {
		fullPath = fmt.Sprintf("/%s/%s", resourceType, id)
	} else {
		fullPath = fmt.Sprintf("/%s/%s/%s", path, resourceType, id)
	}
	fullURL := url.URL{
		Scheme: baseURL.Scheme,
		Host:   baseURL.Host,
		Path:   fullPath,
	}
	return Entry{FullURL: fullURL.String(), Resource: resource, Mode: mode}, nil
}

// relationLink creates links to be used as Bundle.link URLs.
// It builds the query string based on parameters/options and the CapabilityStatement.
func relationLink(
	resourceType string,
	parameters search.Parameters,
	options search.Options,
	capabilityStatement model.CapabilityStatement,
	resolveSearchParameter func(canonical string) (model.Element, error),
) string {
	baseURL, err := implementationBaseURL(capabilityStatement)
	if err != nil {
		return ""
	}
	path := strings.Trim(baseURL.Path, "/ ")
	var fullPath string
	if path == "" {
		fullPath = fmt.Sprintf("/%s", resourceType)
	} else {
		fullPath = fmt.Sprintf("/%s/%s", path, resourceType)
	}
	link := url.URL{
		Scheme: baseURL.Scheme,
		Host:   baseURL.Host,
		Path:   fullPath,
	}
	link.RawQuery = search.BuildQuery(parameters, options)
	return link.String()
}

// Stubs replaced by release-specific files.
var buildR4 = func(links Links, entries []Entry) (model.Resource, error) { return nil, disabledErr[model.R4]() }
var buildR4B = func(links Links, entries []Entry) (model.Resource, error) { return nil, disabledErr[model.R4B]() }
var buildR5 = func(links Links, entries []Entry) (model.Resource, error) { return nil, disabledErr[model.R5]() }

func disabledErr[R model.Release]() error {
	r := model.ReleaseName[R]()
	return fmt.Errorf("release %s disabled by build tag; remove all build tags or add %s", r, r)
}
