// Package search contains types and helpers to work with [FHIR Search].
// You can use these to provide search capabilities for your custom implementation.
//
// Currently, only an API with cursor-based pagination is supported.
// Parameters for offset based pagination might be added eventually if there is demand.
//
// # Example
//
//	import "github.com/damedic/fhir-toolbox-go/capabilities/search"
//
//	func (b *myAPI) SearchCapabilitiesObservation() r4.SearchCapabilities {
//		// return supported search capabilities
//		return r4.SearchCapabilities{
//			Parameters: map[string]r4.SearchParameter{
//				"_id": {Type: search.TypeToken},
//			},
//		}
//	}
//
//	func (b *myAPI) SearchObservation(ctx context.Context, options search.Options) (search.Result, error) {
//		// return the search result
//		return search.Result{ ... }, nil
//	}
//
// [FHIR Search]: https://hl7.org/fhir/search.html
package search

import (
	"fmt"
	"github.com/cockroachdb/apd/v3"
	"github.com/damedic/fhir-toolbox-go/fhirpath"
	"github.com/damedic/fhir-toolbox-go/model"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"
)

// Result contains the result of a search operation.
type Result[R model.Resource] struct {
	Resources []R
	Included  []model.Resource
	Next      Cursor
}

// Cursor is used for pagination.
//
// It references where the server shall pick up querying additional results
// for multi-page queries.
type Cursor string

// Options represents the options passed to a search implementation.
type Options struct {
	// Includes specifies the related resources to include in the search results.
	Includes []string
	// Count defines the maximum number of results to return.
	Count int
	// Cursor allows for pagination of large result sets.
	Cursor Cursor
}

type Parameters interface {
	Map() map[ParameterKey]MatchAll
}

// ParameterKey represents a key for a search parameter,
// consisting of a name and an optional modifier.
type ParameterKey struct {
	// Name is the name of the search parameter.
	Name string
	// Modifier is an optional modifier that can be applied to the search parameter,
	// such as `exact`, `contains`, `identifier`, etc.
	Modifier string
}

func (p ParameterKey) String() string {
	if p.Modifier == "" {
		return p.Name
	} else {
		return fmt.Sprintf("%s:%s", p.Name, p.Modifier)
	}
}

func (p ParameterKey) MarshalText() ([]byte, error) {
	return []byte(p.String()), nil
}

type GenericParams map[string]Criteria

func (p GenericParams) Map() map[ParameterKey]MatchAll {
	m := make(map[ParameterKey]MatchAll, len(p))
	for k, v := range p {
		splits := strings.Split(k, ":")
		paramName := splits[0]
		var paramModifier string
		if len(splits) > 1 {
			paramModifier = splits[1]
		}
		m[ParameterKey{Name: paramName, Modifier: paramModifier}] = v.MatchesAll()
	}
	return m
}

type internalParams map[ParameterKey]MatchAll

func (p internalParams) Map() map[ParameterKey]MatchAll {
	return p
}

type Criteria interface {
	MatchesAll() MatchAll
}

// All represents a slice of possible values for a single search parameter where each of the entry has to match.
type MatchAll []MatchAny

func (a MatchAll) MatchesAll() MatchAll {
	return a
}

// MatchAny represents a slice of possible values for a single search parameter,
// where only one of the values has to match.
type MatchAny []Value

func (o MatchAny) MatchesAll() MatchAll {
	return MatchAll{o}
}

type Type string

const (
	TypeNumber    Type = "number"
	TypeDate      Type = "date"
	TypeString    Type = "string"
	TypeToken     Type = "token"
	TypeReference Type = "reference"
	TypeComposite Type = "composite"
	TypeQuantity  Type = "quantity"
	TypeUri       Type = "uri"
	TypeSpecial   Type = "special"
)

type Prefix string

const (
	PrefixEqual          Prefix = "eq"
	PrefixNotEqual       Prefix = "ne"
	PrefixGreaterThan    Prefix = "gt"
	PrefixLessThan       Prefix = "lt"
	PrefixGreaterOrEqual Prefix = "ge"
	PrefixLessOrEqual    Prefix = "le"
	PrefixStartsAfter    Prefix = "sa"
	PrefixEndsBefore     Prefix = "eb"
)

var allPrefixes = []Prefix{
	PrefixEqual,
	PrefixNotEqual,
	PrefixGreaterThan,
	PrefixLessThan,
	PrefixGreaterOrEqual,
	PrefixLessOrEqual,
	PrefixStartsAfter,
	PrefixEndsBefore,
}

// ParseQuery parses search parameters and options from a [url.Values] query string.
//
// Only parameters supported by the backing implementation as described
// by the passed `capabilityStatement` for the given `resourceType` are used.
//
// The `resolveSearchParameter` function is used to resolve SearchParameter resources
// from their canonical URLs found in the CapabilityStatement.
//
// When `strict` is true, an error is returned if unsupported search parameters are encountered.
// When `strict` is false, unsupported search parameters are silently ignored.
//
// [Result modifying parameters] are parsed into separate fields on the [Options] object.
// All other parameters are returned as the Parameters result.
//
// [Result modifying parameters]: https://hl7.org/fhir/search.html#modifyingresults
func ParseQuery(
	capabilityStatement model.CapabilityStatement,
	resourceType string,
	resolveSearchParameter func(canonical string) (model.Element, error),
	params url.Values,
	tz *time.Location,
	maxCount, defaultCount int,
	strict bool,
) (Parameters, Options, error) {
	parameters := internalParams{}
	options := Options{
		Count: min(defaultCount, maxCount),
	}

	// Build a map of parameter names to their canonical URLs for quick lookup via FHIRPath
	parameterDefinitions := make(map[string]string)
	// Iterate rest.resource where type == resourceType and collect searchParam
	rests := capabilityStatement.Children("rest")
	for _, rest := range rests {
		for _, res := range rest.Children("resource") {
			// In release-specific models, "type" is usually a Code, not a raw string.
			// Use ToString on the first element to extract it robustly across releases.
			typeElts := res.Children("type")
			if len(typeElts) == 0 {
				continue
			}
			tStr, ok, err := typeElts[0].ToString(false)
			if err != nil || !ok {
				continue
			}
			if string(tStr) != resourceType {
				continue
			}
			for _, sp := range res.Children("searchParam") {
				// name
				name, okN, errN := fhirpath.Singleton[fhirpath.String](sp.Children("name"))
				if errN != nil || !okN {
					continue
				}
				// definition canonical string
				defElt := sp.Children("definition")
				if len(defElt) == 0 {
					continue
				}
				defStr, okD, errD := defElt[0].ToString(false)
				if errD != nil || !okD {
					continue
				}
				parameterDefinitions[string(name)] = string(defStr)
			}
		}
	}
	// DEBUG: remove after fixing tests
	// for k, v := range parameterDefinitions { fmt.Println("DEF:", k, v) }

	for k, v := range params {
		switch k {
		case "_count":
			count, err := parseCount(v, maxCount)
			if err != nil {
				return nil, Options{}, err
			}
			options.Count = count

		case "_cursor":
			cursor, err := parseCursor(v)
			if err != nil {
				return nil, Options{}, err
			}
			options.Cursor = cursor

		case "_include":
			options.Includes = v

		// other result modifying parameters which are not supported yet:
		// https://hl7.org/fhir/search.html#modifyingresults
		case "_contained", "_elements", "_graph", "_maxresults", "_revinclude", "_score", "_summary", "_total":

		default:
			splits := strings.Split(k, ":")
			param := ParameterKey{
				Name: splits[0],
			}
			if len(splits) > 1 {
				param.Modifier = splits[1]
			}

			canonical, ok := parameterDefinitions[param.Name]
			if !ok {
				if strict {
					return nil, Options{}, fmt.Errorf("unsupported search parameter: %s", param.String())
				}
				// only known parameters are forwarded
				continue
			}

			// Resolve the SearchParameter using the provided function
			sp, err := resolveSearchParameter(canonical)
			if err != nil || sp == nil {
				// Skip if SearchParameter cannot be resolved
				continue
			}

			ands, err := parseSearchParam(param, v, sp, tz)
			if err != nil {
				return nil, Options{}, err
			}

			parameters[param] = ands
		}
	}

	return parameters, options, nil
}

func parseCount(values []string, maxCount int) (int, error) {
	if len(values) != 1 {
		return 0, fmt.Errorf("multiple _count parameters")
	}
	count, err := strconv.Atoi(values[0])
	if err != nil {
		return 0, fmt.Errorf("invalid _count parameter: %w", err)
	}
	return min(count, maxCount), nil
}

func parseCursor(values []string) (Cursor, error) {
	if len(values) != 1 {
		return "", fmt.Errorf("multiple _cursor parameters")
	}
	return Cursor(values[0]), nil
}

func parseSearchParam(param ParameterKey, urlValues []string, sp model.Element, tz *time.Location) (MatchAll, error) {
	fhirpathType, ok, err := fhirpath.Singleton[fhirpath.String](sp.Children("type"))
	if !ok || err != nil {
		return MatchAll{}, fmt.Errorf("Parameter has no type: %v", sp)
	}
	resolvedType := Type(fhirpathType)

	var supportedModifiers []string
	for _, e := range sp.Children("modifier") {
		m, ok, err := e.ToString(false)
		if !ok || err != nil {
			return MatchAll{}, fmt.Errorf("parameter error reading modifiers: %v", sp)
		}
		supportedModifiers = append(supportedModifiers, string(m))
	}

	// When the :identifier modifier is used, the search value works as a token search.
	if resolvedType == TypeReference && param.Modifier == "identifier" {
		resolvedType = TypeToken
	}

	// empty modifiers in SearchParameters should mean all are supported
	if param.Modifier != "" && (len(supportedModifiers) == 0 || !slices.Contains(supportedModifiers, string(param.Modifier))) {
		return nil, fmt.Errorf("unsupported modifier for parameter %s, supported are: %s", param, supportedModifiers)
	}

	matchAll := make(MatchAll, 0, len(urlValues))
	for _, urlValue := range urlValues {
		splitStrings := strings.Split(urlValue, ",")

		matchAny := make(MatchAny, 0, len(splitStrings))
		for _, s := range splitStrings {
			value, err := parseSearchValue(resolvedType, s, tz)
			if err != nil {
				return nil, fmt.Errorf("invalid search value for parameter %s: %w", param, err)
			}
			matchAny = append(matchAny, value)
		}

		matchAll = append(matchAll, matchAny)
	}
	return matchAll, nil
}

func parseSearchValue(paramType Type, value string, tz *time.Location) (Value, error) {
	prefix := parseSearchValuePrefix(paramType, value)
	if prefix != "" {
		// all prefixes have a width of 2
		value = value[2:]
	}

	switch paramType {
	case TypeNumber:
		dec, _, err := apd.NewFromString(value)
		return Number{
			Prefix: prefix,
			Value:  dec,
		}, err
	case TypeDate:
		date, prec, err := ParseDate(value, tz)
		return Date{
			Prefix:    prefix,
			Precision: prec,
			Value:     date,
		}, err
	case TypeString:
		return String(value), nil
	case TypeToken:
		s := strings.Split(value, "|")
		switch len(s) {
		case 1:
			return Token{
				System: nil,
				Code:   s[0],
			}, nil
		case 2:
			system, err := url.Parse(s[0])
			if err != nil {
				return nil, fmt.Errorf("invalid token system %s: %w", value, err)
			}
			return Token{
				System: system,
				Code:   s[1],
			}, nil
		default:
			return nil, fmt.Errorf("invalid token %s", value)
		}
	case TypeReference:
		// if url, there may be a version appended
		urlSplit := strings.Split(value, "|")

		parsedURL, err := url.Parse(urlSplit[0])
		if err != nil {
			return nil, fmt.Errorf("invalid reference %s: %w", value, err)
		}

		if parsedURL.Scheme != "" {
			switch len(urlSplit) {
			case 1:
				return Reference{
					URL: parsedURL,
				}, nil
			case 2:
				return Reference{
					URL:     parsedURL,
					Version: urlSplit[1],
				}, nil
			default:
				return nil, fmt.Errorf("invalid reference %s", value)

			}
		}

		// no real URL, thus local reference
		localIdSplit := strings.Split(value, "/")
		switch len(localIdSplit) {
		case 1:
			return Reference{
				Id: localIdSplit[0],
			}, nil
		case 2:
			return Reference{
				Type: localIdSplit[0],
				Id:   localIdSplit[1],
			}, nil
		case 4:
			if localIdSplit[2] != "_history" {
				return nil, fmt.Errorf("invalid reference %s, expected _history at 3rd position", value)
			}
			return Reference{
				Type:    localIdSplit[0],
				Id:      localIdSplit[1],
				Version: localIdSplit[3],
			}, nil
		default:
			return nil, fmt.Errorf("invalid reference %s", value)
		}

	case TypeComposite:
		return Composite(strings.Split(value, "$")), nil

	case TypeQuantity:
		s := strings.Split(value, "|")
		number, _, err := apd.NewFromString(s[0])
		if err != nil {
			return nil, fmt.Errorf("invalid quantity number: %w", err)
		}

		switch len(s) {
		case 1:
			return Quantity{
				Prefix: prefix,
				Value:  number,
			}, nil
		case 3:
			system, err := url.Parse(s[1])
			if err != nil {
				return nil, fmt.Errorf("invalid quantity system %s: %w", value, err)
			}
			return Quantity{
				Prefix: prefix,
				Value:  number,
				System: system,
				Code:   s[2],
			}, nil

		default:
			return nil, fmt.Errorf("invalid quantity %s", value)
		}
	case TypeUri:
		parsedURL, err := url.Parse(value)
		if err != nil {
			return nil, fmt.Errorf("invalid reference: %w", err)
		}
		return Uri{
			Value: parsedURL,
		}, nil
	case TypeSpecial:
		return Special(value), nil

	default:
		return nil, fmt.Errorf("unsupported type %s", paramType)
	}
}

func parseSearchValuePrefix(typ Type, value string) Prefix {
	// all prefixes have a width of 2
	if len(value) < 2 {
		return ""
	}

	// only number, date and quantity can have prefixes
	if !slices.Contains([]Type{TypeNumber, TypeDate, TypeQuantity}, typ) {
		return ""
	}

	if !slices.Contains(allPrefixes, Prefix(value[:2])) {
		return ""
	}

	return Prefix(value[:2])
}

// BuildQuery creates a query string from parameters and options.
//
// Search parameters are sorted alphabetically, [result modifying parameters] like `_include`
// are appended at the end.
// The function is deterministic, the same input will always yield the same output.
//
// [result modifying parameters]: https://hl7.org/fhir/search.html#modifyingresults
func BuildQuery(parameters Parameters, opts Options) string {

	var builder strings.Builder

	if parameters != nil {
		builder.WriteString(parameterQuery(parameters).Encode())
	}

	if len(opts.Includes) > 0 {
		includes := append([]string{}, opts.Includes...)
		slices.Sort(includes)

		for _, include := range includes {
			if builder.Len() > 0 {
				builder.WriteByte('&')
			}
			builder.WriteString("_include=")
			builder.WriteString(url.QueryEscape(include))
		}
	}

	if opts.Cursor != "" {
		if builder.Len() > 0 {
			builder.WriteByte('&')
		}
		builder.WriteString("_cursor=")
		builder.WriteString(url.QueryEscape(string(opts.Cursor)))
	}

	if opts.Count > 0 {
		if builder.Len() > 0 {
			builder.WriteByte('&')
		}
		builder.WriteString("_count=")
		builder.WriteString(strconv.Itoa(opts.Count))
	}

	return builder.String()
}

// Query representing the search parameters.
//
// All contained values are sorted, but the returned [url.Values] is backed by a map.
// To obtain a deterministic query string you can call [url.Values.Encode], because
// it will sort the keys alphabetically.
func parameterQuery(p Parameters) url.Values {
	values := url.Values{}

	for key, criteria := range p.Map() {
		for _, matchAny := range criteria.MatchesAll() {
			if len(matchAny) == 0 {
				continue
			}

			nameWithModifier := key.Name
			if key.Modifier != "" {
				nameWithModifier = fmt.Sprintf("%s:%s", key.Name, key.Modifier)
			}

			s := make([]string, 0, len(matchAny))
			for _, v := range matchAny {
				s = append(s, v.String())
			}
			slices.Sort(s)

			values.Add(nameWithModifier, strings.Join(s, ","))
			slices.Sort(values[nameWithModifier])
		}

	}

	return values
}

// Value of a search parameter, determine the concrete type by type assertion.
//
//	switch t := value.(type) {
//	case search.Number:
//	  // handle search parameter of type number
//	}
type Value interface {
	Criteria
	fmt.Stringer
}

type Number struct {
	Prefix Prefix
	Value  *apd.Decimal
}

func (n Number) MatchesAll() MatchAll {
	return MatchAll{MatchAny{n}}
}

func (n Number) String() string {
	if n.Prefix != "" {
		return fmt.Sprintf("%s%s", n.Prefix, n.Value.String())
	} else {
		return n.Value.String()
	}
}

type Date struct {
	Prefix    Prefix
	Precision DatePrecision
	Value     time.Time
}

// DatePrecision represents the precision of date value.
type DatePrecision string

const (
	PrecisionYear       DatePrecision = "year"
	PrecisionMonth      DatePrecision = "month"
	PrecisionDay        DatePrecision = "day"
	PrecisionHourMinute DatePrecision = "hourMinute"
	PrecisionFullTime   DatePrecision = "time"
)

// Format strings for precision aware parsing and encoding.
const (
	DateFormatOnlyYear   = "2006"
	DateFormatUpToMonth  = "2006-01"
	DateFormatUpToDay    = "2006-01-02"
	DateFormatHourMinute = "2006-01-02T15:04Z07:00"
	DateFormatFullTime   = "2006-01-02T15:04:05.999999999Z07:00"
)

func ParseDate(value string, tz *time.Location) (time.Time, DatePrecision, error) {
	date, err := time.ParseInLocation(DateFormatOnlyYear, value, tz)
	if err == nil {
		return date, PrecisionYear, nil
	}
	date, err = time.ParseInLocation(DateFormatUpToMonth, value, tz)
	if err == nil {
		return date, PrecisionMonth, nil
	}
	date, err = time.ParseInLocation(DateFormatUpToDay, value, tz)
	if err == nil {
		return date, PrecisionDay, nil
	}
	date, err = time.ParseInLocation(DateFormatHourMinute, value, tz)
	if err == nil {
		return date, PrecisionHourMinute, nil
	}
	date, err = time.ParseInLocation(DateFormatFullTime, value, tz)
	if err == nil {
		return date, PrecisionFullTime, nil
	}
	return time.Time{}, "", err
}

func (d Date) MatchesAll() MatchAll {
	return MatchAll{MatchAny{d}}
}

func (d Date) String() string {
	b := strings.Builder{}
	if d.Prefix != "" {
		b.WriteString(string(d.Prefix))
	}
	switch d.Precision {
	case PrecisionYear:
		b.WriteString(d.Value.Format(DateFormatOnlyYear))
	case PrecisionMonth:
		b.WriteString(d.Value.Format(DateFormatUpToMonth))
	case PrecisionDay:
		b.WriteString(d.Value.Format(DateFormatUpToDay))
	case PrecisionHourMinute:
		b.WriteString(d.Value.Format(DateFormatHourMinute))
	default:
		b.WriteString(d.Value.Format(DateFormatFullTime))
	}
	return b.String()
}

type String string

func (s String) MatchesAll() MatchAll {
	return MatchAll{MatchAny{s}}
}

func (s String) String() string {
	return string(s)
}

type Token struct {
	// Go URLs can contain URIs
	System *url.URL
	Code   string
}

func (t Token) String() string {
	if t.System == nil {
		return t.Code
	} else {
		return fmt.Sprintf("%s|%s", t.System, t.Code)
	}
}

func (t Token) MatchesAll() MatchAll {
	return MatchAll{MatchAny{t}}
}

type Reference struct {
	Modifier string
	Id       string
	Type     string
	URL      *url.URL
	Version  string
}

func (r Reference) MatchesAll() MatchAll {
	return MatchAll{MatchAny{r}}
}

func (r Reference) String() string {
	if r.URL != nil {
		b := strings.Builder{}
		b.WriteString(r.URL.String())
		if r.Version != "" {
			b.WriteRune('|')
			b.WriteString(r.Version)
		}
		return b.String()
	}

	if r.Type == "" {
		return r.Id
	}

	b := strings.Builder{}
	b.WriteString(r.Type)
	b.WriteRune('/')
	b.WriteString(r.Id)
	if r.Version != "" {
		b.WriteString("/_history/")
		b.WriteString(r.Version)
	}
	return b.String()
}

type Composite []string

func (c Composite) String() string {
	return strings.Join(c, "$")
}

func (c Composite) MatchesAll() MatchAll {
	return MatchAll{MatchAny{c}}
}

type Quantity struct {
	Prefix Prefix
	Value  *apd.Decimal
	System *url.URL
	Code   string
}

func (q Quantity) MatchesAll() MatchAll {
	return MatchAll{MatchAny{q}}
}

func (q Quantity) String() string {
	b := strings.Builder{}
	b.WriteString(string(q.Prefix))
	b.WriteString(q.Value.String())
	if q.Code != "" {
		b.WriteRune('|')
		b.WriteString(q.System.String())
		b.WriteRune('|')
		b.WriteString(q.Code)
	}
	return b.String()
}

type Uri struct {
	// Go URLs can contain URIs
	Value *url.URL
}

func (u Uri) String() string {
	return u.Value.String()
}

func (u Uri) MatchesAll() MatchAll {
	return MatchAll{MatchAny{u}}
}

// Special string contains potential prefixes
type Special string

func (s Special) MatchesAll() MatchAll {
	return MatchAll{MatchAny{s}}
}

func (s Special) String() string {
	return string(s)
}

// Sealed interfaces for search parameters that accept both typed values and String
//
// These interfaces provide more flexible client usage by allowing both strongly-typed
// search values (like Date, Token) and simple string values.
//
// Examples:
//   params.Birthdate = search.Date{Value: time.Now(), Prefix: "ge"}  // strongly typed
//   params.Birthdate = search.String("ge2000-01-01")                 // string-based
//
// The sealed nature (via private methods) ensures only the appropriate search types
// can implement these interfaces, maintaining type safety while providing flexibility.

// StringOrString is a sealed interface that accepts String values
type StringOrString interface {
	Criteria
	sealedStringOrString()
}

// TokenOrString is a sealed interface that accepts Token and String values
type TokenOrString interface {
	Criteria
	sealedTokenOrString()
}

// DateOrString is a sealed interface that accepts Date and String values
type DateOrString interface {
	Criteria
	sealedDateOrString()
}

// ReferenceOrString is a sealed interface that accepts Reference and String values
type ReferenceOrString interface {
	Criteria
	sealedReferenceOrString()
}

// QuantityOrString is a sealed interface that accepts Quantity and String values
type QuantityOrString interface {
	Criteria
	sealedQuantityOrString()
}

// NumberOrString is a sealed interface that accepts Number and String values
type NumberOrString interface {
	Criteria
	sealedNumberOrString()
}

// UriOrString is a sealed interface that accepts Uri and String values
type UriOrString interface {
	Criteria
	sealedUriOrString()
}

// CompositeOrString is a sealed interface that accepts Composite and String values
type CompositeOrString interface {
	Criteria
	sealedCompositeOrString()
}

// SpecialOrString is a sealed interface that accepts Special and String values
type SpecialOrString interface {
	Criteria
	sealedSpecialOrString()
}

// Sealed interface implementations - String can be used for any parameter type
func (s String) sealedStringOrString()    {}
func (s String) sealedTokenOrString()     {}
func (s String) sealedDateOrString()      {}
func (s String) sealedReferenceOrString() {}
func (s String) sealedQuantityOrString()  {}
func (s String) sealedNumberOrString()    {}
func (s String) sealedUriOrString()       {}
func (s String) sealedCompositeOrString() {}
func (s String) sealedSpecialOrString()   {}

// Typed implementations
func (t Token) sealedTokenOrString()         {}
func (d Date) sealedDateOrString()           {}
func (r Reference) sealedReferenceOrString() {}
func (q Quantity) sealedQuantityOrString()   {}
func (n Number) sealedNumberOrString()       {}
func (u Uri) sealedUriOrString()             {}
func (c Composite) sealedCompositeOrString() {}
func (s Special) sealedSpecialOrString()     {}
