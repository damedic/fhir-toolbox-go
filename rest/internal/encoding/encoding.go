package encoding

import (
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"

	"github.com/damedic/fhir-toolbox-go/model"
	"github.com/damedic/fhir-toolbox-go/rest/internal/outcome"
)

type Format string

const (
	FormatJSON Format = "application/fhir+json"
	FormatXML  Format = "application/fhir+xml"
)

func disabledErr[R model.Release]() error {
	r := model.ReleaseName[R]()
	return fmt.Errorf("release %s disabled by build tag; remove all build tags or add %s", r, r)
}

func DecodeResource[R model.Release](r io.Reader, format Format) (model.Resource, error) {
	var release R
	switch any(release).(type) {
	case model.R4:
		return decodeR4Resource(r, format)
	case model.R4B:
		return decodeR4BResource(r, format)
	case model.R5:
		return decodeR5Resource(r, format)
	default:
		// This should never happen as long as we control all implementations of the Release interface.
		// This is achieved by sealing the interface. See the interface definition for more information.
		panic("unsupported release")
	}
}

var decodeR4Resource = func(r io.Reader, format Format) (model.Resource, error) {
	return nil, disabledErr[model.R4]()
}
var decodeR4BResource = func(r io.Reader, format Format) (model.Resource, error) {
	return nil, disabledErr[model.R4B]()
}
var decodeR5Resource = func(r io.Reader, format Format) (model.Resource, error) {
	return nil, disabledErr[model.R5]()
}

func decodingError[R model.Release](encoding string) error {
	return outcome.Error[R]("fatal", "processing", "error parsing "+encoding+" body")
}

func decode[R model.Release, T any](r io.Reader, format Format) (T, error) {
	switch format {
	case FormatJSON:
		return decodeJSON[R, T](r)
	case FormatXML:
		return decodeXML[R, T](r)
	default:
		return *new(T), fmt.Errorf("unsupported format: %s", format)
	}
}

func Encode[T any](w io.Writer, v T, format Format) error {
	switch format {
	case FormatJSON:
		return encodeJSON(w, v)
	case FormatXML:
		return encodeXML(w, v)
	default:
		return fmt.Errorf("unsupported format: %s", format)
	}
}

func encodeJSON[T any](w io.Writer, v T) error {
	encoder := json.NewEncoder(w)
	encoder.SetEscapeHTML(false)

	return encoder.Encode(v)
}

func decodeJSON[R model.Release, T any](r io.Reader) (T, error) {
	var v T
	if err := json.NewDecoder(r).Decode(&v); err != nil {
		return v, decodingError[R]("json")
	}
	return v, nil
}

func encodeXML[T any](w io.Writer, v T) error {
	if _, err := w.Write([]byte(xml.Header)); err != nil {
		return fmt.Errorf("encode xml: %w", err)
	}
	return xml.NewEncoder(w).Encode(v)
}

func decodeXML[R model.Release, T any](r io.Reader) (T, error) {
	var v T
	if err := xml.NewDecoder(r).Decode(&v); err != nil {
		return v, decodingError[R]("xml")
	}
	return v, nil
}
