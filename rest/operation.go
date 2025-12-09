package rest

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strings"

	"github.com/damedic/fhir-toolbox-go/capabilities"
	fhirpath "github.com/damedic/fhir-toolbox-go/fhirpath"
	"github.com/damedic/fhir-toolbox-go/model"
	"github.com/damedic/fhir-toolbox-go/rest/internal/encoding"
	restoutcome "github.com/damedic/fhir-toolbox-go/rest/internal/outcome"
)

// level represents the invocation level of an operation.
type level string

const (
	levelSystem   level = "system"
	levelType     level = "type"
	levelInstance level = "instance"
)

// handleOperation handles all operation invocations routed via patterns with '$' outside braces.
func (s *Server[R]) handleOperation(w http.ResponseWriter, r *http.Request) {
	requestFormat := s.detectFormat(r, "Content-Type")
	responseFormat := s.detectFormat(r, "Accept")

	level, resourceType, resourceID, code, _ := parseOperationRoute(r.URL.Path)

	// build Parameters from request
	var params model.Parameters
	switch r.Method {
	case http.MethodGet:
		p, err := buildParametersFromQuery[R](r.URL.Query())
		if err != nil {
			returnErr[R](w, err, responseFormat)
			return
		}
		params = p
	case http.MethodPost:
		if r.ContentLength == 0 {
			// empty parameters
			p, err := buildParametersFromQuery[R](url.Values{})
			if err != nil {
				returnErr[R](w, err, responseFormat)
				return
			}
			params = p
		} else {
			res, err := encoding.DecodeResource[R](r.Body, encoding.Format(requestFormat))
			if err != nil {
				returnErr[R](w, err, responseFormat)
				return
			}
			p, ok := res.(model.Parameters)
			if !ok {
				returnErr[R](w, fmt.Errorf("invalid Parameters resource"), responseFormat)
				return
			}
			params = p
		}
	default:
		returnErr[R](w, restoutcome.Error[R]("fatal", "processing", "unsupported method for operation"), responseFormat)
		return
	}

	out, err := s.dispatchOperation(r.Context(), r.Method, level, resourceType, resourceID, code, params)
	if err != nil {
		returnErr[R](w, err, responseFormat)
		return
	}
	if out == nil {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	returnResult(w, out, http.StatusOK, responseFormat)
}

// dispatchOperation performs the complete operation invocation flow.
func (s *Server[R]) dispatchOperation(
	ctx context.Context,
	method string,
	level level,
	resourceType, resourceID, code string,
	params model.Parameters,
) (model.Resource, error) {
	anyBackend, err := s.genericBackend()
	if err != nil {
		slog.Error("error in backend configuration", "err", err)
		return nil, err
	}

	backend, impl := anyBackend.(capabilities.GenericOperation)
	if !impl {
		return nil, notImplementedError[R]("operation")
	}

	// Get CapabilityStatement and resolve OperationDefinition
	csw, err := anyBackend.CapabilityStatement(ctx)
	if err != nil {
		slog.Error("error getting metadata", "err", err)
		return nil, err
	}
	cs := csw

	canonical, err := resolveOperationCanonical(cs, level, resourceType, code)
	if err != nil {
		return nil, restoutcome.Error[R]("fatal", "processing", err.Error())
	}
	opID := extractIDFromCanonical(canonical)

	// We need GenericRead to fetch the OperationDefinition
	readBackend, ok := anyBackend.(capabilities.GenericRead)
	if !ok {
		return nil, notImplementedError[R]("read for OperationDefinition")
	}
	opDef, err := readBackend.Read(ctx, "OperationDefinition", opID)
	if err != nil {
		return nil, err
	}

	// Validate allowed level
	if err := checkAllowedLevel(opDef, level, resourceType); err != nil {
		return nil, restoutcome.Error[R]("fatal", "not-supported", err.Error())
	}

	// If GET is used for an operation that affects state, reject
	if method == http.MethodGet {
		if affectsState(opDef) {
			return nil, restoutcome.Error[R]("fatal", "not-supported", "operation with affectsState=true must be invoked via POST")
		}
	}

	// Invoke backend
	out, err := backend.Invoke(ctx, resourceType, resourceID, code, params)
	if err != nil {
		return nil, err
	}

	return out, nil
}

// affectsState inspects an OperationDefinition to determine if affectsState is true.
func affectsState(opDef model.Resource) bool {
	e, ok := opDef.(fhirpath.Element)
	if !ok {
		return false
	}
	b, ok, err := fhirpath.Singleton[fhirpath.Boolean](e.Children("affectsState"))
	if err != nil || !ok {
		return false
	}
	return bool(b)
}

// resolveOperationCanonical locates the OperationDefinition canonical URL.
func resolveOperationCanonical(cs model.CapabilityStatement, lvl level, resourceType string, code string) (string, error) {
	// For type/instance level, look under the specific resource via FHIRPath traversal
	if lvl == levelType || lvl == levelInstance {
		for _, rest := range cs.Children("rest") {
			for _, res := range rest.Children("resource") {
				t, ok, err := fhirpath.Singleton[fhirpath.String](res.Children("type"))
				if err != nil || !ok || string(t) != resourceType {
					continue
				}
				for _, op := range res.Children("operation") {
					name, okN, errN := fhirpath.Singleton[fhirpath.String](op.Children("name"))
					if errN != nil || !okN {
						continue
					}
					if string(name) == code || string(name) == "$"+code {
						defElts := op.Children("definition")
						if len(defElts) > 0 {
							if def, okD, errD := defElts[0].ToString(false); errD == nil && okD {
								return string(def), nil
							}
						}
					}
				}
			}
		}
		return "", fmt.Errorf("operation '%s' not defined on %s level", code, string(lvl))
	}
	// For system level, search top-level operations
	for _, rest := range cs.Children("rest") {
		for _, op := range rest.Children("operation") {
			name, okN, errN := fhirpath.Singleton[fhirpath.String](op.Children("name"))
			if errN != nil || !okN {
				continue
			}
			if string(name) == code || string(name) == "$"+code {
				defElts := op.Children("definition")
				if len(defElts) > 0 {
					if def, okD, errD := defElts[0].ToString(false); errD == nil && okD {
						return string(def), nil
					}
				}
			}
		}
	}
	return "", fmt.Errorf("operation '%s' not defined on %s level", code, string(lvl))
}

// buildParametersFromQuery creates a release-specific Parameters resource from URL query values.
// Values are captured as valueString entries. Repeating parameters are represented
// by multiple Parameter entries with the same Name. Modifiers are kept as part of the Name.
func buildParametersFromQuery[R model.Release](query url.Values) (model.Parameters, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	// We'll compose JSON manually to ensure correct field names for parameters
	buf.WriteString(`{"resourceType":"Parameters","parameter":[`)
	first := true
	for k, values := range query {
		if k == "_format" {
			continue
		}
		for _, v := range values {
			if !first {
				buf.WriteByte(',')
			}
			first = false
			// {"name": <k>, "valueString": <v>}
			buf.WriteString("{\"name\":")
			if err := enc.Encode(k); err != nil { // enc.Encode writes trailing newline; we can trim it by removing last byte if newline
				return nil, fmt.Errorf("encode name: %w", err)
			}
			// Remove the newline added by Encode
			b := buf.Bytes()
			if l := len(b); l > 0 && b[l-1] == '\n' {
				buf.Truncate(l - 1)
			}
			buf.WriteString(",\"valueString\":")
			if err := enc.Encode(v); err != nil {
				return nil, fmt.Errorf("encode value: %w", err)
			}
			b = buf.Bytes()
			if l := len(b); l > 0 && b[l-1] == '\n' {
				buf.Truncate(l - 1)
			}
			buf.WriteByte('}')
		}
	}
	buf.WriteString("]}")

	res, err := encoding.DecodeResource[R](&buf, encoding.Format(FormatJSON))
	if err != nil {
		return nil, err
	}
	p, ok := res.(model.Parameters)
	if !ok {
		return nil, fmt.Errorf("decoded non-Parameters resource: %s", res.ResourceType())
	}
	return p, nil
}

// parseOperationRoute extracts operation invocation info from a URL path.
// Supports:
//   - /$code
//   - /{type}/$code
//   - /{type}/{resourceID}/$code
func parseOperationRoute(path string) (level, string, string, string, bool) {
	// trim spaces and trailing slashes (but keep root slash semantics)
	p := strings.Trim(path, " ")
	if p == "" || p == "/" {
		return "", "", "", "", false
	}
	// split and drop empty segs due to leading slash
	raw := strings.Split(p, "/")
	segs := make([]string, 0, len(raw))
	for _, s := range raw {
		if s != "" {
			segs = append(segs, s)
		}
	}
	if len(segs) == 0 {
		return "", "", "", "", false
	}
	last := segs[len(segs)-1]
	if !strings.HasPrefix(last, "$") || len(last) < 2 {
		return "", "", "", "", false
	}
	code := last[1:]
	switch len(segs) {
	case 1:
		return levelSystem, "", "", code, true
	case 2:
		return levelType, segs[0], "", code, true
	case 3:
		return levelInstance, segs[0], segs[1], code, true
	default:
		return "", "", "", "", false
	}
}

// valuesFromParameters converts a Parameters resource into url.Values by
// extracting string representations of value[x] fields where possible.
// valuesFromParameters was removed; operations use Parameters directly.

// checkAllowedLevel validates cross-version via FHIRPath.
func checkAllowedLevel(def model.Resource, level level, resourceType string) error {
	elem, ok := def.(fhirpath.Element)
	if !ok {
		return fmt.Errorf("unsupported OperationDefinition element")
	}
	system := boolField(elem, "system")
	typeLevel := boolField(elem, "type")
	instance := boolField(elem, "instance")
	allowed := stringList(elem, "resource")

	switch level {
	case levelSystem:
		if !system {
			return fmt.Errorf("operation not allowed at system level")
		}
	case levelType:
		if !typeLevel {
			return fmt.Errorf("operation not allowed at type level")
		}
		if !allowedOnResource(resourceType, allowed) {
			return fmt.Errorf("operation not allowed for resource type %s", resourceType)
		}
	case levelInstance:
		if !instance {
			return fmt.Errorf("operation not allowed at instance level")
		}
		if !allowedOnResource(resourceType, allowed) {
			return fmt.Errorf("operation not allowed for resource type %s", resourceType)
		}
	}
	return nil
}

func boolField(e fhirpath.Element, name string) bool {
	v, ok, err := fhirpath.Singleton[fhirpath.Boolean](e.Children(name))
	if err != nil || !ok {
		return false
	}
	return bool(v)
}

func stringList(e fhirpath.Element, name string) []string {
	var out []string
	for _, c := range e.Children(name) {
		s, ok, err := c.ToString(false)
		if err == nil && ok {
			out = append(out, string(s))
		}
	}
	return out
}

func allowedOnResource(resource string, allowed []string) bool {
	if len(allowed) == 0 {
		return true
	}
	for _, a := range allowed {
		if a == resource {
			return true
		}
	}
	return false
}
