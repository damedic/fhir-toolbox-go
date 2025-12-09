package rest

import (
	"errors"
	"net/http"
	"slices"

	"github.com/damedic/fhir-toolbox-go/fhirpath"
	"github.com/damedic/fhir-toolbox-go/model"
	restoutcome "github.com/damedic/fhir-toolbox-go/rest/internal/outcome"
)

func errToOperationOutcome[R model.Release](err error) (int, model.Resource) {
	var oo model.Resource
	ok := errors.As(err, &oo)
	if ok && oo.ResourceType() == "OperationOutcome" {
		return toHTTPErrorStatus(oo), oo
	}

	res, _ := restoutcome.Build[R]("fatal", "exception", err.Error())
	return http.StatusInternalServerError, res
}

var issueCodeToHTTPStatus = map[string]int{
	// invalid content (generally client errors - 4xx)
	"invalid":   http.StatusBadRequest, // Broad category for invalid content
	"structure": http.StatusBadRequest, // Structure invalid
	"required":  http.StatusBadRequest, // Required element missing
	"value":     http.StatusBadRequest, // Element value invalid
	"invariant": http.StatusBadRequest, // Validation rule failed

	// security problem (client authentication/authorization errors - 4xx)
	"security":   http.StatusForbidden,    // General security issue (could be 401 or 403, 403 is safer default)
	"login":      http.StatusUnauthorized, // Login required (401)
	"unknown":    http.StatusUnauthorized, // Unknown user/authentication failed (401)
	"expired":    http.StatusUnauthorized, // Session expired (401)
	"forbidden":  http.StatusForbidden,    // Not authorized (403)
	"suppressed": http.StatusForbidden,    // Information suppressed due to permissions/rules (403)

	// processing failure (often client errors - 4xx, but could be server state - 4xx/5xx)
	"processing":       http.StatusBadRequest,            // General processing failure (final)
	"not-supported":    http.StatusNotImplemented,        // Operation/Resource/Profile not supported (501) or Bad Request (400) - 501 implies server capability missing
	"duplicate":        http.StatusConflict,              // Duplicate record (409)
	"multiple-matches": http.StatusBadRequest,            // Multiple matches found when one expected (client search too broad)
	"not-found":        http.StatusNotFound,              // Resource not found (404)
	"deleted":          http.StatusGone,                  // Resource deleted (410)
	"too-long":         http.StatusRequestEntityTooLarge, // Content too long (413)
	"code-invalid":     http.StatusBadRequest,            // Invalid code/system provided
	"extension":        http.StatusBadRequest,            // Unacceptable extension
	"too-costly":       http.StatusForbidden,             // Operation too costly (server policy refusal - 403) or 429 if related to rate limits
	"business-rule":    http.StatusBadRequest,            // Business rule violation (400 or 422 Unprocessable Entity)
	"conflict":         http.StatusConflict,              // Edit version conflict (409)

	// transient issue (server errors or temporary states - 5xx or 429)
	"transient":  http.StatusServiceUnavailable,  // General transient issue (503 implies retry)
	"lock-error": http.StatusServiceUnavailable,  // Lock failure (transient server issue - 503)
	"no-store":   http.StatusServiceUnavailable,  // Persistent store unavailable (503)
	"exception":  http.StatusInternalServerError, // Unexpected internal error (500)
	"timeout":    http.StatusGatewayTimeout,      // Internal timeout (504 often implies upstream timeout, 503 can also fit)
	"incomplete": http.StatusServiceUnavailable,  // Results possibly incomplete due to transient issues (could also be 200 OK with info, but 503 if core function failed)
	"throttled":  http.StatusTooManyRequests,     // Throttled / Rate limited (429)
}

func toHTTPErrorStatus(outcome model.Resource) int {
	// define severity levels in order of highest to lowest
	severityRank := map[string]int{
		"fatal":       3,
		"error":       2,
		"warning":     1,
		"information": 0,
	}

	highestSeverity := -1
	// start with 400, we only handle errors and warnings, so we can assume that
	highestStatusCodes := []int{http.StatusBadRequest}

	for _, issue := range outcome.Children("issue") {
		severity, ok, err := fhirpath.Singleton[fhirpath.String](issue.Children("severity"))
		if err != nil || !ok {
			// skip issues without severity or code
			continue
		}
		code, ok, err := fhirpath.Singleton[fhirpath.String](issue.Children("code"))
		if err != nil || !ok {
			// skip issues without severity or code
			continue
		}

		// check if this issue has a higher severity than what we've seen so far
		severityValue, ok := severityRank[string(severity)]
		if !ok {
			// unknown severity, skip
			continue
		}

		statusCode, ok := issueCodeToHTTPStatus[string(code)]
		if !ok {
			// unknown code, skip
			continue
		}

		if severityValue > highestSeverity {
			// found a higher severity, update both severity and status code
			highestSeverity = severityValue
			highestStatusCodes = []int{statusCode}
		} else if severityValue == highestSeverity {
			highestStatusCodes = append(highestStatusCodes, statusCode)
		}
	}

	if len(highestStatusCodes) == 1 {
		return highestStatusCodes[0]
	}

	return (slices.Max(highestStatusCodes) / 100) * 100
}
