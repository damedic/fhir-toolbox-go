package assert

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/damedic/fhir-toolbox-go/testdata/assert/internal/diff"
)

func JSONEqual(t *testing.T, expected, actual string) {
	expectedFormatted := jsonFormat(expected)
	actualFormatted := jsonFormat(actual)
	if expectedFormatted != actualFormatted {
		if expectedFormatted != actualFormatted {
			t.Error(string(diff.Diff("expected", []byte(expectedFormatted), "actual", []byte(actualFormatted))))
		}
	}
}

func jsonFormat(input string) string {
	var obj map[string]any
	err := json.Unmarshal([]byte(input), &obj)
	if err != nil {
		panic(err)
	}

	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetIndent("", "  ")
	enc.SetEscapeHTML(false)
	err = enc.Encode(obj)
	if err != nil {
		panic(err)
	}

	return buf.String()
}
