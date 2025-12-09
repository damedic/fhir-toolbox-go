package assert

import (
	"bytes"
	"encoding/xml"
	"strings"
	"testing"

	"github.com/damedic/fhir-toolbox-go/testdata/assert/internal/diff"
)

func XMLEqual(t *testing.T, expected, actual string) {
	expectedFormatted := xmlFormat(expected)
	actualFormatted := xmlFormat(actual)
	if expectedFormatted != actualFormatted {
		t.Error(string(diff.Diff("expected", []byte(expectedFormatted), "actual", []byte(actualFormatted))))
	}
}

func xmlFormat(input string) string {
	var builder strings.Builder

	decoder := xml.NewDecoder(bytes.NewReader([]byte(input)))
	encoder := xml.NewEncoder(&builder)
	encoder.Indent("", "  ")

	for {
		t, err := decoder.Token()
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			panic(err)
		}

		switch x := t.(type) {
		case xml.CharData:
			t = xml.CharData(bytes.TrimSpace(x))
		case xml.Comment:
			// skip comments for now
			continue
		}

		if err := encoder.EncodeToken(t); err != nil {
			panic(err)
		}
	}
	err := encoder.Flush()
	if err != nil {
		panic(err)
	}

	return builder.String()
}
