package model_test

import (
	"reflect"
	"testing"

	"github.com/damedic/fhir-toolbox-go/model"
	"github.com/damedic/fhir-toolbox-go/model/gen/r4"
	"github.com/damedic/fhir-toolbox-go/utils/ptr"
)

func TestMemSizeR4(t *testing.T) {
	tests := []struct {
		name    string
		element model.Element
		want    int
	}{
		{
			name:    "empty account",
			element: r4.Account{},
			want:    int(reflect.TypeOf(r4.Account{}).Size()),
		},
		{
			name:    "account with id",
			element: r4.Account{Id: &r4.Id{Value: ptr.To("1")}},
			want:    int(reflect.TypeOf(r4.Account{}).Size()+reflect.TypeOf(r4.Id{}).Size()+reflect.TypeOf("").Size()) + len("1"),
		},
		{
			name: "account with extensions",
			element: &r4.Account{
				Extension: []r4.Extension{
					{
						Url: "http://example.com",
					},
				},
			},
			want: int(reflect.TypeOf(r4.Account{}).Size()+reflect.TypeOf(r4.Extension{}).Size()) +
				// because Extension.url is not a pointer, the size of the string header is already included
				len("http://example.com"),
		},
		{
			name: "account with extensions sliced",
			element: &r4.Account{
				Extension: []r4.Extension{
					{
						Url: "http://example.com",
					},
					{},
				}[:1],
			},
			want: int(reflect.TypeOf(r4.Account{}).Size()+
				// unused capacity is counted as well
				2*reflect.TypeOf(r4.Extension{}).Size()) + len("http://example.com"),
		},
		{
			name: "bundle with entry",
			element: &r4.Bundle{
				Entry: []r4.BundleEntry{
					{
						Resource: &r4.Account{},
					},
				},
			},
			want: int(reflect.TypeOf(r4.Bundle{}).Size() +
				reflect.TypeOf(r4.BundleEntry{}).Size() +
				reflect.TypeOf(r4.Account{}).Size()),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.element.MemSize(); got != tt.want {
				t.Errorf("MemSize() = %v, want %v, MemSize() should return the size of the element", got, tt.want)
			}
		})
	}
}
