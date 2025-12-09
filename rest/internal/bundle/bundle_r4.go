//go:build r4 || !(r4 || r4b || r5)

package bundle

import (
	"github.com/damedic/fhir-toolbox-go/model"
	r4 "github.com/damedic/fhir-toolbox-go/model/gen/r4"
	"github.com/damedic/fhir-toolbox-go/utils/ptr"
)

func init() {
	buildR4 = func(links Links, entries []Entry) (model.Resource, error) {
		out := r4.Bundle{
			Type: r4.Code{Value: ptr.To("searchset")},
			Link: []r4.BundleLink{
				{Relation: r4.String{Value: ptr.To("self")}, Url: r4.Uri{Value: ptr.To(links.Self)}},
			},
		}
		if links.Next != "" {
			out.Link = append(out.Link, r4.BundleLink{Relation: r4.String{Value: ptr.To("next")}, Url: r4.Uri{Value: ptr.To(links.Next)}})
		}
		out.Entry = make([]r4.BundleEntry, len(entries))
		for i, e := range entries {
			mode := e.Mode
			out.Entry[i] = r4.BundleEntry{
				FullUrl:  &r4.Uri{Value: ptr.To(e.FullURL)},
				Resource: e.Resource,
				Search:   &r4.BundleEntrySearch{Mode: &r4.Code{Value: &mode}},
			}
		}
		return out, nil
	}
}
