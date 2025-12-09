//go:build r4b || !(r4 || r4b || r5)

package bundle

import (
	"github.com/damedic/fhir-toolbox-go/model"
	r4b "github.com/damedic/fhir-toolbox-go/model/gen/r4b"
	"github.com/damedic/fhir-toolbox-go/utils/ptr"
)

func init() {
	buildR4B = func(links Links, entries []Entry) (model.Resource, error) {
		out := r4b.Bundle{
			Type: r4b.Code{Value: ptr.To("searchset")},
			Link: []r4b.BundleLink{
				{Relation: r4b.String{Value: ptr.To("self")}, Url: r4b.Uri{Value: ptr.To(links.Self)}},
			},
		}
		if links.Next != "" {
			out.Link = append(out.Link, r4b.BundleLink{Relation: r4b.String{Value: ptr.To("next")}, Url: r4b.Uri{Value: ptr.To(links.Next)}})
		}
		out.Entry = make([]r4b.BundleEntry, len(entries))
		for i, e := range entries {
			mode := e.Mode
			out.Entry[i] = r4b.BundleEntry{
				FullUrl:  &r4b.Uri{Value: ptr.To(e.FullURL)},
				Resource: e.Resource,
				Search:   &r4b.BundleEntrySearch{Mode: &r4b.Code{Value: &mode}},
			}
		}
		return out, nil
	}
}
