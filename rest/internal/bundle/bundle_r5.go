//go:build r5 || !(r4 || r4b || r5)

package bundle

import (
	"github.com/damedic/fhir-toolbox-go/model"
	r5 "github.com/damedic/fhir-toolbox-go/model/gen/r5"
	"github.com/damedic/fhir-toolbox-go/utils/ptr"
)

func init() {
	buildR5 = func(links Links, entries []Entry) (model.Resource, error) {
		out := r5.Bundle{
			Type: r5.Code{Value: ptr.To("searchset")},
			Link: []r5.BundleLink{
				{Relation: r5.Code{Value: ptr.To("self")}, Url: r5.Uri{Value: ptr.To(links.Self)}},
			},
		}
		if links.Next != "" {
			out.Link = append(out.Link, r5.BundleLink{Relation: r5.Code{Value: ptr.To("next")}, Url: r5.Uri{Value: ptr.To(links.Next)}})
		}
		out.Entry = make([]r5.BundleEntry, len(entries))
		for i, e := range entries {
			mode := e.Mode
			out.Entry[i] = r5.BundleEntry{
				FullUrl:  &r5.Uri{Value: ptr.To(e.FullURL)},
				Resource: e.Resource,
				Search:   &r5.BundleEntrySearch{Mode: &r5.Code{Value: &mode}},
			}
		}
		return out, nil
	}
}
