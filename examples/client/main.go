package main

import (
	"context"
	"fmt"
	"github.com/damedic/fhir-toolbox-go/capabilities/search"
	"github.com/damedic/fhir-toolbox-go/model/gen/r4"
	"github.com/damedic/fhir-toolbox-go/rest"
	"io"
	"log"
	"net/url"
)

func main() {
	baseURL, err := url.Parse("https://server.fire.ly")
	if err != nil {
		log.Fatal(err)
	}

	client := &rest.ClientR4{
		BaseURL: baseURL,
	}

	// Read patient
	patient, err := client.ReadPatient(context.Background(), "example")
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Read patient:\n%s\n", patient)

	// Search for patients using typed search parameters
	result, err := client.SearchPatient(context.Background(),
		r4.PatientParams{
			Birthdate: search.String("ge2000-01-01"), // Using String for date - search parameters accept string values for convenience
			Gender:    search.Token{Code: "female"},
		},
		search.Options{
			Count: 5,
		},
	)
	if err != nil {
		log.Fatal(err)
	}

	// Generic search parameters allow setting of modifiers
	result, err = client.SearchPatient(context.Background(),
		search.GenericParams{
			"birthdate":  search.String("ge2000-01-01"),
			"gender:not": search.Token{Code: "male"}, // with modifier
		},
		search.Options{
			Count: 5,
		},
	)
	if err != nil {
		log.Fatal(err)
	}

	// The client also implements the generic API for dynamic resource type use cases
	_, err = client.Search(context.Background(),
		"Patient",
		search.GenericParams{
			"birthdate":  search.String("ge2000-01-01"),
			"gender:not": search.Token{Code: "male"}, // with modifier
		},
		search.Options{
			Count: 5,
		},
	)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Found %d patients:\n%s\n", len(result.Resources), result)

	// Search for patient with pagination
	initialResult, err := client.SearchPatient(context.Background(),
		r4.PatientParams{
			Birthdate: search.String("ge2000-01-01"),
		},
		search.Options{
			Count: 5,
		},
	)
	if err != nil {
		log.Fatal(err)
	}

	iter := rest.Iterator(client, initialResult)
	pageNo := 0

	// Get 5 pages
	for pageNo < 5 {
		page, err := iter.Next(context.Background())
		if err != nil {
			// io.EOF signals that there are no more pages.
			if err == io.EOF {
				break
			}
			log.Fatalf("Failed to fetch next page: %v", err)
		}

		// handle page
		fmt.Printf("Page %d:\n%s\n\n", pageNo, page)

		pageNo++
	}
}
