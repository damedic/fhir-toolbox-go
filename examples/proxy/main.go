// A FHIR server that serves resources from another FHIR server.
package main

import (
	"log"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strings"

	"github.com/damedic/fhir-toolbox-go/model"
	"github.com/damedic/fhir-toolbox-go/rest"
)

func main() {
	var backendUrl = os.Args[1]

	textHandler := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	})
	slog.SetDefault(slog.New(textHandler))

	log.Printf("FHIR Server: %s", backendUrl)

	// Parse the backend URL
	baseURL, err := url.Parse(strings.TrimRight(backendUrl, "/"))
	if err != nil {
		log.Fatalf("Invalid backend URL: %v", err)
	}

	// Create a REST client to the backing server
	var client = &rest.ClientR5{
		BaseURL: baseURL,
		Client:  http.DefaultClient,
		Format:  rest.FormatJSON,
	}

	// Create the REST server.
	// You can plug in any backend you want here.
	server := &rest.Server[model.R5]{
		Backend: client,
	}

	// Start the server and listen on port 80.
	log.Println("listening on http://localhost")
	log.Fatal(http.ListenAndServe(":80", server))
}
