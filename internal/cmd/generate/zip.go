package main

import (
	"archive/zip"
	"bytes"
	"encoding/json"
	"github.com/damedic/fhir-toolbox-go/internal/generate/model"
	"io"
	"log"
)

type bundles struct {
	resources    model.Bundle
	types        model.Bundle
	searchParams model.Bundle
	valueSets    model.Bundle
}

func readJSONFromZIP(path string) bundles {
	log.Println("opening zip archive...")
	zip, err := zip.OpenReader(path)
	if err != nil {
		log.Fatal(err)
	}
	defer zip.Close()

	log.Println("unmarshalling JSON...")
	return bundles{
		resources:    readAndParseJSON(&zip.Reader, "profiles-resources.json"),
		types:        readAndParseJSON(&zip.Reader, "profiles-types.json"),
		searchParams: readAndParseJSON(&zip.Reader, "search-parameters.json"),
		valueSets:    readAndParseJSON(&zip.Reader, "valuesets.json"),
	}

}

func readAndParseJSON(zip *zip.Reader, name string) model.Bundle {
	file, err := zip.Open(name)
	if err != nil {
		file, err = zip.Open("definitions.json/" + name)
	}
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	var buf bytes.Buffer
	_, err = io.Copy(&buf, file)

	var bundle model.Bundle
	err = json.Unmarshal(buf.Bytes(), &bundle)
	if err != nil {
		log.Fatal(err)
	}

	return bundle
}
