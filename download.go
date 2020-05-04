package main

import (
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
)

// download schema url example
// https://schema_registry_host/subjects/dev_oos_geofencing-value/versions/4/schema

type DownloadCommand struct {
	httpClient *http.Client
	cmd        *flag.FlagSet
	host       *string
	file       *string
	outdir     *string
}

func (dc *DownloadCommand) downloadSchema(subject, version, ext string) error {
	resp, err := dc.httpClient.Get(fmt.Sprintf("%s/subjects/%s/versions/%s/schema", *dc.host, subject, version))
	if err != nil {
		return errors.New("failed to get schema")
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode > 300 {
		return errors.New(fmt.Sprintf("got response with status: %s, statusCode: %d\n", resp.Status, resp.StatusCode))
	}
	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return errors.New("failed to read body")
	}
	if _, err := os.Stat(fmt.Sprintf("%s/%s", *dc.outdir, subject)); os.IsNotExist(err) {
		if err := os.Mkdir(fmt.Sprintf("%s/%s", *dc.outdir, subject), os.ModePerm); err != nil {
			return errors.New("failed to create dir")
		}
	}
	err = ioutil.WriteFile(fmt.Sprintf("%s/%s/schema%s", *dc.outdir, subject, ext), data, os.ModePerm)
	if err != nil {
		return errors.New("failed to write file")
	}
	return nil
}

func (dc *DownloadCommand) Do() {
	file, err := os.Open(*dc.file)
	if err != nil {
		fmt.Println("failed to open a file")
		os.Exit(1)
	}
	defer file.Close()
	reader := csv.NewReader(file)
	reader.FieldsPerRecord = 3 // subject,version
	records, err := reader.ReadAll()
	if err != nil {
		fmt.Println("failed to read file")
		os.Exit(1)
	}

	if _, err := os.Stat(*dc.outdir); os.IsNotExist(err) {
		if err := os.Mkdir(*dc.outdir, os.ModePerm); err != nil {
			fmt.Println("failed to create dir")
			os.Exit(1)
		}
	}

	for _, record := range records[1:] { // skip header line
		var subject, version, ext = record[0], record[1], record[2]
		if err := dc.downloadSchema(subject, version, ext); err != nil {
			fmt.Printf("failed to download schema for subject %s, version %s: %s\n", subject, version, err.Error())
			os.Exit(1)
		}
	}
}

func (dc *DownloadCommand) Parse() {
	dc.cmd.Parse(os.Args[2:])
	if !dc.cmd.Parsed() {
		fmt.Println("failed to parse command")
		os.Exit(1)
	}
	if *dc.host == "" {
		fmt.Println("host is required")
		os.Exit(1)
	}
	if *dc.file == "" {
		fmt.Println("file is required")
		os.Exit(1)
	}
}

func NewDownloadCommand() *DownloadCommand {
	downloadCommand := flag.NewFlagSet("download", flag.ExitOnError)

	host := downloadCommand.String("host", "", "Schema registry host")
	file := downloadCommand.String("file", "./schemas.csv", "Path to .csv file")
	outdir := downloadCommand.String("outdir", "./api", "Path to out dir")

	client := &http.Client{Transport: http.DefaultTransport}

	return &DownloadCommand{httpClient: client, cmd: downloadCommand, host: host, file: file, outdir: outdir}
}
