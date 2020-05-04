package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
)

// register schema url example
// curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
//  --data '{"schema": "{\"type\": \"string\"}"}' \
//  http://localhost:8081/subjects/Kafka-key/versions

type reqBody struct {
	Schema string `json:"schema"`
}

type RegisterCommand struct {
	httpClient *http.Client
	cmd        *flag.FlagSet
	host       *string
	subject    *string
	version    *string
	file       *string
}

func (rc *RegisterCommand) uploadFile(data []byte) ([]byte, error) {
	r, err := http.NewRequest(
		http.MethodPost,
		fmt.Sprintf("%s/subjects/%s/versions", *rc.host, *rc.subject),
		bytes.NewBuffer(data),
	)
	if err != nil {
		return nil, errors.New("failed to create new request")
	}
	r.Header.Set("Content-Type", "application/vnd.schemaregistry.v1+json")
	resp, err := rc.httpClient.Do(r)
	if err != nil {
		return nil, errors.New("failed to send a request")
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode > 300 {
		return nil, errors.New(fmt.Sprintf("got response with status: %s, statusCode: %d\n", resp.Status, resp.StatusCode))
	}

	data, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.New("failed to read body")
	}
	return data, nil
}

func (rc *RegisterCommand) Do() {
	file, err := os.Open(*rc.file)
	if err != nil {
		fmt.Printf("failed to open a file")
		os.Exit(1)
	}
	defer file.Close()

	data, err := ioutil.ReadAll(file)
	if err != nil {
		fmt.Println("failed to read file content")
		os.Exit(1)
	}

	var rBody = reqBody{Schema: string(data)}
	body, err := json.Marshal(rBody)
	if err != nil {
		fmt.Println("failed to marshal request body")
		os.Exit(1)
	}

	data, err = rc.uploadFile(body)
	if err != nil {
		fmt.Printf(
			"failed to upload the file for subject: %s, version: %s. Error: %s\n",
			*rc.subject, *rc.version, err.Error(),
		)
		os.Exit(1)
	}
	fmt.Printf("API response: %s\n", string(data))
}

func (rc *RegisterCommand) Parse() {
	rc.cmd.Parse(os.Args[2:])
	if !rc.cmd.Parsed() {
		fmt.Println("failed to parse command")
		os.Exit(1)
	}
	if *rc.host == "" {
		fmt.Println("host is required")
		os.Exit(1)
	}
	if *rc.file == "" {
		fmt.Println("file is required")
		os.Exit(1)
	}
	if *rc.subject == "" {
		fmt.Println("subject is required")
		os.Exit(1)
	}
}

func NewRegisterCommand() *RegisterCommand {
	registerCommand := flag.NewFlagSet("register", flag.ExitOnError)

	host := registerCommand.String("host", "", "Schema registry host")
	subject := registerCommand.String("subject", "", "Schema subject")
	file := registerCommand.String("file", "", "Path to schema file")
	version := registerCommand.String("version", "1", "Schema version")

	client := &http.Client{Transport: http.DefaultTransport}

	return &RegisterCommand{
		httpClient: client,
		cmd:        registerCommand,
		host:       host,
		subject:    subject,
		version:    version,
		file:       file,
	}
}
