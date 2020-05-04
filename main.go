package main

import (
	"flag"
	"fmt"
	"os"
)

// examples confluent schema registry usage
// https://docs.confluent.io/current/schema-registry/using.html

const (
	DownloadCmd = "download"
	RegisterCmd = "register"
)

type Cmd interface {
	Do()
	Parse()
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("subcommand is required")
		os.Exit(1)
	}

	var cmd Cmd

	switch os.Args[1] {
	case DownloadCmd:
		cmd = NewDownloadCommand()
		cmd.Parse()
	case RegisterCmd:
		cmd = NewRegisterCommand()
		cmd.Parse()
	default:
		flag.PrintDefaults()
		os.Exit(1)
	}

	cmd.Do()
}
