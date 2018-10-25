package main

import (
	"flag"
	"log"
	"os"
)

func main() {
	flagSet := flag.NewFlagSet("mesosmock", flag.ExitOnError)
	config := flagSet.String("config", "", "path to config.json")
	flagSet.String("ip", "127.0.0.1", "IP address to listen on for HTTP requests")
	flagSet.String("port", "5050", "port to listen on for HTTP requests")
	flagSet.String("hostname", "localhost", "hostname for the master")
	flagSet.Int("agentCount", 1, "number of agents to mock in the cluster")

	flagSet.Parse(os.Args[1:])

	opts, err := ConfigOptions(*config, flagSet)
	if err != nil {
		log.Fatalf("ERROR: Could not load config %s: %v", *config, err)
	}

	InitState(opts)

	server := NewServer(opts)

	log.Printf("Starting server on %s...\n", opts.address)
	if err := server.ListenAndServe(); err != nil {
		log.Fatalf("Could not listen on %s: %v\n", opts.address, err)
	}
}
