package main

import (
	"flag"
	"log"
	"os"

	"github.com/irvinlim/mesosmock/pkg/config"
	"github.com/irvinlim/mesosmock/pkg/state"
)

func main() {
	flagSet := flag.NewFlagSet("mesosmock", flag.ExitOnError)
	configFile := flagSet.String("config", "", "path to config.json")
	flagSet.String("ip", "127.0.0.1", "IP address to listen on for HTTP requests")
	flagSet.String("port", "5050", "port to listen on for HTTP requests")
	flagSet.String("hostname", "localhost", "hostname for the master")
	flagSet.Int("agentCount", 1, "number of agents to mock in the cluster")

	flagSet.Parse(os.Args[1:])

	opts, err := config.ConfigOptions(*configFile, flagSet)
	if err != nil {
		log.Fatalf("ERROR: Could not load config %s: %v", *configFile, err)
	}

	s, err := state.NewMasterState(opts)
	if err != nil {
		log.Fatalf("ERROR: %v", err)
	}

	server := NewServer(opts, s)

	log.Printf("Starting server on %s...\n", opts.GetAddress())
	if err := server.ListenAndServe(); err != nil {
		log.Fatalf("Could not listen on %s: %v\n", opts.GetAddress(), err)
	}
}
