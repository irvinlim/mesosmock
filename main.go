package main

import (
	"flag"
	"log"

	"github.com/irvinlim/mesosmock/pkg/api"
	"github.com/irvinlim/mesosmock/pkg/config"
	"github.com/irvinlim/mesosmock/pkg/state"
)

func main() {
	configFile := flag.String("config", "", "path to config.json")
	flag.Parse()

	opts, err := config.NewOptions(*configFile)
	if err != nil {
		log.Fatalf("ERROR: Could not create config: %s", err)
	}

	s, err := state.NewMasterState(opts)
	if err != nil {
		log.Fatalf("ERROR: %s", err)
	}

	server := api.NewServer(opts, s)
	if err := server.ListenAndServe(); err != nil {
		log.Fatalf("Could not start server: %s", err)
	}
}
