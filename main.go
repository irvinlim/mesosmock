package main

import (
	"flag"

	"github.com/irvinlim/mesosmock/pkg/api"
	"github.com/irvinlim/mesosmock/pkg/config"
	"github.com/irvinlim/mesosmock/pkg/state"
	log "github.com/sirupsen/logrus"
)

func main() {
	log.SetFormatter(&log.TextFormatter{ForceColors: true})

	configFile := flag.String("config", "", "path to config.json")
	logLevel := flag.String("logLevel", "debug", "log level: panic,fatal,error,warn,info,debug,trace")
	flag.Parse()

	opts, err := config.NewOptions(*configFile)
	if err != nil {
		log.Fatalf("ERROR: Could not create config: %s", err)
	}

	level, err := log.ParseLevel(*logLevel)
	if err != nil {
		log.Fatalf("ERROR: %s", err)
	}
	log.SetLevel(level)

	s, err := state.NewMasterState(opts)
	if err != nil {
		log.Fatalf("ERROR: %s", err)
	}

	server := api.NewServer(opts, s)
	if err := server.ListenAndServe(); err != nil {
		log.Fatalf("Could not start server: %s", err)
	}
}
