package main

import (
	"flag"
	"fmt"
	"github.com/BurntSushi/toml"
	"github.com/mreiferson/go-options"
	"net"
)

// Options for configuration of mesosmock, passed via
// command-line arguments, or loaded from config file.
type Options struct {
	IP         string `flag:"ip" cfg:"ip"`
	Port       int    `flag:"port" cfg:"port"`
	Hostname   string `flag:"hostname" cfg:"hostname"`
	AgentCount int    `flag:"agentCount" cfg:"agent_count"`

	address string
}

func newOptions() *Options {
	return &Options{
		IP:         "127.0.0.1",
		Port:       5050,
		Hostname:   "localhost",
		AgentCount: 2,
	}
}

// ConfigOptions parses a TOML config file, resolves it with the command-line flags
// and validates/processes the options.
func ConfigOptions(config string, flagSet *flag.FlagSet) (*Options, error) {
	o := newOptions()
	cfg := map[string]interface{}{}

	if config != "" {
		if _, err := toml.DecodeFile(config, &cfg); err != nil {
			return nil, err
		}
	}

	options.Resolve(o, flagSet, cfg)

	if net.ParseIP(o.IP) == nil {
		return nil, fmt.Errorf("invalid listening IP address specified: %s", o.IP)
	}

	if o.AgentCount <= 0 {
		return nil, fmt.Errorf("agent count must be positive")
	}

	o.address = fmt.Sprintf("%s:%d", o.IP, o.Port)

	return o, nil
}
