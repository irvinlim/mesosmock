package main

import (
	"flag"
	"fmt"
	"strings"

	"github.com/BurntSushi/toml"
	"github.com/mreiferson/go-options"
)

// Options for configuration of mesosmock, passed via
// command-line arguments, or loaded from config file.
type Options struct {
	ListenAddr string `flag:"listenAddr" cfg:"listen_addr"`
}

func newOptions() *Options {
	return &Options{}
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

	if o.ListenAddr == "" {
		return nil, fmt.Errorf("invalid listen address specified: %#v", o.ListenAddr)
	}

	if !strings.Contains(o.ListenAddr, ":") {
		o.ListenAddr = ":" + o.ListenAddr
	}

	return o, nil
}
