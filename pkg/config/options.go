package config

import (
	"fmt"
	"net"

	"github.com/BurntSushi/toml"
)

// Options for configuration of mesosmock loaded from a config file.
type Options struct {
	IP       string
	Port     int
	Hostname string
	Mesos    *mesosOptions
}

type mesosOptions struct {
	AgentCount int `toml:"agent_count"`
}

func newOptions() *Options {
	return &Options{
		IP:       "127.0.0.1",
		Port:     5050,
		Hostname: "localhost",
		Mesos: &mesosOptions{
			AgentCount: 1,
		},
	}
}

// NewOptions parses command-line arguments and a TOML config file,
// producing a new Options struct.
func NewOptions(configFile string) (*Options, error) {
	o := newOptions()

	// Parse TOML config
	if configFile != "" {
		if _, err := toml.DecodeFile(configFile, &o); err != nil {
			return nil, err
		}
	}

	// Validate options values
	if net.ParseIP(o.IP) == nil {
		return nil, fmt.Errorf("invalid listening IP address specified: %s", o.IP)
	}
	if o.Mesos.AgentCount <= 0 {
		return nil, fmt.Errorf("agent count must be positive")
	}

	return o, nil
}

func (o Options) GetAddress() string {
	return fmt.Sprintf("%s:%d", o.IP, o.Port)
}
