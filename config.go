package main

import (
	"fmt"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Config is the top-level user configuration (config.yaml).
type Config struct {
	// OIDsFile points at the JSON file holding the OID groups (snmp.json).
	OIDsFile string `yaml:"oids_file"`
	// Output selects where samples go: stdout (dev) or an HTTP ingest server.
	Output OutputConfig `yaml:"output"`
	// Defaults apply to every device unless the device overrides them.
	Defaults DeviceDefaults `yaml:"defaults"`
	Devices  []Device       `yaml:"devices"`
}

// OutputConfig configures the sample sink.
type OutputConfig struct {
	Type   string `yaml:"type"`   // stdout | http
	Format string `yaml:"format"` // stdout only: text | json

	// http only
	URL           string        `yaml:"url"`
	Token         string        `yaml:"token"`
	BatchSize     int           `yaml:"batch_size"`
	FlushInterval time.Duration `yaml:"flush_interval"`
	Timeout       time.Duration `yaml:"timeout"`
	Gzip          *bool         `yaml:"gzip"` // default true
}

// Compress reports whether HTTP bodies are gzip-compressed (default true).
func (o OutputConfig) Compress() bool { return o.Gzip == nil || *o.Gzip }

// DeviceDefaults holds SNMP settings and group intervals shared by all devices.
type DeviceDefaults struct {
	Port      int                      `yaml:"port"`
	Community string                   `yaml:"community"`
	Version   string                   `yaml:"version"`
	Timeout   time.Duration            `yaml:"timeout"`
	Retries   int                      `yaml:"retries"`
	Groups    map[string]time.Duration `yaml:"groups"`
}

// Device is one SNMP target. Any zero field falls back to Defaults.
type Device struct {
	Name      string        `yaml:"name"`
	Host      string        `yaml:"host"`
	Port      int           `yaml:"port"`
	Community string        `yaml:"community"`
	Version   string        `yaml:"version"`
	Vendor    string        `yaml:"vendor"`
	Timeout   time.Duration `yaml:"timeout"`
	Retries   int           `yaml:"retries"`
	// Groups maps a group name from snmp.json (counters, system, ...) to its
	// polling interval. A zero interval disables the group for this device.
	Groups map[string]time.Duration `yaml:"groups"`
}

// LoadConfig reads, merges defaults into each device, and validates the config.
func LoadConfig(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	// ${VAR} references are expanded from the environment so secrets such as
	// the ingest token or the community string can stay out of the file.
	raw = []byte(os.ExpandEnv(string(raw)))
	var cfg Config
	if err := yaml.Unmarshal(raw, &cfg); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}

	if cfg.OIDsFile == "" {
		cfg.OIDsFile = "snmp.json"
	}
	if err := cfg.Output.applyDefaults(); err != nil {
		return nil, fmt.Errorf("%s: output: %w", path, err)
	}

	d := &cfg.Defaults
	if d.Port == 0 {
		d.Port = 161
	}
	if d.Community == "" {
		d.Community = "public"
	}
	if d.Version == "" {
		d.Version = "2c"
	}
	if d.Timeout == 0 {
		d.Timeout = 5 * time.Second
	}
	if d.Retries == 0 {
		d.Retries = 1
	}

	if len(cfg.Devices) == 0 {
		return nil, fmt.Errorf("%s: no devices defined", path)
	}
	for i := range cfg.Devices {
		dev := &cfg.Devices[i]
		if dev.Host == "" {
			return nil, fmt.Errorf("device #%d: host is required", i+1)
		}
		if dev.Name == "" {
			dev.Name = dev.Host
		}
		if dev.Port == 0 {
			dev.Port = d.Port
		}
		if dev.Community == "" {
			dev.Community = d.Community
		}
		if dev.Version == "" {
			dev.Version = d.Version
		}
		if dev.Timeout == 0 {
			dev.Timeout = d.Timeout
		}
		if dev.Retries == 0 {
			dev.Retries = d.Retries
		}
		switch strings.ToLower(dev.Version) {
		case "1", "2c", "2":
		default:
			return nil, fmt.Errorf("device %s: unsupported snmp version %q (use 1 or 2c)", dev.Name, dev.Version)
		}

		// Device groups override defaults; groups only in defaults are inherited.
		merged := make(map[string]time.Duration, len(d.Groups)+len(dev.Groups))
		for g, iv := range d.Groups {
			merged[g] = iv
		}
		for g, iv := range dev.Groups {
			merged[g] = iv
		}
		dev.Groups = merged
		if len(dev.Groups) == 0 {
			return nil, fmt.Errorf("device %s: no groups configured", dev.Name)
		}
	}
	return &cfg, nil
}

func (o *OutputConfig) applyDefaults() error {
	if o.Type == "" {
		o.Type = "stdout"
	}
	if o.Format == "" {
		o.Format = "text"
	}
	if o.BatchSize <= 0 {
		o.BatchSize = 1000
	}
	if o.FlushInterval <= 0 {
		o.FlushInterval = 5 * time.Second
	}
	if o.Timeout <= 0 {
		o.Timeout = 10 * time.Second
	}
	switch o.Type {
	case "stdout":
		if o.Format != "text" && o.Format != "json" {
			return fmt.Errorf("format must be text or json, got %q", o.Format)
		}
	case "http":
		if o.URL == "" {
			return fmt.Errorf("url is required when type is http")
		}
	default:
		return fmt.Errorf("type must be stdout or http, got %q", o.Type)
	}
	return nil
}
