package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"sort"
	"sync"
	"syscall"
	"time"
)

func main() {
	cfgPath := flag.String("config", "config.yaml", "path to the YAML config file")
	output := flag.String("output", "", "override output: text, json (stdout) or http")
	once := flag.Bool("once", false, "poll every group a single time and exit")
	debug := flag.Bool("debug", false, "enable debug logging")
	flag.Parse()

	lvl := slog.LevelInfo
	if *debug {
		lvl = slog.LevelDebug
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: lvl}))

	cfg, err := LoadConfig(*cfgPath)
	if err != nil {
		logger.Error("config error", "err", err)
		os.Exit(1)
	}
	switch *output {
	case "":
	case "text", "json":
		cfg.Output.Type, cfg.Output.Format = "stdout", *output
	case "http":
		cfg.Output.Type = "http"
		if cfg.Output.URL == "" {
			logger.Error("-output http requires output.url in the config")
			os.Exit(2)
		}
	default:
		logger.Error("invalid -output, use text, json or http", "value", *output)
		os.Exit(2)
	}

	oids, err := LoadOIDs(cfg.OIDsFile)
	if err != nil {
		logger.Error("oids error", "err", err)
		os.Exit(1)
	}

	// Build the pollers up front so config mistakes fail before polling starts.
	var pollers []*Poller
	samples := make(chan Sample, 4096)
	for _, dev := range cfg.Devices {
		groups := make([]string, 0, len(dev.Groups))
		for g := range dev.Groups {
			groups = append(groups, g)
		}
		sort.Strings(groups)
		for _, g := range groups {
			iv := dev.Groups[g]
			if iv <= 0 {
				continue
			}
			objs, err := oids.Resolve(g, dev.Vendor)
			if err != nil {
				logger.Error("config error", "device", dev.Name, "group", g, "err", err)
				os.Exit(1)
			}
			pollers = append(pollers, &Poller{
				dev:      dev,
				group:    g,
				interval: iv,
				objects:  objs,
				out:      samples,
				log:      logger.With("device", dev.Name, "host", dev.Host, "group", g),
			})
			logger.Info("scheduled", "device", dev.Name, "host", dev.Host, "vendor", dev.Vendor,
				"group", g, "interval", iv, "objects", len(objs))
		}
	}
	if len(pollers) == 0 {
		logger.Error("nothing to poll: every group has a zero interval")
		os.Exit(1)
	}

	var sink Sink
	flushEvery := time.Second
	switch cfg.Output.Type {
	case "http":
		sink = newHTTPSink(cfg.Output, logger)
		flushEvery = cfg.Output.FlushInterval
		logger.Info("output", "type", "http", "url", cfg.Output.URL,
			"batch_size", cfg.Output.BatchSize, "flush_interval", flushEvery, "gzip", cfg.Output.Compress())
	default:
		sink = newStdoutSink(cfg.Output.Format)
		logger.Info("output", "type", "stdout", "format", cfg.Output.Format)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var writerDone sync.WaitGroup
	writerDone.Add(1)
	go func() {
		defer writerDone.Done()
		runWriter(samples, sink, flushEvery)
	}()

	var wg sync.WaitGroup
	for _, p := range pollers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if *once {
				p.pollOnce(ctx)
			} else {
				p.Run(ctx)
			}
		}()
	}
	wg.Wait()
	close(samples)
	writerDone.Wait()
	if ctx.Err() != nil {
		logger.Info("shutting down")
	}
}
