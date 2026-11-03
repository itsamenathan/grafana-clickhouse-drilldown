// SPDX-License-Identifier: AGPL-3.0-only
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"grafana-clickhouse-drilldown/internal/httpapi"
	"grafana-clickhouse-drilldown/internal/limits"
	"grafana-clickhouse-drilldown/internal/sqlgen"
	"grafana-clickhouse-drilldown/internal/storage"
)

var version = "dev"

func main() {
	if len(os.Args) == 2 && os.Args[1] == "healthcheck" {
		_, port, err := net.SplitHostPort(env("ADAPTER_LISTEN", "127.0.0.1:3100"))
		if err != nil {
			os.Exit(1)
		}
		client := &http.Client{Timeout: 3 * time.Second}
		response, err := client.Get("http://127.0.0.1:" + port + "/ready")
		if err != nil {
			os.Exit(1)
		}
		response.Body.Close()
		if response.StatusCode != http.StatusOK {
			os.Exit(1)
		}
		return
	}
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, nil)))
	if err := run(); err != nil {
		slog.Error("adapter stopped", "error", err.Error())
		os.Exit(1)
	}
}

func run() error {
	config, err := parseConfig(os.Getenv)
	if err != nil {
		return err
	}
	password := os.Getenv("CLICKHOUSE_PASSWORD")
	if path := os.Getenv("CLICKHOUSE_PASSWORD_FILE"); path != "" {
		if password != "" {
			return errors.New("choose only one ClickHouse password source")
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return errors.New("cannot read ClickHouse password file")
		}
		password = strings.TrimSuffix(string(data), "\n")
	}
	if password == "" {
		return errors.New("a restricted ClickHouse credential is required")
	}
	if path := os.Getenv("SOURCE_PATH"); path != "" {
		if stat, err := os.Stat(path); err != nil || !stat.Mode().IsRegular() {
			return errors.New("corresponding source archive is missing")
		}
	}
	db, err := storage.Open(storage.Config{
		Address:  env("CLICKHOUSE_ADDRESS", "127.0.0.1:9000"),
		Database: env("CLICKHOUSE_DATABASE", "default"), Table: env("CLICKHOUSE_TABLE", "otel_logs"),
		User: os.Getenv("CLICKHOUSE_USER"), Password: password,
		ServiceNameAttributes: config.ServiceNameAttributes,
		Limits: storage.Limits{
			MaxScanRows: int(config.MaxScanRows), MaxScanBytes: config.MaxScanBytes,
			QueryTimeout:        config.Limits.RequestTimeout,
			MaxPageDecodedBytes: min(16<<20, config.Limits.MaxRetainedBytes),
		},
	})
	if err != nil {
		return err
	}
	defer db.Close()
	readyCtx, readyCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer readyCancel()
	if err := db.Ready(readyCtx); err != nil {
		return err
	}
	options := httpapi.Options{Limits: config.Limits, SourcePath: os.Getenv("SOURCE_PATH"), SourceURL: os.Getenv("SOURCE_OFFER_URL")}
	for _, origin := range strings.Split(os.Getenv("ADAPTER_ALLOWED_ORIGINS"), ",") {
		if origin = strings.TrimSpace(origin); origin != "" {
			options.AllowedOrigins = append(options.AllowedOrigins, origin)
		}
	}
	server := &http.Server{
		Addr: env("ADAPTER_LISTEN", "127.0.0.1:3100"), Handler: httpapi.New(db, version, options),
		ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second,
		WriteTimeout: config.Limits.RequestTimeout + 5*time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 16384,
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	result := make(chan error, 1)
	go func() { result <- server.ListenAndServe() }()
	select {
	case err := <-result:
		if !errors.Is(err, http.ErrServerClosed) {
			return errors.New("HTTP listener failed")
		}
		return nil
	case <-ctx.Done():
		shutdown, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		return server.Shutdown(shutdown)
	}
}

func env(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

// runtimeConfig contains no credentials and can be validated before connecting.
type runtimeConfig struct {
	ServiceNameAttributes string
	Limits                limits.Config
	MaxScanRows           uint64
	MaxScanBytes          uint64
}

func parseConfig(getenv func(string) string) (runtimeConfig, error) {
	config := runtimeConfig{Limits: (limits.Config{}).WithDefaults(), MaxScanRows: 200000, MaxScanBytes: 256 << 20}
	for _, setting := range []struct {
		name   string
		target *time.Duration
		zero   bool
	}{
		{"ADAPTER_MAX_QUERY_RANGE", &config.Limits.MaxQueryRange, true},
		{"ADAPTER_REQUEST_TIMEOUT", &config.Limits.RequestTimeout, false},
		{"ADAPTER_TAIL_LOOKBACK", &config.Limits.TailLookback, false},
	} {
		if raw := getenv(setting.name); raw != "" {
			value, err := time.ParseDuration(raw)
			if err != nil || value < 0 || value == 0 && !setting.zero {
				return runtimeConfig{}, fmt.Errorf("invalid %s: expected a positive Go duration, or zero for the optional range cap", setting.name)
			}
			*setting.target = value
		}
	}
	for _, setting := range []struct {
		name   string
		target *int
	}{
		{"ADAPTER_PATTERN_LINES", &config.Limits.PatternLines},
		{"ADAPTER_MAX_RETAINED_BYTES", &config.Limits.MaxRetainedBytes},
	} {
		if raw := getenv(setting.name); raw != "" {
			value, err := strconv.Atoi(raw)
			if err != nil || value <= 0 {
				return runtimeConfig{}, fmt.Errorf("invalid %s: expected a positive integer", setting.name)
			}
			*setting.target = value
		}
	}
	for _, setting := range []struct {
		name   string
		target *uint64
	}{
		{"ADAPTER_MAX_SCAN_ROWS", &config.MaxScanRows},
		{"ADAPTER_MAX_SCAN_BYTES", &config.MaxScanBytes},
	} {
		if raw := getenv(setting.name); raw != "" {
			value, err := strconv.ParseUint(raw, 10, 63)
			if err != nil || value == 0 {
				return runtimeConfig{}, fmt.Errorf("invalid %s: expected a positive integer", setting.name)
			}
			*setting.target = value
		}
	}
	config.ServiceNameAttributes = getenv("ADAPTER_SERVICE_NAME_ATTRIBUTES")
	if _, err := sqlgen.ParseServiceMapping(config.ServiceNameAttributes); err != nil {
		return runtimeConfig{}, fmt.Errorf("invalid ADAPTER_SERVICE_NAME_ATTRIBUTES: %w", err)
	}
	if err := config.Limits.Validate(); err != nil {
		return runtimeConfig{}, err
	}
	return config, nil
}
