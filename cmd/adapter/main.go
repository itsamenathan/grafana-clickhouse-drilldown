// SPDX-License-Identifier: AGPL-3.0-only
package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"grafana-clickhouse-drilldown/internal/httpapi"
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
	})
	if err != nil {
		return err
	}
	defer db.Close()
	server := &http.Server{
		Addr: env("ADAPTER_LISTEN", "127.0.0.1:3100"), Handler: httpapi.New(db, version, httpapi.Options{SourcePath: os.Getenv("SOURCE_PATH"), SourceURL: os.Getenv("SOURCE_OFFER_URL")}),
		ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second,
		WriteTimeout: 20 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 16384,
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
