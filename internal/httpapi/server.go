// SPDX-License-Identifier: AGPL-3.0-only
package httpapi

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"grafana-clickhouse-drilldown/internal/engine"
	"grafana-clickhouse-drilldown/internal/limits"
)

type Readiness interface {
	Ready(context.Context) error
}

type apiError struct {
	Status    string `json:"status"`
	ErrorType string `json:"errorType"`
	Error     string `json:"error"`
}

type Options struct {
	SourcePath string
	SourceURL  string
}

func New(db Readiness, version string, options ...Options) http.Handler {
	var option Options
	if len(options) != 0 {
		option = options[0]
	}
	sourceHash := ""
	if option.SourcePath != "" {
		if file, err := os.Open(option.SourcePath); err == nil {
			hash := sha256.New()
			if _, err := io.Copy(hash, file); err == nil {
				sourceHash = fmt.Sprintf("%x", hash.Sum(nil))
			}
			file.Close()
		}
	}
	mux := http.NewServeMux()
	if sourceHash != "" {
		mux.HandleFunc("GET /source", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/gzip")
			w.Header().Set("Content-Disposition", `attachment; filename="grafana-clickhouse-drilldown-source.tar.gz"`)
			http.ServeFile(w, r, option.SourcePath)
		})
	}
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		respond(w, http.StatusOK, map[string]string{"status": "alive"})
	})
	mux.HandleFunc("GET /ready", func(w http.ResponseWriter, r *http.Request) {
		if err := db.Ready(r.Context()); err != nil {
			respond(w, http.StatusServiceUnavailable, apiError{"error", "unavailable", "database is not ready"})
			return
		}
		respond(w, http.StatusOK, map[string]string{"status": "ready"})
	})
	mux.HandleFunc("GET /loki/api/v1/status/buildinfo", func(w http.ResponseWriter, r *http.Request) {
		respond(w, http.StatusOK, map[string]string{"application": "grafana-clickhouse-drilldown", "version": version, "license": "AGPL-3.0-only", "sourceURL": option.SourceURL, "sourceSHA256": sourceHash})
	})
	if store, ok := db.(engine.Store); ok {
		api := &queryAPI{engine: engine.New(store), db: db}
		gate, _ := limits.New(2, 32)
		mux.HandleFunc("/loki/api/v1/", func(w http.ResponseWriter, r *http.Request) {
			release, err := gate.Acquire(r.Context())
			if err != nil {
				fail(w, err)
				return
			}
			defer release()
			api.route(w, r)
		})
	}
	// Unsupported endpoints must never return successful placeholder results.
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		respond(w, http.StatusNotImplemented, apiError{"error", "unsupported", "endpoint is not implemented"})
	})
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if sourceHash != "" {
			url := option.SourceURL
			if url == "" {
				url = "/source"
			}
			w.Header().Set("Link", "<"+url+">; rel=\"source\"")
		}
		if len(r.Header.Values("X-Scope-OrgID")) != 0 {
			respond(w, http.StatusBadRequest, apiError{"error", "bad_data", "tenant routing is not supported"})
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
		defer cancel()
		r.Body = http.MaxBytesReader(w, r.Body, 16384)
		mux.ServeHTTP(w, r.WithContext(ctx))
	})
}

func respond(w http.ResponseWriter, status int, value any) {
	encoded, err := json.Marshal(value)
	if err != nil || len(encoded) > 4<<20 {
		status = http.StatusUnprocessableEntity
		encoded, _ = json.Marshal(apiError{"error", "limit", "response exceeds the bounded byte budget"})
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_, _ = w.Write(append(encoded, '\n'))
}
