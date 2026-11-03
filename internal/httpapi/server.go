// SPDX-License-Identifier: AGPL-3.0-only
package httpapi

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"

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
	Limits     limits.Config
	SourcePath string
	SourceURL  string
	// AllowedOrigins lists browser origins, such as Grafana's public URL,
	// that may open tail websockets through Grafana's data-source proxy.
	// Requests without an Origin header (Grafana's backend) are always allowed.
	AllowedOrigins []string
}

func New(db Readiness, version string, options ...Options) http.Handler {
	var option Options
	if len(options) != 0 {
		option = options[0]
	}
	option.Limits = option.Limits.WithDefaults()
	if err := option.Limits.Validate(); err != nil {
		panic(err) // Programming error; environment configuration is validated before New.
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
		api := &queryAPI{engine: engine.NewWithLimits(store, option.Limits), db: db, tails: make(chan struct{}, maxTails), origins: option.AllowedOrigins, limits: option.Limits}
		gate, _ := limits.New(2, 32)
		// A tail owns a long-lived websocket. It takes the storage gate per
		// poll instead of holding an HTTP admission slot for its lifetime.
		mux.HandleFunc(tailPath, api.tail)
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
		r.Body = http.MaxBytesReader(w, r.Body, 16384)
		if r.URL.Path == tailPath {
			// Each tail poll has its own query deadline; the stream has a
			// separate maximum duration.
			mux.ServeHTTP(w, r)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), option.Limits.RequestTimeout)
		defer cancel()
		mux.ServeHTTP(w, r.WithContext(ctx))
	})
}

// respond encodes a whole response before sending headers. Every payload is
// bounded before encoding (log limits and the retained budget, metric samples
// and series, value limits), so there is no separate response-size cap; log
// pages, the largest responses, are streamed by respondStreams instead.
func respond(w http.ResponseWriter, status int, value any) {
	encoded, err := json.Marshal(value)
	if err != nil {
		status = http.StatusInternalServerError
		encoded, _ = json.Marshal(apiError{"error", "internal", "response encoding failed"})
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_, _ = w.Write(append(encoded, '\n'))
}

// respondStreams writes a log page value by value, so a page never exists
// twice in memory: once as events and again as one encoded buffer. Values
// are plain strings and label maps and cannot fail to encode; if one ever
// does after headers are sent, the connection is aborted so the client sees
// an error rather than a truncated but valid-looking response.
func respondStreams(w http.ResponseWriter, streams []engine.Stream, categorize bool) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
	out := bufio.NewWriterSize(w, 64<<10)
	write := func(value any) {
		encoded, err := json.Marshal(value)
		if err != nil {
			panic(http.ErrAbortHandler)
		}
		_, _ = out.Write(encoded)
	}
	_, _ = out.WriteString(`{"status":"success","data":{"resultType":"streams",`)
	if categorize {
		_, _ = out.WriteString(`"encodingFlags":["categorize-labels"],`)
	}
	_, _ = out.WriteString(`"result":[`)
	for i, stream := range streams {
		if i > 0 {
			_ = out.WriteByte(',')
		}
		_, _ = out.WriteString(`{"stream":`)
		write(stream.Labels)
		_, _ = out.WriteString(`,"values":[`)
		for j, value := range stream.Values {
			if j > 0 {
				_ = out.WriteByte(',')
			}
			write(value)
		}
		_, _ = out.WriteString(`]}`)
	}
	_, _ = out.WriteString("]}}\n")
	_ = out.Flush()
}
