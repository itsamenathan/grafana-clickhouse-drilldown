// SPDX-License-Identifier: AGPL-3.0-only
package httpapi

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"grafana-clickhouse-drilldown/internal/record"
	"grafana-clickhouse-drilldown/internal/sqlgen"
)

type blockingStore struct{ release <-chan struct{} }

func (blockingStore) Ready(context.Context) error { return nil }
func (s blockingStore) Scan(ctx context.Context, _ sqlgen.Predicate, _, _ int64, _ func(record.Raw) error) error {
	select {
	case <-s.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestHTTPBoundedAdmission(t *testing.T) {
	release := make(chan struct{})
	server := httptest.NewServer(New(blockingStore{release}, "test"))
	defer server.Close()
	client := &http.Client{Timeout: 5 * time.Second}
	results := make(chan int, 35)
	for i := 0; i < 35; i++ {
		go func() {
			response, err := client.Get(server.URL + "/loki/api/v1/query_range?query=" + url.QueryEscape(`{service_name="api"}`))
			if err != nil {
				results <- 0
				return
			}
			io.Copy(io.Discard, response.Body)
			response.Body.Close()
			results <- response.StatusCode
		}()
	}
	first := <-results
	close(release)
	if first != http.StatusTooManyRequests {
		t.Fatalf("full admission queue did not return explicit overload: %d", first)
	}
	for i := 0; i < 34; i++ {
		if code := <-results; code != 200 && code != 429 {
			t.Fatalf("unexpected queued request result: %d", code)
		}
	}
}

func TestResponseSizeFailsBeforeSuccessfulHeaders(t *testing.T) {
	response := httptest.NewRecorder()
	respond(response, 200, map[string]string{"large": strings.Repeat("x", 5<<20)})
	if response.Code != http.StatusUnprocessableEntity || strings.Contains(response.Body.String(), `"status":"success"`) {
		t.Fatal("oversized result became successful partial output")
	}
}

func TestExactTimeAndParameterValidation(t *testing.T) {
	for value, want := range map[string]int64{
		"1767225600000000011":            1767225600000000011,
		"1767225600.000000011":           1767225600000000011,
		"2026-01-01T00:00:00.000000011Z": 1767225600000000011,
	} {
		if got, err := timestamp(value, 0); err != nil || got != want {
			t.Fatalf("timestamp %s lost precision: %d %v", value, got, err)
		}
	}
	for _, raw := range []string{"query=a&query=b", "query=a&interval=1", "query=" + strings.Repeat("a", 32769)} {
		request := httptest.NewRequest("GET", "/?"+raw, nil)
		if err := parameters(request, "query"); err == nil {
			t.Fatal("invalid/repeated/unknown parameter accepted")
		}
	}
	request := httptest.NewRequest("POST", "/", strings.NewReader("query=a"))
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	if err := parameters(request, "query"); err != nil || request.Form.Get("query") != "a" {
		t.Fatal("form-encoded POST was not parsed")
	}
	request = httptest.NewRequest("GET", "/?query+start=ignored", nil)
	if err := parameters(request, "query start"); err == nil {
		t.Fatal("combined parameter name bypassed the allowlist")
	}
	request = httptest.NewRequest("POST", "/", strings.NewReader(`{"query":"ignored"}`))
	request.Header.Set("Content-Type", "application/json")
	if err := parameters(request, "query"); err == nil {
		t.Fatal("unsupported body encoding was silently ignored")
	}
}

func TestCanceledRequestDoesNotReachStorage(t *testing.T) {
	release := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	request := httptest.NewRequest("GET", "/loki/api/v1/query_range?query="+url.QueryEscape(`{service_name="api"}`), nil).WithContext(ctx)
	response := httptest.NewRecorder()
	New(blockingStore{release}, "test").ServeHTTP(response, request)
	var body map[string]any
	json.Unmarshal(response.Body.Bytes(), &body)
	if response.Code != 499 || body["errorType"] != "canceled" {
		t.Fatalf("cancellation was not explicit: %d %v", response.Code, body)
	}
}
