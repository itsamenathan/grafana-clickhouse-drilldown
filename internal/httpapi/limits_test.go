// SPDX-License-Identifier: AGPL-3.0-only
package httpapi

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"testing"
	"time"

	"grafana-clickhouse-drilldown/internal/engine"
	"grafana-clickhouse-drilldown/internal/limits"
	"grafana-clickhouse-drilldown/internal/record"
	"grafana-clickhouse-drilldown/internal/sqlgen"
)

type blockingStore struct{ release <-chan struct{} }

func (blockingStore) Ready(context.Context) error { return nil }
func (s blockingStore) Scan(ctx context.Context, _ sqlgen.Predicate, _, _ int64, _ record.ScanOptions, _ func(record.Raw) error) error {
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

func TestLargeResponsesAreNotCapped(t *testing.T) {
	response := httptest.NewRecorder()
	respond(response, 200, map[string]string{"large": strings.Repeat("x", 5<<20)})
	if response.Code != http.StatusOK || response.Body.Len() < 5<<20 {
		t.Fatalf("bounded payload was rejected: %d", response.Code)
	}
}

func TestStreamedLogPageMatchesMarshaledResponse(t *testing.T) {
	// A 2,000-line page with ~2.4 KB of metadata per line exceeds the old
	// 4 MiB cap; streaming must produce exactly the marshaled document.
	metadata := map[string]string{}
	for i := 0; i < 33; i++ {
		metadata[fmt.Sprintf("log_attribute_%d", i)] = strings.Repeat("v", 60)
	}
	for _, categorize := range []bool{false, true} {
		var streams []engine.Stream
		for i := 0; i < 2000; i++ {
			value := []any{fmt.Sprint(1767225600000000000 + i), "line \"quoted\" <html> 雪 " + fmt.Sprint(i)}
			if categorize {
				value = append(value, map[string]any{"structuredMetadata": metadata})
			}
			labels := map[string]string{"service_name": "api"}
			if !categorize {
				labels = map[string]string{"service_name": "api", "n": fmt.Sprint(i)}
			}
			streams = append(streams, engine.Stream{Labels: labels, Values: [][]any{value}})
		}
		want := success("streams", streams)
		if categorize {
			want["data"].(map[string]any)["encodingFlags"] = []string{"categorize-labels"}
		}
		recorder := httptest.NewRecorder()
		respondStreams(recorder, streams, categorize)
		var got, expected any
		if err := json.Unmarshal(recorder.Body.Bytes(), &got); err != nil {
			t.Fatalf("streamed response is not valid JSON: %v", err)
		}
		encoded, _ := json.Marshal(want)
		json.Unmarshal(encoded, &expected)
		if recorder.Code != 200 || !reflect.DeepEqual(got, expected) || (categorize && recorder.Body.Len() < 4<<20) {
			t.Fatalf("categorize=%v: streamed page differs from marshaled response (%d bytes)", categorize, recorder.Body.Len())
		}
	}
	empty := httptest.NewRecorder()
	respondStreams(empty, []engine.Stream{}, false)
	if strings.TrimSpace(empty.Body.String()) != `{"status":"success","data":{"resultType":"streams","result":[]}}` {
		t.Fatalf("empty page: %s", empty.Body.String())
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

func TestMetricRangeAllowsGrafanaStepAlignment(t *testing.T) {
	server := httptest.NewServer(New(&memoryStore{}, "test", Options{Limits: limits.Config{MaxQueryRange: time.Hour}}))
	defer server.Close()
	end := time.Date(2026, 1, 1, 1, 0, 10, 0, time.UTC).UnixNano()
	for span, want := range map[time.Duration]int{time.Hour + 10*time.Second: 200, time.Hour + 20*time.Second: 400} {
		params := url.Values{"query": {`sum by (detected_level) (count_over_time({service_name="api"}[10s]))`}, "step": {"10"},
			"start": {fmt.Sprint(end - int64(span))}, "end": {fmt.Sprint(end)}}
		response, err := http.Get(server.URL + "/loki/api/v1/query_range?" + params.Encode())
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		if response.StatusCode != want {
			t.Fatalf("metric range %v: status %d, want %d", span, response.StatusCode, want)
		}
	}
}

func TestWideRangesAcceptedByDefault(t *testing.T) {
	end := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC).UnixNano()
	for _, endpoint := range []string{"query_range", "labels", "label/service_name/values", "series", "detected_labels", "detected_fields", "detected_field/detected_level/values", "index/volume", "index/volume_range", "index/stats", "patterns"} {
		params := url.Values{"query": {`{service_name="api"}`}, "start": {fmt.Sprint(end - int64(30*24*time.Hour))}, "end": {fmt.Sprint(end)}}
		if endpoint == "index/volume_range" || endpoint == "patterns" {
			params.Set("step", "1h")
		}
		response := httptest.NewRecorder()
		New(&memoryStore{}, "test").ServeHTTP(response, httptest.NewRequest("GET", "/loki/api/v1/"+endpoint+"?"+params.Encode(), nil))
		if response.Code != 200 {
			t.Fatalf("wide %s rejected: %d %s", endpoint, response.Code, response.Body.String())
		}
	}
	params := url.Values{"query": {`sum(count_over_time({service_name="api"}[10s]))`}, "start": {fmt.Sprint(end - int64(30*24*time.Hour))}, "end": {fmt.Sprint(end)}, "step": {"1h"}}
	response := httptest.NewRecorder()
	New(&memoryStore{}, "test").ServeHTTP(response, httptest.NewRequest("GET", "/loki/api/v1/query_range?"+params.Encode(), nil))
	if response.Code != 200 {
		t.Fatalf("wide metric range rejected: %d %s", response.Code, response.Body.String())
	}
}

func TestConfiguredRangeAndOutputBounds(t *testing.T) {
	end := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC).UnixNano()
	for _, tc := range []struct {
		endpoint, query, step string
		span                  time.Duration
		cap                   time.Duration
	}{
		{"query_range", `{service_name="api"}`, "", 3 * time.Hour, time.Hour},
		{"labels", `{service_name="api"}`, "", 3 * time.Hour, time.Hour},
		{"patterns", `{service_name="api"}`, "30s", 3 * time.Hour, time.Hour},
		{"query_range", `sum(count_over_time({service_name="api"}[10s]))`, "1s", time.Hour, 0},
		{"query_range", `sum(count_over_time({service_name="api"}[10s]))`, "1.000000001s", time.Minute, 0},
		{"query_range", `sum(count_over_time({service_name="api"}[30m]))`, "1m", time.Minute, 10 * time.Minute},
	} {
		params := url.Values{"query": {tc.query}, "start": {fmt.Sprint(end - int64(tc.span))}, "end": {fmt.Sprint(end)}}
		if tc.step != "" {
			params.Set("step", tc.step)
		}
		response := httptest.NewRecorder()
		New(&memoryStore{}, "test", Options{Limits: limits.Config{MaxQueryRange: tc.cap}}).ServeHTTP(response, httptest.NewRequest("GET", "/loki/api/v1/"+tc.endpoint+"?"+params.Encode(), nil))
		if response.Code < 400 || !strings.Contains(response.Body.String(), `"status":"error"`) {
			t.Fatalf("bound succeeded: %+v %d %s", tc, response.Code, response.Body.String())
		}
	}
}

func TestBoundsKeepValidityAndZeroWidth(t *testing.T) {
	for _, tc := range []struct {
		start, end string
		valid      bool
	}{
		{"-1", "1000000", false}, {"2000000", "1000000", false}, {"1000000", "1000000", true},
		{"", "0", true}, {"0", "9223372036854775807", true},
	} {
		request := httptest.NewRequest("GET", "/?"+url.Values{"start": {tc.start}, "end": {tc.end}}.Encode(), nil)
		if err := request.ParseForm(); err != nil {
			t.Fatal(err)
		}
		_, _, err := boundsLimit(request, 0)
		if (err == nil) != tc.valid {
			t.Fatalf("bounds %+v: %v", tc, err)
		}
	}
}

func TestDrilldownLimitsMatchConfiguration(t *testing.T) {
	for _, config := range []limits.Config{{}, {MaxQueryRange: 721 * time.Hour, RequestTimeout: 30 * time.Second}} {
		response := httptest.NewRecorder()
		New(&memoryStore{}, "test", Options{Limits: config}).ServeHTTP(response, httptest.NewRequest("GET", "/loki/api/v1/drilldown-limits", nil))
		var body struct {
			Limits map[string]any `json:"limits"`
		}
		if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
			t.Fatal(err)
		}
		if response.Code != 200 || body.Limits["max_query_length"] != config.MaxQueryRange.String() || body.Limits["max_query_range"] != config.MaxQueryRange.String() || body.Limits["query_timeout"] != config.WithDefaults().RequestTimeout.String() {
			t.Fatalf("misreported limits: %d %v", response.Code, body)
		}
	}
}

func TestConfiguredRequestDeadline(t *testing.T) {
	response := httptest.NewRecorder()
	New(blockingStore{make(chan struct{})}, "test", Options{Limits: limits.Config{RequestTimeout: 10 * time.Millisecond}}).ServeHTTP(response, httptest.NewRequest("GET", "/loki/api/v1/query_range?query="+url.QueryEscape(`{service_name="api"}`), nil))
	if response.Code != 504 || !strings.Contains(response.Body.String(), `"errorType":"timeout"`) {
		t.Fatalf("deadline not enforced: %d %s", response.Code, response.Body.String())
	}
}

func TestConfiguredRetainedBudgetReachesEngine(t *testing.T) {
	store := &memoryStore{}
	store.add(row(1, strings.Repeat("x", 1000)))
	response := httptest.NewRecorder()
	params := url.Values{"query": {`{service_name="api"}`}, "start": {"0"}, "end": {"100"}}
	New(store, "test", Options{Limits: limits.Config{MaxRetainedBytes: 100}}).ServeHTTP(response, httptest.NewRequest("GET", "/loki/api/v1/query_range?"+params.Encode(), nil))
	if response.Code < 400 || !strings.Contains(response.Body.String(), "memory budget") {
		t.Fatalf("engine ignored configured retained budget: %d %s", response.Code, response.Body.String())
	}
}

type summaryMemoryStore struct{ *memoryStore }

func (*summaryMemoryStore) Summarize(context.Context, sqlgen.Predicate, int64, int64) ([]record.ServiceSummary, error) {
	return []record.ServiceSummary{{Service: "api", Entries: 1, Bytes: 10}}, nil
}

func TestDiscoveryAndVolumeRetainedBudgets(t *testing.T) {
	store := &memoryStore{}
	store.add(row(1, "small body"))
	for _, db := range []Readiness{store, &summaryMemoryStore{store}} {
		for _, endpoint := range []string{"labels", "series", "index/volume", "index/volume_range", "index/stats"} {
			response := httptest.NewRecorder()
			params := url.Values{"query": {`{service_name="api"}`}, "start": {"0"}, "end": {"100"}}
			New(db, "test", Options{Limits: limits.Config{MaxRetainedBytes: 100}}).ServeHTTP(response, httptest.NewRequest("GET", "/loki/api/v1/"+endpoint+"?"+params.Encode(), nil))
			if response.Code < 400 || !strings.Contains(response.Body.String(), "memory budget") {
				t.Fatalf("%s ignored retained budget: %d %s", endpoint, response.Code, response.Body.String())
			}
		}
	}
}
