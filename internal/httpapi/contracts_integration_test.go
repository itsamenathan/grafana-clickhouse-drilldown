//go:build integration

// SPDX-License-Identifier: AGPL-3.0-only
package httpapi

import (
	"encoding/json"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"grafana-clickhouse-drilldown/internal/limits"
	"grafana-clickhouse-drilldown/internal/storage"
)

func TestHTTPContractsAgainstClickHouse(t *testing.T) {
	_, server := fixtureServer(t)
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano()
	params := url.Values{"start": {strconv.FormatInt(base-1, 10)}, "end": {strconv.FormatInt(base+100, 10)}, "query": {`{service_name="fixture-api"}`}}
	request := func(path string, extra url.Values) map[string]any {
		t.Helper()
		p := maps.Clone(params)
		for key, values := range extra {
			p[key] = values
		}
		status, result := getJSON(t, server.URL+"/loki/api/v1/"+path, p, true)
		if status != http.StatusOK {
			t.Fatalf("%s: %d %v", path, status, result)
		}
		return result
	}
	logs := canonical(t, request("query_range", url.Values{"query": {`{service_name="fixture-api"} |= "duplicate"`}}))
	if len(logs) != 2 || logs[0] != logs[1] {
		t.Fatal("stored duplicate events were collapsed")
	}
	// Resume at a timestamp boundary outside an equal-time cohort. This
	// explicitly does not claim a cursor can resume within identical events.
	whole := canonical(t, request("query_range", nil))
	before := canonical(t, request("query_range", url.Values{"end": {strconv.FormatInt(base+7, 10)}, "direction": {"forward"}}))
	after := canonical(t, request("query_range", url.Values{"start": {strconv.FormatInt(base+7, 10)}, "direction": {"backward"}}))
	joined := append(before, after...)
	sort.Strings(joined)
	if !reflect.DeepEqual(joined, whole) {
		t.Fatal("timestamp-boundary pagination lost or duplicated events")
	}
	for _, direction := range []string{"forward", "backward"} {
		limited := request("query_range", url.Values{"limit": {"1"}, "direction": {direction}})
		streams := limited["data"].(map[string]any)["result"].([]any)
		if len(streams) != 1 || len(streams[0].(map[string]any)["values"].([]any)) != 1 {
			t.Fatal("pagination limit was not global")
		}
		got := streams[0].(map[string]any)["values"].([]any)[0].([]any)[0]
		want := base - 1
		if direction == "backward" {
			want = base + 11
		}
		if got != strconv.FormatInt(want, 10) {
			t.Fatalf("wrong %s page boundary: %v", direction, got)
		}
	}
	if got := canonical(t, request("query_range", url.Values{"query": {"{service_name=`__impossible_service__`}"}})); len(got) != 0 {
		t.Fatal("impossible backtick selector leaked another service")
	}
	labels := request("detected_labels", url.Values{"query": {""}})["detectedLabels"].([]any)
	if len(labels) != 1 || labels[0].(map[string]any)["cardinality"].(interface{ String() string }).String() != "3" {
		t.Fatalf("incorrect effective-service cardinality: %+v", labels)
	}
	instant := url.Values{"start": {strconv.FormatInt(base, 10)}, "end": {strconv.FormatInt(base, 10)}}
	contextLabels := request("labels", instant)["data"].([]any)
	if len(contextLabels) != 1 || contextLabels[0] != "service_name" {
		t.Fatal("Grafana's zero-width millisecond metadata lookup lost the selected stream")
	}
	contextLogs := canonical(t, request("query_range", url.Values{
		"start": {strconv.FormatInt(base-int64(2*time.Hour), 10)},
		"end":   {strconv.FormatInt(base, 10)},
		"step":  {"7200"},
	}))
	if len(contextLogs) != 1 {
		t.Fatal("two-hour backward context lost the earlier boundary event")
	}
	fields := request("detected_fields", url.Values{"query": {`{service_name="fixture-api"} | json | logfmt | drop __error__, __error_details__`}})["fields"].([]any)
	found := map[string]map[string]any{}
	for _, value := range fields {
		f := value.(map[string]any)
		found[f["label"].(string)] = f
	}
	if found["log_a_db"] == nil || found["log_a_ub"] == nil || found["log_a_db"]["parsers"] != nil || found["log_a_ub"]["parsers"] != nil {
		t.Fatal("structured metadata aliases collided or require a parser")
	}
	if found["duration"] == nil || found["duration"]["type"] != "int" || len(found["duration"]["parsers"].([]any)) != 2 {
		t.Fatalf("numeric/parser metadata is wrong: %v", found["duration"])
	}
	values := request("detected_field/log_a_db/values", nil)["values"].([]any)
	if len(values) != 1 || values[0] != "dot" {
		t.Fatalf("wrong structured field values: %+v", values)
	}
	filtered := canonical(t, request("query_range", url.Values{"query": {`{service_name="fixture-api"} | log_a_db="dot"`}}))
	if len(filtered) != 1 {
		t.Fatal("structured metadata filter failed")
	}
	seriesParams := url.Values{"start": params["start"], "end": params["end"], "match[]": {`{service_name="fixture-api"}`, `{service_name=~"fixture-.+"}`}}
	status, result := getJSON(t, server.URL+"/loki/api/v1/series", seriesParams, false)
	if status != 200 || len(result["data"].([]any)) != 3 {
		t.Fatalf("repeated match[] union failed: %d %+v", status, result)
	}
	for _, query := range []string{
		`{service_name="fixture-api"} | line_format "wrong"`,
		`{service_name="fixture-api"} trailing`,
		`{service_name="fixture-api"} |~ "["`,
		`avg(count_over_time({service_name="fixture-api"}[1m]))`,
	} {
		p := maps.Clone(params)
		p.Set("query", query)
		status, result := getJSON(t, server.URL+"/loki/api/v1/query_range", p, false)
		if status < 400 || result["status"] != "error" {
			t.Fatalf("unsupported query succeeded: %s", query)
		}
	}
	volume := request("index/volume", nil)
	value := volume["data"].(map[string]any)["result"].([]any)[0].(map[string]any)["value"].([]any)[1]
	if value != "194" {
		t.Fatalf("volume must be UTF-8 message bytes, not row count: %v", value)
	}
	stats := request("index/stats", nil)
	if stats["entries"].(interface{ String() string }).String() != "10" || stats["bytes"].(interface{ String() string }).String() != "194" {
		t.Fatalf("stats and selected records disagree: %v", stats)
	}
	status, health := getJSON(t, server.URL+"/loki/api/v1/query", url.Values{"query": {`vector(1)+vector(1)`}}, false)
	if status != 200 || len(health["data"].(map[string]any)["result"].([]any)) != 1 {
		t.Fatal("Grafana health expression failed")
	}
	for _, path := range []string{"query_range", "labels", "label/service_name/values", "series", "detected_labels", "detected_fields", "detected_field/detected_level/values", "index/volume", "index/volume_range", "index/stats"} {
		t.Run("form-post/"+path, func(t *testing.T) {
			p := maps.Clone(params)
			getStatus, expected := getJSON(t, server.URL+"/loki/api/v1/"+path, p, false)
			response, err := http.Post(server.URL+"/loki/api/v1/"+path, "application/x-www-form-urlencoded", strings.NewReader(p.Encode()))
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			var actual map[string]any
			decoder := json.NewDecoder(io.LimitReader(response.Body, 5<<20))
			decoder.UseNumber()
			if err := decoder.Decode(&actual); err != nil {
				t.Fatal(err)
			}
			if getStatus != 200 || response.StatusCode != 200 || !reflect.DeepEqual(actual, expected) {
				t.Fatalf("form POST differs from GET for %s", path)
			}
		})
	}
}

func TestSQLLogPageStaysWithinResultBudget(t *testing.T) {
	_, server := fixtureServerTable(t, "otel_logs_large")
	base := int64(1767225600000000000)
	params := url.Values{"start": {strconv.FormatInt(base, 10)}, "end": {strconv.FormatInt(base+1000, 10)}, "limit": {"100"}}
	for _, direction := range []string{"forward", "backward"} {
		params.Set("direction", direction)
		params.Set("query", `{service_name="fixture-bulk"} | json | logfmt | drop __error__, __error_details__`)
		status, result := getJSON(t, server.URL+"/loki/api/v1/query_range", params, true)
		if status != 200 {
			t.Fatalf("bounded %s page failed: %d", direction, status)
		}
		streams := result["data"].(map[string]any)["result"].([]any)
		count := 0
		for _, stream := range streams {
			for _, raw := range stream.(map[string]any)["values"].([]any) {
				value := raw.([]any)
				ns, _ := strconv.ParseInt(value[0].(string), 10, 64)
				if direction == "forward" && (ns < base || ns >= base+100) || direction == "backward" && (ns < base+900 || ns >= base+1000) {
					t.Fatal("SQL page selected the wrong edge")
				}
				count++
			}
		}
		if count != 100 {
			t.Fatalf("page returned %d records instead of 100", count)
		}
	}
	// A predicate cannot use an SQL limit. The 6 MB selection exceeds one
	// 4 MiB reader result, so it must be read in pages, filtered exactly and
	// stopped once the page is full, from the requested edge.
	params.Set("query", `{service_name="fixture-bulk"} | missing=""`)
	for _, direction := range []string{"forward", "backward"} {
		params.Set("direction", direction)
		status, result := getJSON(t, server.URL+"/loki/api/v1/query_range", params, false)
		if status != 200 {
			t.Fatalf("paged filtered %s scan failed: %d %v", direction, status, result)
		}
		values := result["data"].(map[string]any)["result"].([]any)[0].(map[string]any)["values"].([]any)
		first, _ := strconv.ParseInt(values[0].([]any)[0].(string), 10, 64)
		if len(values) != 100 || direction == "forward" && first != base || direction == "backward" && first != base+999 {
			t.Fatalf("filtered %s page has %d records starting at %d", direction, len(values), first-base)
		}
	}
	// Counting every record of the 6 MB selection through a filter is exact.
	metric := url.Values{"query": {`sum(count_over_time({service_name="fixture-bulk"} | missing="" [1s]))`}, "time": {strconv.FormatInt(base+int64(time.Second)-int64(time.Millisecond), 10)}}
	status, result := getJSON(t, server.URL+"/loki/api/v1/query", metric, false)
	if status != 200 || result["data"].(map[string]any)["result"].([]any)[0].(map[string]any)["value"].([]any)[1] != "1000" {
		t.Fatalf("paged metric lost records: %d %v", status, result)
	}
	params.Del("direction")
	params.Set("query", `{service_name="fixture-bulk"} | missing=""`)
	status, result = getJSON(t, server.URL+"/loki/api/v1/index/volume", params, false)
	if status != 200 || result["data"].(map[string]any)["result"].([]any)[0].(map[string]any)["value"].([]any)[1] != "6000000" {
		t.Fatalf("filtered volume did not cover every record: %d %v", status, result)
	}
	// Overview summaries must not transfer the six megabytes of source rows.
	params.Del("direction")
	params.Set("query", `{service_name="fixture-bulk"}`)
	for _, endpoint := range []string{"index/volume", "index/stats", "labels", "detected_labels", "series", "label/service_name/values"} {
		status, result := getJSON(t, server.URL+"/loki/api/v1/"+endpoint, params, false)
		if status != 200 {
			t.Fatalf("selector-only %s must aggregate within the result budget: %d", endpoint, status)
		}
		switch endpoint {
		case "index/volume":
			series := result["data"].(map[string]any)["result"].([]any)
			if len(series) != 1 || series[0].(map[string]any)["value"].([]any)[1] != "6000000" {
				t.Fatal("summary lost message bytes or truncated input rows")
			}
		case "index/stats":
			if result["entries"].(json.Number).String() != "1000" || result["bytes"].(json.Number).String() != "6000000" {
				t.Fatal("summary statistics did not cover every selected record")
			}
		case "labels":
			if !reflect.DeepEqual(result["data"], []any{"service_name"}) {
				t.Fatal("large selector lost its indexed label")
			}
		}
	}
	params.Set("query", `{service_name="fixture-bulk"} | missing="x"`)
	status, result = getJSON(t, server.URL+"/loki/api/v1/index/volume", params, false)
	if status != 200 || len(result["data"].(map[string]any)["result"].([]any)) != 0 {
		t.Fatalf("summary optimization bypassed a residual filter: %v", result)
	}
}

func TestUnsplittableTimestampFailsExplicitly(t *testing.T) {
	_, server := fixtureServerTable(t, "otel_logs_tie")
	base := int64(1767225600000000000)
	params := url.Values{"start": {strconv.FormatInt(base, 10)}, "end": {strconv.FormatInt(base+1, 10)}, "query": {`{service_name="fixture-tie"} |= "tie"`}}
	status, result := getJSON(t, server.URL+"/loki/api/v1/query_range", params, false)
	if status < 400 || result["status"] != "error" || !strings.Contains(result["error"].(string), "narrow") {
		t.Fatalf("over-budget single timestamp returned %d %v", status, result)
	}
}

func TestPatternsAgainstClickHouse(t *testing.T) {
	_, server := fixtureServerTable(t, "otel_logs_patterns")
	base := int64(1767225600000000000)
	params := url.Values{"start": {strconv.FormatInt(base, 10)}, "end": {strconv.FormatInt(base+int64(2*time.Minute), 10)}, "query": {`{service_name="fixture-patterns"}`}, "step": {"30s"}}
	status, result := getJSON(t, server.URL+"/loki/api/v1/patterns", params, false)
	if status != 200 {
		t.Fatalf("patterns failed: %d %v", status, result)
	}
	totals := map[string]int64{}
	levels := map[string]string{}
	for _, raw := range result["data"].([]any) {
		series := raw.(map[string]any)
		pattern := series["pattern"].(string)
		levels[pattern] = series["level"].(string)
		for _, sample := range series["samples"].([]any) {
			point := sample.([]any)
			ts, _ := point[0].(json.Number).Int64()
			value, _ := point[1].(json.Number).Int64()
			if ts%30 != 0 || ts < base/1e9 || ts >= base/1e9+120 {
				t.Fatalf("sample timestamp outside the stepped range: %d", ts)
			}
			totals[pattern] += value
		}
	}
	want := map[string]int64{"order <_> shipped to warehouse <_>": 40, "payment <_> declined for card <_>": 40}
	wantLevels := map[string]string{"order <_> shipped to warehouse <_>": "info", "payment <_> declined for card <_>": "error"}
	if !reflect.DeepEqual(totals, want) || !reflect.DeepEqual(levels, wantLevels) {
		t.Fatalf("patterns %v levels %v", totals, levels)
	}
	// Drilldown filters by a pattern with |> and excludes it with !>.
	logs := url.Values{"start": params["start"], "end": params["end"], "limit": {"1000"}}
	for pattern, count := range want {
		logs.Set("query", `{service_name="fixture-patterns"} |> "`+pattern+`"`)
		if got := len(canonical(t, mustJSON(t, server.URL+"/loki/api/v1/query_range", logs))); int64(got) != count {
			t.Fatalf("pattern filter %q selected %d lines, want %d", pattern, got, count)
		}
		logs.Set("query", `{service_name="fixture-patterns"} !> "`+pattern+`"`)
		if got := len(canonical(t, mustJSON(t, server.URL+"/loki/api/v1/query_range", logs))); int64(got) != 120-count {
			t.Fatalf("pattern exclusion %q kept %d lines", pattern, got)
		}
	}
	params.Set("query", `{service_name="fixture-patterns"} | json`)
	if status, _ := getJSON(t, server.URL+"/loki/api/v1/patterns", params, false); status != http.StatusBadRequest {
		t.Fatal("pattern pipeline was accepted")
	}
}

func mustJSON(t *testing.T, target string, params url.Values) map[string]any {
	t.Helper()
	status, result := getJSON(t, target, params, false)
	if status != 200 {
		t.Fatalf("%s: %d %v", target, status, result)
	}
	return result
}

func TestWideRangesAgainstClickHouse(t *testing.T) {
	_, server := fixtureServer(t)
	base := int64(1767225600000000000)
	params := url.Values{"query": {`{service_name="fixture-api"}`}, "start": {strconv.FormatInt(base-int64(7*24*time.Hour), 10)}, "end": {strconv.FormatInt(base+int64(7*24*time.Hour), 10)}}
	for _, endpoint := range []string{"query_range", "labels", "detected_labels", "detected_fields", "series", "index/volume", "index/stats"} {
		status, result := getJSON(t, server.URL+"/loki/api/v1/"+endpoint, params, false)
		if status != 200 {
			t.Fatalf("wide %s: %d %v", endpoint, status, result)
		}
	}
	params.Set("query", `sum(count_over_time({service_name="fixture-api"}[2h]))`)
	params.Set("step", "1h")
	// Exact event timestamps below one millisecond remain in the first window.
	status, result := getJSON(t, server.URL+"/loki/api/v1/query_range", params, false)
	if status != 200 {
		t.Fatalf("wide metric/window: %d %v", status, result)
	}
}

func TestHTTPScanBudgetFailsExplicitlyAgainstClickHouse(t *testing.T) {
	for name, costs := range map[string]storage.Limits{"rows": {MaxScanRows: 1}, "bytes": {MaxScanBytes: 1}} {
		t.Run(name, func(t *testing.T) {
			db, err := storage.Open(storage.Config{ServiceNameAttributes: "journald.CONTAINER_NAME,journald._SYSTEMD_UNIT", Address: "127.0.0.1:19000", Database: "fixture", Table: "otel_logs", User: "fixture_reader", Password: "fixture-reader-not-production", Limits: costs})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { db.Close() })
			server := httptest.NewServer(New(db, "integration-test"))
			t.Cleanup(server.Close)
			base := int64(1767225600000000000)
			params := url.Values{"query": {`{service_name="fixture-api"} | json | missing=""`}, "start": {strconv.FormatInt(base-1, 10)}, "end": {strconv.FormatInt(base+100, 10)}}
			status, result := getJSON(t, server.URL+"/loki/api/v1/query_range", params, false)
			if status < 400 || result["status"] != "error" || result["data"] != nil || !strings.Contains(result["error"].(string), "scan budget") {
				t.Fatalf("cost bound returned successful partial output: %d %v", status, result)
			}
		})
	}
}

func TestRecentPatternCoverageAgainstClickHouse(t *testing.T) {
	db, _ := fixtureServerTable(t, "otel_logs_patterns")
	server := httptest.NewServer(New(db, "integration-test", Options{Limits: limits.Config{PatternLines: 90}}))
	t.Cleanup(server.Close)
	base := int64(1767225600000000000)
	params := url.Values{"query": {`{service_name="fixture-patterns"}`}, "start": {strconv.FormatInt(base-int64(24*time.Hour), 10)}, "end": {strconv.FormatInt(base+int64(2*time.Minute), 10)}, "step": {"30s"}}
	response, err := http.Get(server.URL + "/loki/api/v1/patterns?" + params.Encode())
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var result struct {
		Data []struct {
			Samples [][2]int64 `json:"samples"`
		} `json:"data"`
	}
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		t.Fatal(err)
	}
	total := int64(0)
	for _, pattern := range result.Data {
		for _, sample := range pattern.Samples {
			total += sample[1]
		}
	}
	if response.StatusCode != 200 || response.Header.Get("X-Patterns-Sampled") != "true" || response.Header.Get("X-Patterns-Covered-Start") != strconv.FormatInt(base+int64(30*time.Second), 10) || response.Header.Get("X-Patterns-Lines") != "90" || total != 60 {
		t.Fatalf("incorrect recent training: %d %v total=%d", response.StatusCode, response.Header, total)
	}
}
