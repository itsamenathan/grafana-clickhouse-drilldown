//go:build integration

// SPDX-License-Identifier: AGPL-3.0-only
package httpapi

import (
	"encoding/json"
	"io"
	"maps"
	"net/http"
	"net/url"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
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
	// A predicate needs the complete bounded scan, not an unsafe pre-filter
	// SQL limit. The 6 MB fixture must exceed the reader's 4 MiB result budget.
	params.Set("query", `{service_name="fixture-bulk"} | missing=""`)
	status, result := getJSON(t, server.URL+"/loki/api/v1/query_range", params, false)
	if status < 400 || result["status"] != "error" {
		t.Fatal("filtered over-budget selection returned partial success")
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
	params.Set("query", `{service_name="fixture-bulk"} | missing=""`)
	status, result = getJSON(t, server.URL+"/loki/api/v1/index/volume", params, false)
	if status < 400 || result["status"] != "error" {
		t.Fatal("summary optimization bypassed a residual filter")
	}
}
