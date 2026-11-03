// SPDX-License-Identifier: AGPL-3.0-only
package engine

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"grafana-clickhouse-drilldown/internal/query"
)

func TestSlidingWindowsAndBoundaries(t *testing.T) {
	var events []Event
	for _, ns := range []int64{0, 1, 10e9, 10e9, 20e9, 30e9} {
		events = append(events, Event{Timestamp: ns, Body: "abc", Processed: query.Processed{Stream: map[string]string{"service_name": "api"}, Metadata: map[string]string{"detected_level": "info"}}})
	}
	metric := query.Metric{Function: "count_over_time", Window: 20 * time.Second, Sum: true}
	series, err := Metrics(context.Background(), events, metric, 10e9, 30e9, 10e9, false)
	if err != nil || len(series) != 1 || len(series[0].Values) != 3 {
		t.Fatalf("invalid matrix: %+v %v", series, err)
	}
	for i, want := range []string{"4", "4", "2"} {
		if got := series[0].Values[i][1]; got != want {
			t.Fatalf("window %d = %v, want %s", i, got, want)
		}
	}
	metric.Function = "bytes_rate"
	series, err = Metrics(context.Background(), events, metric, 10e9, 10e9, 1e9, true)
	if err != nil || series[0].Value[1] != "0.6" {
		t.Fatalf("wrong bytes rate: %+v %v", series, err)
	}
}

func TestDuplicatesLimitAndCategorization(t *testing.T) {
	event := Event{Timestamp: 1767225600000000011, Body: "duplicate", Processed: query.Processed{Stream: map[string]string{"service_name": "api"}, Metadata: map[string]string{"log_key": "value"}, Parsed: map[string]string{"n": "12"}}}
	streams := Streams([]Event{event, event}, "backward", 2, true)
	if len(streams) != 1 || len(streams[0].Values) != 2 || len(streams[0].Values[0]) != 3 || streams[0].Values[0][0] != "1767225600000000011" {
		t.Fatalf("duplicate or nanosecond loss: %+v", streams)
	}
	if len(Streams([]Event{event, event}, "forward", 1, false)[0].Values) != 1 {
		t.Fatal("global limit not honored")
	}
	if Seconds(event.Timestamp) != json.Number("1767225600.000000011") {
		t.Fatal("seconds encoding lost nanoseconds")
	}
}

func TestMetricParserErrorAndCancellation(t *testing.T) {
	event := Event{Timestamp: 1e9, Processed: query.Processed{Parsed: map[string]string{"__error__": "JSONParserErr"}}}
	metric := query.Metric{Function: "count_over_time", Window: time.Minute, Sum: true}
	if _, err := Metrics(context.Background(), []Event{event}, metric, 2e9, 2e9, 1, true); err == nil {
		t.Fatal("metric parser error silently ignored")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := Metrics(ctx, nil, metric, 1, 1, 1, true); err == nil {
		t.Fatal("canceled metric succeeded")
	}
}
