// SPDX-License-Identifier: AGPL-3.0-only
package engine

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"grafana-clickhouse-drilldown/internal/query"
	"grafana-clickhouse-drilldown/internal/record"
	"grafana-clickhouse-drilldown/internal/sqlgen"
)

// fakeStore serves ascending rows within [start,end) and ignores the SQL
// predicate, so the exact residual pipeline alone selects records.
type fakeStore struct {
	rows    []record.Raw
	options []record.ScanOptions
	visited int
}

func (s *fakeStore) Scan(ctx context.Context, _ sqlgen.Predicate, start, end int64, options record.ScanOptions, visit func(record.Raw) error) error {
	s.options = append(s.options, options)
	rows := slices.Clone(s.rows)
	if options.Backward {
		slices.Reverse(rows)
	}
	sent := 0
	for _, row := range rows {
		if row.Timestamp < start || row.Timestamp >= end {
			continue
		}
		if options.Limit > 0 && sent >= options.Limit {
			return nil
		}
		if !options.Metadata {
			row.LogAttributes, row.ResourceAttributes, row.TraceID, row.SpanID = nil, nil, "", ""
		}
		s.visited++
		sent++
		if err := visit(row); err != nil {
			if err == record.ErrStop {
				return nil
			}
			return err
		}
	}
	return nil
}

func source(t *testing.T, text string) Source {
	t.Helper()
	selection, err := query.Parse(text)
	if err != nil {
		t.Fatal(err)
	}
	compiled, err := Compile(selection)
	if err != nil {
		t.Fatal(err)
	}
	return compiled
}

func TestLabelFiltersForceCompleteMetadata(t *testing.T) {
	store := &fakeStore{rows: []record.Raw{{Timestamp: 1, Body: "a", Service: "api", Level: "info", LogAttributes: map[string]string{"x": "y"}}}}
	matched := 0
	err := New(store).Each(context.Background(), source(t, `{service_name="api"} | log_x="y"`), 0, 10, record.ScanOptions{}, func(Event) error {
		matched++
		return nil
	})
	if err != nil || matched != 1 || !store.options[0].Metadata {
		t.Fatalf("label filter evaluated without attributes: matched=%d options=%+v err=%v", matched, store.options, err)
	}
}

func TestFilteredLogsStopAtLimitInBothDirections(t *testing.T) {
	store := &fakeStore{}
	for i := 0; i < 10; i++ {
		body := "drop"
		if i%2 == 0 {
			body = fmt.Sprintf("keep %d", i)
		}
		store.rows = append(store.rows, record.Raw{Timestamp: int64(i), Body: body, Service: "api", Level: "info"})
	}
	engine := New(store)
	for direction, want := range map[string][]string{"forward": {"keep 0", "keep 2"}, "backward": {"keep 6", "keep 8"}} {
		store.visited = 0
		events, err := engine.Logs(context.Background(), source(t, `{service_name="api"} |~ "keep"`), 0, 10, direction, 2)
		if err != nil {
			t.Fatal(err)
		}
		var got []string
		for _, event := range events {
			got = append(got, event.Body)
		}
		if !slices.Equal(got, want) || store.visited > 4 || store.options[len(store.options)-1].Limit != 0 {
			t.Fatalf("%s: got %v after %d rows, options %+v", direction, got, store.visited, store.options)
		}
	}
}

func TestMetricsReadAttributesOnlyWhenOutputNeedsThem(t *testing.T) {
	store := &fakeStore{rows: []record.Raw{{Timestamp: int64(time.Minute), Body: "a", Service: "api", Level: "info", LogAttributes: map[string]string{"x": "y"}}}}
	engine := New(store)
	for text, metadata := range map[string]bool{
		`sum by (detected_level) (count_over_time({service_name="api"}[1m]))`:                                false,
		`sum(count_over_time({service_name="api"} | json | logfmt | drop __error__, __error_details__[1m]))`: false,
		`sum by (log_x) (count_over_time({service_name="api"}[1m]))`:                                         true,
		`count_over_time({service_name="api"}[1m])`:                                                          true,
		`sum without (service_name) (count_over_time({service_name="api"}[1m]))`:                             true,
	} {
		plan, err := query.ParseQuery(text)
		if err != nil {
			t.Fatal(err)
		}
		compiled, _ := Compile(plan.Selection)
		series, err := engine.Metric(context.Background(), compiled, *plan.Metric, int64(time.Minute), int64(time.Minute), int64(time.Second), true)
		if err != nil || len(series) != 1 || store.options[len(store.options)-1].Metadata != metadata {
			t.Fatalf("%s: metadata=%v series=%+v err=%v", text, store.options[len(store.options)-1].Metadata, series, err)
		}
	}
}

func TestPatternsUseLokiDrainPerLevelWithLokiPruning(t *testing.T) {
	store := &fakeStore{}
	base := int64(1767225600) * 1e9
	add := func(n int, level, format string) {
		for i := 0; i < n; i++ {
			store.rows = append(store.rows, record.Raw{Timestamp: base + int64(len(store.rows))*int64(time.Second), Body: fmt.Sprintf(format, i*7, i), Service: "api", Level: level})
		}
	}
	add(40, "info", "request %d completed for user %d in handler")
	add(35, "error", "request %d completed for user %d in handler")
	add(10, "info", "rare event %d happened at %d today")
	slices.SortFunc(store.rows, func(a, b record.Raw) int { return int(a.Timestamp - b.Timestamp) })
	end := base + int64(len(store.rows))*int64(time.Second)
	patterns, err := New(store).Patterns(context.Background(), source(t, `{service_name="api"}`), base, end, int64(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if store.options[0].Metadata {
		t.Fatal("pattern detection transferred attribute maps")
	}
	totals := map[string]int64{}
	for _, pattern := range patterns {
		if !strings.Contains(pattern.Pattern, "<_>") || strings.Contains(pattern.Pattern, "rare") {
			t.Fatalf("unexpected pattern %q", pattern.Pattern)
		}
		for _, sample := range pattern.Samples {
			// Drain's resolution is ten seconds even when a finer step is asked.
			if sample[0]%10 != 0 || sample[0] < base/1e9 || sample[0] >= end/1e9 {
				t.Fatalf("sample timestamp is not a Unix-second step: %v", sample)
			}
			totals[pattern.Level] += sample[1]
		}
	}
	if len(patterns) != 2 || totals["info"] != 40 || totals["error"] != 35 || patterns[0].Level != "info" {
		t.Fatalf("wrong level partition, pruning or totals: %+v %v", patterns, totals)
	}
}

func TestUnwrapUsesLokiExtractionErrorsAndGrouping(t *testing.T) {
	store := &fakeStore{}
	for i, body := range []string{"duration=2s", "duration=4s", "duration=oops", "other=1"} {
		store.rows = append(store.rows, record.Raw{Timestamp: int64(time.Minute) + 1 + int64(i), Body: body, Service: "api", Level: "info"})
	}
	engine := New(store)
	evaluate := func(text string) ([]Series, error) {
		plan, err := query.ParseQuery(text)
		if err != nil {
			t.Fatal(err)
		}
		compiled, _ := Compile(plan.Selection)
		return engine.Metric(context.Background(), compiled, *plan.Metric, 2*int64(time.Minute), 2*int64(time.Minute), int64(time.Minute), true)
	}
	// Loki rewrites the grouped average as sum/count. Its denominator strips
	// unwrap, so duration=oops survives duration!="" and counts as a third row.
	series, err := evaluate(`avg_over_time({service_name="api"} | logfmt | duration!="" | unwrap duration(duration) | __error__="" [1m]) by ()`)
	if err != nil || len(series) != 1 || len(series[0].Metric) != 0 || series[0].Value[1] != "2" {
		t.Fatalf("avg of unwrapped durations: %+v %v", series, err)
	}
	series, err = evaluate(`sum_over_time({service_name="api"} | logfmt | unwrap duration(duration) | __error__="" [1m])`)
	if err != nil || len(series) != 1 || series[0].Value[1] != "6" || series[0].Metric["service_name"] != "api" {
		t.Fatalf("sum of unwrapped durations: %+v %v", series, err)
	}
	// Without the error filter, the conversion failure must fail the query
	// even though by () removes every other label.
	if _, err := evaluate(`avg_over_time({service_name="api"} | logfmt | unwrap duration(duration) [1m]) by ()`); err == nil {
		t.Fatal("unwrap conversion error was silently averaged")
	}
}
