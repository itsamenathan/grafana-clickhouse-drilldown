// SPDX-License-Identifier: AGPL-3.0-only
package engine

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"grafana-clickhouse-drilldown/internal/limits"
	"grafana-clickhouse-drilldown/internal/query"
	"grafana-clickhouse-drilldown/internal/record"
	"grafana-clickhouse-drilldown/internal/sqlgen"
)

type bucketStore struct {
	fakeStore
	calls int
	err   error
}

func (s *bucketStore) MetricBuckets(_ context.Context, predicate sqlgen.Predicate, start, end, step int64, groups []string) ([]record.MetricBucket, error) {
	s.calls++
	if s.err != nil {
		return nil, s.err
	}
	buckets := map[string]*record.MetricBucket{}
	for _, r := range s.rows {
		if r.Timestamp <= start || r.Timestamp > end {
			continue
		}
		b := record.MetricBucket{Timestamp: start + ((r.Timestamp-start-1)/step+1)*step}
		for _, name := range groups {
			if name == "service_name" {
				b.Service = r.Service
			}
			if name == "detected_level" {
				b.Level = r.Level
			}
		}
		key := fmt.Sprintf("%d/%s/%s", b.Timestamp, b.Service, b.Level)
		if buckets[key] == nil {
			buckets[key] = &b
		}
		if predicate.ParserGuard && (strings.Contains(r.Body, "error") || strings.Contains(strings.ReplaceAll(r.Body, `\/`, "/"), `\`) || r.Service == "") {
			buckets[key].ParserGuard++
		}
		buckets[key].Entries++
		buckets[key].Bytes += uint64(len(r.Body))
	}
	result := []record.MetricBucket{}
	for _, b := range buckets {
		result = append(result, *b)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Timestamp < result[j].Timestamp })
	return result, nil
}

func TestMetricBucketsEqualStreaming(t *testing.T) {
	base := int64(4 * time.Hour)
	rows := []record.Raw{}
	for i, offset := range []int64{-2e9, -2e9 + 1, -1, 0, 1, 1e9, 1e9, 1e9 + 1, 2e9, 3e9, 5e9} {
		service, level := "api", "info"
		if i%3 == 0 {
			service, level = "", "error"
		}
		rows = append(rows, record.Raw{Timestamp: base + offset, Body: "雪hello", Service: service, Level: level, LogAttributes: map[string]string{"field": fmt.Sprint(i)}})
	}
	for _, function := range []string{"count_over_time", "rate", "bytes_over_time", "bytes_rate"} {
		for _, groups := range [][]string{nil, {"service_name"}, {"detected_level"}, {"service_name", "detected_level"}} {
			for _, instant := range []bool{false, true} {
				store := &bucketStore{fakeStore: fakeStore{rows: rows}}
				m := query.Metric{Function: function, Window: 2 * time.Second, Sum: true, Groups: groups}
				end := base + 5e9
				if instant {
					end = base
				}
				src := source(t, `{service_name="api"}`)
				got, err := New(store).Metric(context.Background(), src, m, base, end, 1e9, instant)
				if err != nil {
					t.Fatal(err)
				}
				want, err := New(&store.fakeStore).Metric(context.Background(), src, m, base, end, 1e9, instant)
				if err != nil {
					t.Fatal(err)
				}
				if store.calls != 1 || !reflect.DeepEqual(got, want) {
					t.Fatalf("%s %v instant=%v: SQL=%+v stream=%+v", function, groups, instant, got, want)
				}
			}
		}
	}
}

func TestMetricBucketFallbackAndBounds(t *testing.T) {
	base := int64(4 * time.Hour)
	for _, tc := range []struct {
		expression       string
		start, end, step int64
		push             bool
	}{
		{`sum(count_over_time({service_name="api"}[2s]))`, base, base + 2e9, 1e9, true},
		{`sum(count_over_time({service_name="api"}[3s]))`, base, base + 2e9, 2e9, false},
		{`sum(count_over_time({service_name="api"}[2s]))`, base + 1, base + 2e9 + 1, 1e9, true},
		{`sum(count_over_time({service_name="api"}[2s]))`, base, base + 1, 1e9, true},
		{`count_over_time({service_name="api"}[2s])`, base, base, 1e9, false},
		{`sum by (log_x)(count_over_time({service_name="api"}[2s]))`, base, base, 1e9, false},
		{`sum without (service_name)(count_over_time({service_name="api"}[2s]))`, base, base, 1e9, false},
		{`sum(count_over_time({service_name="api"}| json [2s]))`, base, base, 1e9, false},
		{`sum(count_over_time({service_name="api"}| json | logfmt | drop __error__, __error_details__ [2h]))`, base, base, 1e9, true},
	} {
		p, err := query.ParseQuery(tc.expression)
		if err != nil {
			t.Fatal(err)
		}
		src, err := Compile(p.Selection)
		if err != nil {
			t.Fatal(err)
		}
		store := &bucketStore{}
		if _, err := New(store).Metric(context.Background(), src, *p.Metric, tc.start, tc.end, tc.step, false); err != nil {
			t.Fatal(err)
		}
		if (store.calls > 0) != tc.push {
			t.Errorf("wrong fallback: %s", tc.expression)
		}
	}
	store := &bucketStore{err: errors.New("bucket budget exceeded")}
	p, _ := query.ParseQuery(`sum(count_over_time({service_name="api"}[2s]))`)
	src, _ := Compile(p.Selection)
	if _, err := New(store).Metric(context.Background(), src, *p.Metric, base, base, 1e9, false); err != store.err || len(store.options) != 0 {
		t.Fatal("failed SQL aggregation fell back to an expensive scan")
	}
	store.calls = 0
	if _, err := New(store).Metric(context.Background(), src, *p.Metric, base, base+MaxPoints*1e9, 1e9, false); err == nil || store.calls != 0 {
		t.Fatal("point bound was not enforced before database work")
	}
}

func TestMetricBucketSeriesAndSampleBudgets(t *testing.T) {
	for _, tc := range []struct{ groups, points int }{{1001, 1}, {1000, 11}} {
		store := &bucketStore{}
		for i := 0; i < tc.groups; i++ {
			store.rows = append(store.rows, record.Raw{Timestamp: 60e9, Service: fmt.Sprint(i), Level: "info"})
		}
		metric := query.Metric{Function: "count_over_time", Window: time.Minute, Sum: true, Groups: []string{"service_name"}}
		_, err := New(store).Metric(context.Background(), source(t, `{service_name=~".+"}`), metric, 60e9, 60e9+int64(tc.points-1)*1e9, 1e9, false)
		if err == nil {
			t.Fatalf("accepted %d groups and %d points", tc.groups, tc.points)
		}
	}
}

func TestMetricsAndLogsHonorConfiguredRetention(t *testing.T) {
	store := &bucketStore{fakeStore: fakeStore{rows: []record.Raw{{Timestamp: 60e9, Body: "line", Service: "api", Level: "info"}}}}
	engine := NewWithLimits(store, limits.Config{MaxRetainedBytes: 1})
	if _, err := engine.Logs(context.Background(), source(t, `{service_name="api"}`), 0, 61e9, "forward", 1); err != ErrRetainedLimit {
		t.Fatalf("log retention budget ignored: %v", err)
	}
	metric := query.Metric{Function: "count_over_time", Window: time.Minute, Sum: true}
	if _, err := engine.Metric(context.Background(), source(t, `{service_name="api"}`), metric, 60e9, 60e9, 1e9, true); err != ErrRetainedLimit {
		t.Fatalf("metric retention budget ignored: %v", err)
	}
}

func TestGuardedMetricsMatchStreaming(t *testing.T) {
	for _, body := range []string{`{"n":12,"service_name":"other","detected_level":"error"}`, `n=12 service_name=other detected_level=other`, `not json`, `{"__error__":"body"}`, `!!error!!=body`, `{"_":{"error":{"_":"body"}}}`, `{"\u005f_error__":"body"}`, `{"__\u0065rror__":"body"}`, `service_name=from_body`, `{"path":"\/api\/v1"}`, `{"e\/rror":"safe"}`, `{"path":"\\/api"}`} {
		for _, service := range []string{"api", ""} {
			for _, function := range []string{"count_over_time", "rate", "bytes_over_time", "bytes_rate"} {
				store := &bucketStore{fakeStore: fakeStore{rows: []record.Raw{{Timestamp: 60e9, Service: service, Level: "info", Body: body}}}}
				p, err := query.ParseQuery(`sum by (service_name, detected_level)(` + function + `({detected_level=~".+"} | json | logfmt | drop __error__, __error_details__ [2s]))`)
				if err != nil {
					t.Fatal(err)
				}
				src, _ := Compile(p.Selection)
				got, gotErr := New(store).Metric(context.Background(), src, *p.Metric, 60e9, 62e9, 1e9, false)
				want, wantErr := New(&fakeStore{rows: store.rows}).Metric(context.Background(), src, *p.Metric, 60e9, 62e9, 1e9, false)
				if (gotErr != nil) != (wantErr != nil) || !reflect.DeepEqual(got, want) {
					t.Fatalf("%s service=%q body=%s: SQL=%v/%v stream=%v/%v", function, service, body, got, gotErr, want, wantErr)
				}
				guarded := strings.Contains(body, "error") || strings.Contains(strings.ReplaceAll(body, `\/`, "/"), `\`) || service == ""
				if store.calls != 1 || (len(store.options) > 0) != guarded {
					t.Fatalf("incorrect guard/fallback for %s service=%q", body, service)
				}
			}
		}
	}
}

func TestMetricBucketsUseRelativeEvaluationGrid(t *testing.T) {
	for _, phase := range []int64{0, 1, 20 * 60e9, 123456789} {
		for _, extra := range []int64{0, 1, 999999999} {
			base := int64(4*time.Hour) + phase
			rows := []record.Raw{}
			for _, offset := range []int64{-2e9, -2e9 + 1, -1, 0, 1, 1e9, 1e9 + 1, 2e9, 3e9} {
				rows = append(rows, record.Raw{Timestamp: base + offset, Service: "api", Level: "info", Body: "hello"})
			}
			store := &bucketStore{fakeStore: fakeStore{rows: rows}}
			for _, text := range []string{`sum(count_over_time({service_name="api"}[2s]))`, `sum by (detected_level)(bytes_rate({service_name="api"} | json | logfmt | drop __error__, __error_details__ [2s]))`} {
				p, _ := query.ParseQuery(text)
				src, _ := Compile(p.Selection)
				got, err := New(store).Metric(context.Background(), src, *p.Metric, base, base+2e9+extra, 1e9, false)
				if err != nil {
					t.Fatal(err)
				}
				want, err := New(&fakeStore{rows: rows}).Metric(context.Background(), src, *p.Metric, base, base+2e9+extra, 1e9, false)
				if err != nil || !reflect.DeepEqual(got, want) {
					t.Fatalf("phase=%d extra=%d SQL=%v stream=%v err=%v", phase, extra, got, want, err)
				}
			}
			if store.calls != 2 {
				t.Fatal("relative grid streamed instead of aggregating")
			}
		}
	}
}
