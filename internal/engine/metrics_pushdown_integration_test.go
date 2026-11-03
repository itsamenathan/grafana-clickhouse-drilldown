//go:build integration

// SPDX-License-Identifier: AGPL-3.0-only
package engine

import (
	"context"
	"reflect"
	"testing"
	"time"

	"grafana-clickhouse-drilldown/internal/query"
	"grafana-clickhouse-drilldown/internal/record"
	"grafana-clickhouse-drilldown/internal/sqlgen"
	"grafana-clickhouse-drilldown/internal/storage"
)

func fixtureStorage(t *testing.T, table string, costs storage.Limits) *storage.Client {
	t.Helper()
	db, err := storage.Open(storage.Config{ServiceNameAttributes: "journald.CONTAINER_NAME,journald._SYSTEMD_UNIT", Address: "127.0.0.1:19000", Database: "fixture", Table: table, User: "fixture_reader", Password: "fixture-reader-not-production", Limits: costs})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func TestClickHouseMetricsEqualStreaming(t *testing.T) {
	db := fixtureStorage(t, "otel_logs", storage.Limits{})
	// Hiding the optional aggregate capability forces the existing reference path.
	stream := New(struct{ Store }{db})
	pushed := New(db)
	base := int64(1767225600000000000)
	for _, expression := range []string{
		`sum(count_over_time({service_name=~".+"}[2s]))`,
		`sum by (service_name,detected_level)(rate({service_name=~".+"}[2s]))`,
		`sum by (detected_level)(bytes_over_time({service_name="fixture-api"}[2s]))`,
		`sum by (service_name)(bytes_rate({service_name="fixture-api"} != "health" [2s]))`,
		`sum by (service_name)(count_over_time({detected_level="unknown"}[2s]))`,
		`sum(count_over_time({service_name="fixture-api"}| json | logfmt | drop __error__, __error_details__[2s]))`,
		`sum(count_over_time({service_name="fixture-api"}| logfmt --strict | drop __error__, __error_details__[2s]))`,
		`sum(count_over_time({service_name="fixture-api"}[2h]))`,
	} {
		t.Run(expression, func(t *testing.T) {
			p, err := query.ParseQuery(expression)
			if err != nil {
				t.Fatal(err)
			}

			src, err := Compile(p.Selection)
			if err != nil {
				t.Fatal(err)
			}
			for _, instant := range []bool{false, true} {
				end := base + 3e9
				if instant {
					end = base + 1e9
				}
				ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
				defer cancel()
				got, err := pushed.Metric(ctx, src, *p.Metric, base+1e9, end, 1e9, instant)
				if err != nil {
					t.Fatal(err)
				}
				want, err := stream.Metric(ctx, src, *p.Metric, base+1e9, end, 1e9, instant)
				if err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("SQL=%+v streaming=%+v", got, want)
				}
			}
		})
	}
}

func TestClickHouseMetricPushdownDoesNotScanRawRows(t *testing.T) {
	// All 1000 bodies are 6000 bytes. SQL aggregation succeeds even with a raw
	// row budget of one and a body budget smaller than every individual line.
	db := fixtureStorage(t, "otel_logs_large", storage.Limits{MaxScanRows: 1, MaxRecordBytes: 128})
	p, _ := query.ParseQuery(`sum(bytes_over_time({service_name="fixture-bulk"}[2h]))`)
	src, _ := Compile(p.Selection)
	base := int64(1767225600000000000)
	got, err := New(db).Metric(context.Background(), src, *p.Metric, base+1e9, base+1e9, 1e9, true)
	if err != nil || len(got) != 1 || got[0].Value[1] != "6000000" {
		t.Fatalf("aggregation copied bodies or lost bytes: %+v %v", got, err)
	}
	if _, err := New(struct{ Store }{db}).Metric(context.Background(), src, *p.Metric, base+1e9, base+1e9, 1e9, true); err == nil {
		t.Fatal("reference scan ignored raw row/body budget")
	}
}

func TestClickHouseAttributePrefiltersEqualResidual(t *testing.T) {
	db := fixtureStorage(t, "otel_logs", storage.Limits{})
	base := int64(1767225600000000000)
	selector, _ := query.Parse(`{service_name="fixture-api"}`)
	baseline, _ := sqlgen.Selection(selector)
	for _, stages := range []string{
		`| log_a_db="dot"`, `| log_a_db!="dot"`, `| log_a_db=""`, `| log_a_db!=""`,
		`| resource_host_dname=~"fixture-[a-z]+"`, `| resource_host_dname!~"fixture-[a-z]+"`,
		`| log_a_db="dot" or detected_level="info"`,
		`| log_a_db="dot" and detected_level!="info"`,
		`| log_a_db="dot" or missing=""`,
		`| (log_a_db="dot" or detected_level="info") and log_missing=""`,
		`| log_a_db="dot" or duration>10`,
		`| json | log_a_db=""`, `| json log_foo="duration" | log_foo="12"`, `| logfmt | log_duration="12"`,
		`| drop log_a_db | log_a_db=""`,
	} {
		t.Run(stages, func(t *testing.T) {
			src := source(t, `{service_name="fixture-api"} `+stages)
			reference := src
			reference.compiled.Predicate = baseline
			read := func(s Source) []Event {
				result := []Event{}
				err := New(db).Each(context.Background(), s, base-1e9, base+1e9, record.ScanOptions{Metadata: true}, func(e Event) error { result = append(result, e); return nil })
				if err != nil {
					t.Fatal(err)
				}
				return result
			}
			got, want := read(src), read(reference)
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("prefilter excluded residual matches: got %d want %d", len(got), len(want))
			}
		})
	}
}

func TestClickHouseMetricBucketsHonorExactSecondBoundaries(t *testing.T) {
	db := fixtureStorage(t, "otel_logs_patterns", storage.Limits{})
	base := int64(1767225600000000000)
	for _, function := range []string{"count_over_time", "rate", "bytes_over_time", "bytes_rate"} {
		metric := query.Metric{Function: function, Window: 20 * time.Second, Sum: true, Groups: []string{"detected_level"}}
		src := source(t, `{service_name="fixture-patterns"}`)
		got, err := New(db).Metric(context.Background(), src, metric, base+20e9, base+100e9, 10e9, false)
		if err != nil {
			t.Fatal(err)
		}
		want, err := New(struct{ Store }{db}).Metric(context.Background(), src, metric, base+20e9, base+100e9, 10e9, false)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("%s boundary mismatch: SQL=%+v stream=%+v", function, got, want)
		}
	}
}

func TestMetricBucketBudgetFailsInsteadOfTruncating(t *testing.T) {
	db := fixtureStorage(t, "otel_logs_patterns", storage.Limits{MaxMetricBuckets: 1})
	base := int64(1767225600000000000)
	p, _ := query.ParseQuery(`sum(count_over_time({service_name="fixture-patterns"}[20s]))`)
	src, _ := Compile(p.Selection)
	got, err := New(db).Metric(context.Background(), src, *p.Metric, base+20e9, base+100e9, 10e9, false)
	if err == nil || got != nil {
		t.Fatalf("bucket budget returned partial success: %d series %v", len(got), err)
	}
}

func TestGuardedClickHouseMetricsDoNotTransferBodies(t *testing.T) {
	db := fixtureStorage(t, "otel_logs_large", storage.Limits{MaxScanRows: 1, MaxRecordBytes: 128})
	for _, function := range []string{"count_over_time", "rate", "bytes_over_time", "bytes_rate"} {
		p, err := query.ParseQuery(`sum by (service_name,detected_level)(` + function + `({service_name="fixture-bulk"} | json | logfmt | drop __error__, __error_details__ [2s]))`)
		if err != nil {
			t.Fatal(err)
		}
		src, _ := Compile(p.Selection)
		got, err := New(db).Metric(context.Background(), src, *p.Metric, 1767225601000000000, 1767225601000000000, 1e9, true)
		if err != nil || len(got) != 1 {
			t.Fatalf("guarded %s copied bodies: %+v %v", function, got, err)
		}
	}
}

func TestGuardedClickHouseMetricsAllowEscapedSlashes(t *testing.T) {
	db := fixtureStorage(t, "otel_logs_parser_guards", storage.Limits{MaxScanRows: 1, MaxRecordBytes: 128})
	p, err := query.ParseQuery(`sum(count_over_time({service_name="fixture-guard-safe"} | json | logfmt | drop __error__, __error_details__ [2s]))`)
	if err != nil {
		t.Fatal(err)
	}
	src, _ := Compile(p.Selection)
	got, err := New(db).Metric(context.Background(), src, *p.Metric, 1767225601000000000, 1767225601000000000, 1e9, true)
	if err != nil || len(got) != 1 || got[0].Value[1] != "5" {
		t.Fatalf("slash-only escapes forced streaming: %+v %v", got, err)
	}
}

func TestClickHouseMetricBucketsUseRelativeEvaluationGrid(t *testing.T) {
	db := fixtureStorage(t, "otel_logs_patterns", storage.Limits{})
	base := int64(1767225600000000000)
	for _, phase := range []int64{1, 123456789, 20e9} {
		for _, function := range []string{"count_over_time", "rate", "bytes_over_time", "bytes_rate"} {
			p, err := query.ParseQuery(`sum by (detected_level)(` + function + `({service_name="fixture-patterns"} | json | logfmt | drop __error__, __error_details__ [20s]))`)
			if err != nil {
				t.Fatal(err)
			}
			src, _ := Compile(p.Selection)
			start, end := base+phase, base+phase+100e9+12345
			got, err := New(db).Metric(context.Background(), src, *p.Metric, start, end, 10e9, false)
			if err != nil {
				t.Fatal(err)
			}
			want, err := New(struct{ Store }{db}).Metric(context.Background(), src, *p.Metric, start, end, 10e9, false)
			if err != nil || !reflect.DeepEqual(got, want) {
				t.Fatalf("%s phase=%d SQL=%v streaming=%v err=%v", function, phase, got, want, err)
			}
		}
	}
}
