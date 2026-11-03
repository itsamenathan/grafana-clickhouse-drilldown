//go:build integration

// SPDX-License-Identifier: AGPL-3.0-only
package storage

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"

	"grafana-clickhouse-drilldown/internal/chcontext"
	"grafana-clickhouse-drilldown/internal/query"
	"grafana-clickhouse-drilldown/internal/record"
	"grafana-clickhouse-drilldown/internal/sqlgen"
)

func TestRestrictedFixtureReadiness(t *testing.T) {
	client, err := Open(Config{Address: "127.0.0.1:19000", Database: "fixture", Table: "otel_logs", User: "fixture_reader", Password: "fixture-reader-not-production"})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := client.Ready(ctx); err != nil {
		t.Fatal(err)
	}
	client.table = "fixture.nonexistent"
	if err := client.Ready(ctx); err == nil {
		t.Fatal("missing table reported ready")
	}
}

func TestFixtureReaderCannotWrite(t *testing.T) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr:        []string{"127.0.0.1:19000"},
		Auth:        clickhouse.Auth{Database: "fixture", Username: "fixture_reader", Password: "fixture-reader-not-production"},
		DialTimeout: 3 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ctx = chcontext.ReadOnly(ctx)
	if err := conn.Ping(ctx); err != nil {
		t.Fatal("fixture is not reachable")
	}
	if err := conn.Exec(ctx, "CREATE TABLE fixture.forbidden (x UInt8) ENGINE=Memory"); err != nil {
		var exception *clickhouse.Exception
		if !errors.As(err, &exception) || (exception.Code != 164 && exception.Code != 497) {
			t.Fatalf("write failed for an unexpected reason: %v", err)
		}
	} else {
		t.Fatal("restricted fixture reader could create a table")
	}
}

func TestNativeReadOnlyDeadlineCancellation(t *testing.T) {
	client, err := Open(Config{Address: "127.0.0.1:19000", Database: "fixture", Table: "otel_logs", User: "fixture_reader", Password: "fixture-reader-not-production"})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var readonly uint8
	var concurrentLimit uint8
	var executionLimit float64
	if err := client.conn.QueryRow(chcontext.ReadOnly(ctx), "SELECT getSetting('readonly'), getSetting('max_execution_time'), getSetting('max_concurrent_queries_for_user')").Scan(&readonly, &executionLimit, &concurrentLimit); err != nil {
		t.Fatal(err)
	}
	if readonly != 1 || executionLimit != 15 || concurrentLimit != 2 {
		t.Fatalf("reader restrictions changed: readonly=%d timeout=%g concurrency=%d", readonly, executionLimit, concurrentLimit)
	}
	queryCtx, cancelQuery := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancelQuery()
	start := time.Now()
	var value uint8
	err = client.conn.QueryRow(chcontext.ReadOnly(queryCtx), "SELECT sleepEachRow(3) FROM numbers(1)").Scan(&value)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline cancellation, got %v", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("deadline did not interrupt the native driver promptly: %s", elapsed)
	}
	if err := client.Ready(ctx); err != nil {
		t.Fatal("reader failed after query cancellation")
	}
}

func TestPagedScanIsCompleteAndDeterministicInBothDirections(t *testing.T) {
	client, err := Open(Config{Address: "127.0.0.1:19000", Database: "fixture", Table: "otel_logs_large", User: "fixture_reader", Password: "fixture-reader-not-production"})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	predicate := sqlgen.Predicate{SQL: "1", Parameters: map[string]string{}}
	base := int64(1767225600000000000)
	// Six megabytes exceed a single 4 MiB reader result, so this needs pages.
	pages, err := client.plan(ctx, predicate, window{base, base + 1000}, true, 0)
	if err != nil || len(pages) < 3 {
		t.Fatalf("plan did not split the selection: %d pages, %v", len(pages), err)
	}
	read := func(backward bool) []int64 {
		var timestamps []int64
		err := client.Scan(ctx, predicate, base, base+1000, record.ScanOptions{Backward: backward, Metadata: true}, func(row record.Raw) error {
			timestamps = append(timestamps, row.Timestamp)
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		return timestamps
	}
	forward, backward := read(false), read(true)
	slices.Reverse(backward)
	if len(forward) != 1000 || !slices.Equal(forward, backward) || !slices.IsSorted(forward) {
		t.Fatalf("paged scans lost, duplicated or misordered records: %d forward, %d backward", len(forward), len(backward))
	}
}

func TestWideLogScansReadOnlyTheRequestedEdge(t *testing.T) {
	client, err := Open(Config{Address: "127.0.0.1:19000", Database: "fixture", Table: "otel_logs_patterns", User: "fixture_reader", Password: "fixture-reader-not-production", Limits: Limits{MaxScanRows: 1}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	base := int64(1767225600000000000)
	for _, backward := range []bool{false, true} {
		start, end := base, base+int64(30*24*time.Hour)
		want := base
		if backward {
			start, end = base-int64(30*24*time.Hour), base+120e9
			want = base + 119e9
		}
		var timestamps []int64
		// Limit stays zero, as for a filtering residual. The visitor alone knows
		// when enough exact matches have arrived. Reading a whole page from the
		// entire range would exceed this request's one-row budget.
		err := client.Scan(context.Background(), sqlgen.Predicate{SQL: "1"}, start, end, record.ScanOptions{Backward: backward, Metadata: true, EdgeWindows: true}, func(row record.Raw) error {
			timestamps = append(timestamps, row.Timestamp)
			return record.ErrStop
		})
		if err != nil || len(timestamps) != 1 || timestamps[0] != want {
			t.Fatalf("edge scan backward=%v got=%v want=%d err=%v", backward, timestamps, want, err)
		}
	}
}

func TestGrowingWindowScansKeepCompleteReverseOrder(t *testing.T) {
	client, err := Open(Config{Address: "127.0.0.1:19000", Database: "fixture", Table: "otel_logs_patterns", User: "fixture_reader", Password: "fixture-reader-not-production"})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	base := int64(1767225600000000000)
	read := func(backward bool) []record.Raw {
		result := []record.Raw{}
		err := client.Scan(context.Background(), sqlgen.Predicate{SQL: "1"}, base, base+120e9, record.ScanOptions{Backward: backward, Metadata: true, EdgeWindows: true}, func(row record.Raw) error { result = append(result, row); return nil })
		if err != nil {
			t.Fatal(err)
		}
		return result
	}
	forward, backward := read(false), read(true)
	slices.Reverse(backward)
	if len(forward) != 120 {
		t.Fatalf("lost rows: %d", len(forward))
	}
	for i := range forward {
		if forward[i].Timestamp != backward[i].Timestamp || forward[i].Body != backward[i].Body {
			t.Fatalf("not reverse order at %d", i)
		}
	}
}

func TestScanByteBudgetFailsExplicitly(t *testing.T) {
	client, err := Open(Config{Address: "127.0.0.1:19000", Database: "fixture", Table: "otel_logs", User: "fixture_reader", Password: "fixture-reader-not-production", Limits: Limits{MaxScanBytes: 1}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	base := int64(1767225600000000000)
	visited := 0
	err = client.Scan(context.Background(), sqlgen.Predicate{SQL: "1"}, base, base+1e9, record.ScanOptions{Metadata: true}, func(record.Raw) error { visited++; return nil })
	if !errors.Is(err, ErrScanLimit) || visited != 0 {
		t.Fatalf("byte budget returned partial success: visited=%d err=%v", visited, err)
	}
}

func TestReaderProfileAuthority(t *testing.T) {
	for _, user := range []string{"fixture_admin", "fixture_unsafe_reader"} {
		password := "fixture-reader-not-production"
		if user == "fixture_admin" {
			password = "fixture-admin-not-production"
		}
		client, err := Open(Config{Address: "127.0.0.1:19000", Database: "fixture", Table: "otel_logs", User: user, Password: password})
		if err == nil {
			client.Close()
			t.Fatalf("unsafe reader %s was accepted", user)
		}
	}
	client, err := Open(Config{Address: "127.0.0.1:19000", Database: "fixture", Table: "otel_logs_large", User: "fixture_small_reader", Password: "fixture-reader-not-production"})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if client.pageRows != 50 || client.pageBytes != 512<<10 || client.planBuckets != 50 {
		t.Fatalf("profile not applied: %d/%d/%d", client.pageRows, client.pageBytes, client.planBuckets)
	}
	if err := client.Ready(context.Background()); err != nil {
		t.Fatal(err)
	}
	count := 0
	err = client.Scan(context.Background(), sqlgen.Predicate{SQL: "1"}, 1767225600000000000, 1767225600000001000, record.ScanOptions{Metadata: true}, func(record.Raw) error { count++; return nil })
	if err != nil || count != 1000 {
		t.Fatalf("small profile truncated scan: count=%d err=%v", count, err)
	}
}

func TestServiceIdentityMappingAtEveryStorageEndpoint(t *testing.T) {
	base := int64(1767225600000000000)
	for _, tc := range []struct {
		attributes, service string
		count               uint64
	}{
		{"", "raw-service", 2}, {"journald.CONTAINER_NAME,journald._SYSTEMD_UNIT", "fixture-api", 9},
		{"resource:host.name,journald.CONTAINER_NAME,journald._SYSTEMD_UNIT", "fixture-host", 1},
	} {
		client, err := Open(Config{Address: "127.0.0.1:19000", Database: "fixture", Table: "otel_logs", User: "fixture_reader", Password: "fixture-reader-not-production", ServiceNameAttributes: tc.attributes})
		if err != nil {
			t.Fatal(err)
		}
		func() {
			defer client.Close()
			selection, err := query.Parse(`{service_name="` + tc.service + `"}`)
			if err != nil {
				t.Fatal(err)
			}
			predicate, _ := sqlgen.Selection(selection)
			summaries, err := client.Summarize(context.Background(), predicate, base, base+1e9)
			if err != nil || len(summaries) != 1 || summaries[0].Service != tc.service || summaries[0].Entries != tc.count {
				t.Fatalf("summary %s: %+v %v", tc.attributes, summaries, err)
			}
			buckets, err := client.MetricBuckets(context.Background(), predicate, base, base+1e9, 1e9, []string{"service_name"})
			if err != nil || len(buckets) != 1 || buckets[0].Service != tc.service || buckets[0].Entries != tc.count {
				t.Fatalf("metric %s: %+v %v", tc.attributes, buckets, err)
			}
			count := uint64(0)
			err = client.Scan(context.Background(), predicate, base, base+1e9, record.ScanOptions{}, func(row record.Raw) error {
				if row.Service != tc.service {
					t.Fatal(row.Service)
				}
				count++
				return nil
			})
			if err != nil || count != tc.count {
				t.Fatalf("scan %s: %d %v", tc.attributes, count, err)
			}
		}()
	}
}

func TestCountAggregatesDoNotEvaluateBodies(t *testing.T) {
	client, err := Open(Config{Address: "127.0.0.1:19000", Database: "fixture", Table: "otel_logs", User: "fixture_reader", Password: "fixture-reader-not-production"})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	// A nonconstant throwing body expression is harmless only when ClickHouse
	// can prune the Body column. Byte consumers must evaluate it and fail.
	client.table = "(SELECT fromUnixTimestamp64Nano(1767225600000000001) AS Timestamp, 'api' AS ServiceName, 'info' AS SeverityText, toUInt8(9) AS SeverityNumber, toString(throwIf(number = 1)) AS Body FROM numbers(3))"
	p := sqlgen.Predicate{SQL: "1", CountOnly: true}
	summaries, err := client.Summarize(context.Background(), p, 1767225600000000000, 1767225601000000000)
	if err != nil || len(summaries) != 1 || summaries[0].Entries != 3 || summaries[0].Bytes != 0 {
		t.Fatalf("count summary read Body: %+v %v", summaries, err)
	}
	buckets, err := client.MetricBuckets(context.Background(), p, 1767225600000000000, 1767225601000000000, 1e9, nil)
	if err != nil || len(buckets) != 1 || buckets[0].Entries != 3 || buckets[0].Bytes != 0 {
		t.Fatalf("count metric read Body: %+v %v", buckets, err)
	}
	p.CountOnly = false
	if _, err := client.Summarize(context.Background(), p, 1767225600000000000, 1767225601000000000); err == nil {
		t.Fatal("byte summary skipped Body")
	}
	if _, err := client.MetricBuckets(context.Background(), p, 1767225600000000000, 1767225601000000000, 1e9, nil); err == nil {
		t.Fatal("byte metric skipped Body")
	}
}
