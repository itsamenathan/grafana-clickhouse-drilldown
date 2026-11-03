//go:build integration

// SPDX-License-Identifier: AGPL-3.0-only
package storage

import (
	"context"
	"errors"
	"testing"

	"grafana-clickhouse-drilldown/internal/record"
	"grafana-clickhouse-drilldown/internal/sqlgen"
)

func longServiceClient(t *testing.T, attributes string) *Client {
	t.Helper()
	c, err := Open(Config{Address: "127.0.0.1:19000", Database: "fixture", Table: "otel_logs", User: "fixture_reader", Password: "fixture-reader-not-production", ServiceNameAttributes: attributes})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	// Generated synthetic rows need no writes or new fixture grants. Distinct
	// 60 KB identities defeat compression and exceed the 16 MiB decoded cap.
	c.table = `(SELECT fromUnixTimestamp64Nano(1767225600000000000 + toInt64(number)) AS Timestamp, 'x' AS Body,
		concat(toString(number), repeat('s', 60000)) AS ServiceName, 'info' AS SeverityText, toUInt8(9) AS SeverityNumber,
		map('identity', concat(toString(number), repeat('a', 60000))) AS LogAttributes,
		CAST(map() AS Map(String, String)) AS ResourceAttributes, '' AS TraceId, '' AS SpanId FROM numbers(400))`
	return c
}

func TestScanPlannerIncludesProjectedServiceAndLevel(t *testing.T) {
	for _, mapping := range []string{"", "identity"} {
		c := longServiceClient(t, mapping)
		ctx := context.Background()
		span := window{1767225600000000000, 1767225600000000400}
		buckets, err := c.buckets(ctx, sqlgen.Predicate{SQL: "1"}, span, 400, false)
		if err != nil || len(buckets) != 1 || buckets[0].bytes < 400*(60000+rowOverhead+5) {
			t.Fatalf("projected identity omitted: buckets=%+v err=%v", buckets, err)
		}
		count := 0
		err = c.Scan(ctx, sqlgen.Predicate{SQL: "1"}, span.start, span.end, record.ScanOptions{}, func(record.Raw) error { count++; return nil })
		if err != nil || count != 400 {
			t.Fatalf("planned scan: count=%d err=%v", count, err)
		}
	}
}

func TestSplitFetchSharesDecodedBudget(t *testing.T) {
	for _, backward := range []bool{false, true} {
		c := longServiceClient(t, "")
		span := window{1767225600000000000, 1767225600000000400}
		rows, err := c.fetch(context.Background(), sqlgen.Predicate{SQL: "1"}, span, record.ScanOptions{Backward: backward}, 0, 0)
		if !errors.Is(err, ErrScanLimit) || len(rows) != 0 {
			t.Fatalf("split page exceeded decoded cap: rows=%d err=%v", len(rows), err)
		}
		// A limited read that fits must still stop in the requested direction.
		rows, err = c.fetch(context.Background(), sqlgen.Predicate{SQL: "1"}, span, record.ScanOptions{Backward: backward}, 100, 0)
		if err != nil || len(rows) != 100 {
			t.Fatalf("limited split: rows=%d err=%v", len(rows), err)
		}
		want := span.start
		if backward {
			want = span.end - 1
		}
		if rows[0].Timestamp != want {
			t.Fatalf("wrong split ordering: %d, want %d", rows[0].Timestamp, want)
		}
	}
}
