//go:build integration

// SPDX-License-Identifier: AGPL-3.0-only
package storage

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"

	"grafana-clickhouse-drilldown/internal/chcontext"
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
