//go:build integration

// SPDX-License-Identifier: AGPL-3.0-only
package sqlgen

import (
	"context"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"

	"grafana-clickhouse-drilldown/internal/chcontext"
	"grafana-clickhouse-drilldown/internal/query"
)

// The fixed loopback fixture address, database and public test credentials are
// intentional. There is no production URL or credential fallback.
func TestSelectionAgainstClickHouse(t *testing.T) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr:        []string{"127.0.0.1:19000"},
		Auth:        clickhouse.Auth{Database: "fixture", Username: "fixture_reader", Password: "fixture-reader-not-production"},
		DialTimeout: 3 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	for _, tc := range []struct {
		input string
		want  uint64
	}{
		{"{service_name=`fixture-api`}", 10},
		{`{service_name="fixture-unit"}`, 1},
		{`{service_name="fixture-worker"}`, 1},
		{`{service_name="fixture-api"} |= "duplicate"`, 2},
		{`{service_name="fixture-api"} != "duplicate"`, 8},
		{`{service_name="fixture-api"} |~ "(?i)error"`, 1},
		{`{service_name="fixture-api"} !~ "(?i)error"`, 9},
		{`{service_name="fixture-api"} |~ "^a.b$"`, 0},
		{`{service_name="fixture-api"} |~ "(?s)^a.b$"`, 1},
		{`{service_name=~"api"}`, 0},
		{`{service_name=~".*api"}`, 10},
		{`{service_name="fixture-api",detected_level="error"}`, 1},
		{`{service_name="fixture-api",detected_level="critical"}`, 1},
		{`{service_name="fixture-api"} != ""`, 0},
		{`{service_name="fixture-api"} |= ""`, 10},
		{`{service_name="fixture-api"} |= "hello" != "worker" |~ "api$"`, 1},
		{"{service_name=`'; DROP TABLE fixture.otel_logs; --`}", 0},
		{`{service_name="fixture-api"} |= "backtick \u0060"`, 1},
	} {
		t.Run(tc.input, func(t *testing.T) {
			selection, err := query.Parse(tc.input)
			if err != nil {
				t.Fatal(err)
			}
			predicate, err := Selection(selection)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			ctx = clickhouse.Context(chcontext.ReadOnly(ctx), clickhouse.WithParameters(clickhouse.Parameters(predicate.Parameters)))
			var count uint64
			if err := conn.QueryRow(ctx, "SELECT count() FROM fixture.otel_logs WHERE "+predicate.SQL).Scan(&count); err != nil {
				t.Fatal(err)
			}
			if count != tc.want {
				t.Fatalf("got %d records, want %d", count, tc.want)
			}
		})
	}
}
