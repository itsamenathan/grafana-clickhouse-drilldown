//go:build integration

// SPDX-License-Identifier: AGPL-3.0-only
package sqlgen

import (
	"context"
	"encoding/hex"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"grafana-clickhouse-drilldown/internal/chcontext"
	"grafana-clickhouse-drilldown/internal/query"
)

func TestRegexPrefiltersKeepEveryLokiMatch(t *testing.T) {
	conn, err := clickhouse.Open(&clickhouse.Options{Addr: []string{"127.0.0.1:19000"}, Auth: clickhouse.Auth{Database: "fixture", Username: "fixture_reader", Password: "fixture-reader-not-production"}, DialTimeout: 3 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	for _, value := range []string{"xxabxx", "K", "K", "k", "ſ", "S", "s", "\n", "a\nb", "", "雪", "a\x00b"} {
		for _, pattern := range []string{`a(b|cd)`, `(?i)k`, `(?i)s`, `.+`, `.*`, `a.*b`, `(?s)a.b`, `^a.b$`, `(?m)^a.b$`, `a|b`} {
			for _, operator := range []string{`|~`, `!~`, `| log_x=~`, `| log_x!~`} {
				text := `{service_name="api"} ` + operator + ` "` + pattern + `"`
				selection, err := query.Parse(text)
				if err != nil {
					t.Fatal(err)
				}
				predicate, err := Selection(selection)
				if err != nil {
					t.Fatal(err)
				}
				parameters := clickhouse.Parameters(predicate.Parameters)
				parameters["value"] = hex.EncodeToString([]byte(value))
				ctx := clickhouse.Context(chcontext.ReadOnly(context.Background()), clickhouse.WithParameters(parameters))
				var selected uint8
				statement := "SELECT toUInt8(" + predicate.SQL + ") FROM (SELECT 'api' AS ServiceName, unhex({value:String}) AS Body, map('x', Body) AS LogAttributes)"
				if err := conn.QueryRow(ctx, statement).Scan(&selected); err != nil {
					t.Fatal(err)
				}
				residual, err := selection.NewEvaluator()
				if err != nil {
					t.Fatal(err)
				}
				_, matches, err := residual.Process(context.Background(), 1, value, map[string]string{"service_name": "api"}, map[string]string{"log_x": value})
				if err != nil {
					t.Fatal(err)
				}
				if matches && selected == 0 {
					t.Errorf("SQL discarded Loki match: %s value=%q", text, value)
				}
			}
		}
	}
}
