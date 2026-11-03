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
	"grafana-clickhouse-drilldown/internal/record"
)

func TestMetadataPrefiltersCannotExcludeLokiMatches(t *testing.T) {
	conn, err := clickhouse.Open(&clickhouse.Options{Addr: []string{"127.0.0.1:19000"}, Auth: clickhouse.Auth{Database: "fixture", Username: "fixture_reader", Password: "fixture-reader-not-production"}, DialTimeout: 3 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	for _, value := range []string{"", "dot", "a\nb", "雪", "yes", "a\\nb", "a\tb", "quote'", "a\x00b", "xxabxx", "K", "K", "ſ"} {
		for _, stages := range []string{
			`| log_x=""`, `| log_x!=""`, `| log_x="dot"`, `| log_x!="dot"`, `| log_x="a\nb"`, `| log_x="a\\nb"`, `| log_x="a\tb"`, `| log_x="quote'"`,
			`| log_x=~"dot|雪"`, `| log_x!~"dot|雪"`,
			`| log_x=~"a(b|cd)"`, `| log_x!~"a(b|cd)"`, `| log_x=~"(?i)k"`, `| log_x!~"(?i)k"`, `| log_x!~"(?i)s"`,
			`| log_x=~"(?s)a.*b"`, `| log_x!~"(?s)a.*b"`,
			`| log_x=~".+"`, `| log_x!~".+"`, `| log_x=~""`, `| log_x!~""`, `| log_x=~"a{0}|dot"`, `| log_x!~"a{0}|dot"`,
			`| (log_x="dot" or detected_level="info") and resource_x!="no"`,
			`| log_x="dot" or duration>10`,
			`| log_x="dot" or missing=""`,
			`| json | log_x="yes"`, `| logfmt | log_x="yes"`,
			`| drop log_x | log_x=""`,
		} {
			for _, shape := range []string{"missing", "value", "duplicate"} {
				selection, err := query.Parse(`{service_name="api"} ` + stages)
				if err != nil {
					t.Fatal(err)
				}
				predicate, err := Selection(selection)
				if err != nil {
					t.Fatal(err)
				}
				parameters := clickhouse.Parameters{}
				for k, v := range predicate.Parameters {
					parameters[k] = v
				}
				body := `{"log_x":"yes"}`
				for k, v := range map[string]string{"key": "x", "value": value, "first": "different", "service": "api", "level": "INFO", "body": body} {
					parameters[k] = hex.EncodeToString([]byte(v))
				}
				attrs := "CAST(map() AS Map(String, String))"
				if shape == "value" {
					attrs = "map(unhex({key:String}), unhex({value:String}))"
				}
				if shape == "duplicate" {
					attrs = "map(unhex({key:String}), unhex({first:String}), unhex({key:String}), unhex({value:String}))"
				}
				statement := "SELECT toUInt8(" + predicate.SQL + "), LogAttributes, ResourceAttributes FROM (SELECT unhex({service:String}) AS ServiceName, unhex({level:String}) AS SeverityText, toUInt8(9) AS SeverityNumber, unhex({body:String}) AS Body, " + attrs + " AS LogAttributes, map(unhex({key:String}), unhex({value:String})) AS ResourceAttributes)"
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				ctx = clickhouse.Context(chcontext.ReadOnly(ctx), clickhouse.WithParameters(parameters))
				var selected uint8
				row := record.Raw{Body: body}
				err = conn.QueryRow(ctx, statement).Scan(&selected, &row.LogAttributes, &row.ResourceAttributes)
				cancel()
				if err != nil {
					t.Fatal(err)
				}
				evaluator, err := selection.NewEvaluator()
				if err != nil {
					t.Fatal(err)
				}
				metadata := row.Metadata()
				metadata["detected_level"] = "info"
				_, matches, err := evaluator.Process(context.Background(), 1, row.Body, map[string]string{"service_name": "api"}, metadata)
				if err != nil {
					t.Fatal(err)
				}
				if matches && selected == 0 {
					t.Fatalf("SQL excluded a Loki match: stage=%s shape=%s value=%q", stages, shape, value)
				}
			}
		}
	}
}
