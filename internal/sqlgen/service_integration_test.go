//go:build integration

// SPDX-License-Identifier: AGPL-3.0-only
package sqlgen

import (
	"context"
	"encoding/hex"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/prometheus/prometheus/model/labels"
	"grafana-clickhouse-drilldown/internal/chcontext"
	"grafana-clickhouse-drilldown/internal/query"
	"testing"
	"time"
)

func TestServiceRegexKeyPrefiltersAreExact(t *testing.T) {
	conn, err := clickhouse.Open(&clickhouse.Options{Addr: []string{"127.0.0.1:19000"}, Auth: clickhouse.Auth{Database: "fixture", Username: "fixture_reader", Password: "fixture-reader-not-production"}})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	for _, pattern := range []string{"api", "api|worker", "archive-.*", "(?i)api.*", "(?i)é", "[ab]", "a?", ".*", "(?m)^api$", "a.b", "(?-s)a.b", "(?:api|worker).*", "api\\x00.*"} {
		p, err := Selection(query.Selection{Matchers: []query.Matcher{{Name: "service_name", Op: query.Regex, Value: pattern}}})
		if err != nil {
			t.Fatal(err)
		}
		matcher, err := labels.NewMatcher(labels.MatchRegexp, "service_name", pattern)
		if err != nil {
			t.Fatal(err)
		}
		for _, value := range []string{"", "api", "worker", "apiculture", "archive-sweeper", "API", "É", "a\nb", "a", "b", "api\x00foo", "api\n"} {
			parameters := clickhouse.Parameters{}
			for k, v := range p.Parameters {
				parameters[k] = v
			}
			parameters["service_value"] = hex.EncodeToString([]byte(value))
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			ctx = clickhouse.Context(chcontext.ReadOnly(ctx), clickhouse.WithParameters(parameters))
			var selected uint8
			err = conn.QueryRow(ctx, "SELECT toUInt8("+p.SQL+") FROM (SELECT unhex({service_value:String}) AS ServiceName)").Scan(&selected)
			cancel()
			if err != nil || (selected != 0) != matcher.Matches(value) {
				t.Fatalf("pattern=%q value=%q selected=%d err=%v SQL=%s", pattern, value, selected, err, p.SQL)
			}
		}
	}
}
