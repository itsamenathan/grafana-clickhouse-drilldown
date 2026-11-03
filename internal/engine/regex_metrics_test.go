// SPDX-License-Identifier: AGPL-3.0-only
package engine

import (
	"context"
	"testing"

	"grafana-clickhouse-drilldown/internal/query"
	"grafana-clickhouse-drilldown/internal/record"
)

func TestRegexMetricsNeverBypassLokiResidual(t *testing.T) {
	for _, tc := range []struct {
		body, filter string
		matches      bool
	}{
		{"K", `|~ "(?i)k"`, false}, {"K", `!~ "(?i)k"`, true},
		{"ſ", `|~ "(?i)s"`, false}, {"ſ", `!~ "(?i)s"`, true},
		{"\n", `|~ ".+"`, true}, {"\n", `!~ ".+"`, false},
	} {
		for _, parsers := range []string{"", ` | json | logfmt | drop __error__, __error_details__`} {
			text := `sum(count_over_time({service_name="api"} ` + tc.filter + parsers + ` [1m]))`
			t.Run(tc.body+"/"+text, func(t *testing.T) {
				plan, err := query.ParseQuery(text)
				if err != nil {
					t.Fatal(err)
				}
				src, err := Compile(plan.Selection)
				if err != nil {
					t.Fatal(err)
				}
				store := &bucketStore{fakeStore: fakeStore{rows: []record.Raw{{Timestamp: 60e9, Body: tc.body, Service: "api", Level: "info"}}}}
				result, err := New(store).Metric(context.Background(), src, *plan.Metric, 60e9, 60e9, 1e9, true)
				if err != nil || store.calls != 0 || (len(result) > 0) != tc.matches {
					t.Fatalf("metric bypassed residual: result=%v err=%v SQL calls=%d", result, err, store.calls)
				}
				if tc.matches && (len(result) != 1 || result[0].Value[1] != "1") {
					t.Fatalf("wrong count: %+v", result)
				}
			})
		}
	}
}
