// SPDX-License-Identifier: AGPL-3.0-only
package sqlgen

import (
	"testing"

	"grafana-clickhouse-drilldown/internal/query"
)

func FuzzCompile(f *testing.F) {
	for _, text := range []string{
		"{service_name=`fixture-api`}",
		`{service_name="fixture-api"} | json | logfmt | drop __error__, __error_details__ | duration > 10`,
		`sum by (detected_level) (count_over_time({service_name="fixture-api"}[30s]))`,
		`{service_name="fixture-api"} != "a" or "b"`,
		`avg_over_time({service_name="fixture-api"} | logfmt | unwrap duration(d) | __error__="" [1m]) by ()`,
	} {
		f.Add(text)
	}
	f.Fuzz(func(t *testing.T, text string) {
		plan, err := query.ParseQuery(text)
		if err != nil || plan.Health {
			return
		}
		compiled, err := Compile(plan.Selection)
		if err != nil || compiled.Predicate.SQL == "" || compiled.Residual == nil {
			t.Fatalf("validated plan did not compile: %v", err)
		}
		if plan.Metric != nil && plan.Metric.Unwrap() {
			if _, err := plan.Metric.NewExtractor(); err != nil {
				t.Fatalf("validated unwrap has no extractor: %v", err)
			}
		}
	})
}
