// SPDX-License-Identifier: AGPL-3.0-only
package query

import "testing"

func TestRegexStagesRequireResidualForMetrics(t *testing.T) {
	for _, filter := range []string{`|~ "(?i)k"`, `!~ "(?i)k"`, `|~ ".+"`, `!~ ".+"`, `|~ "a(b|cd)"`, `|~ "(?s)a.b"`, `|~ "literal"`} {
		for _, parsers := range []string{"", ` | json | logfmt | drop __error__, __error_details__`} {
			text := `sum(count_over_time({service_name="api"} ` + filter + parsers + ` [1m]))`
			t.Run(text, func(t *testing.T) {
				plan, err := ParseQuery(text)
				if err != nil {
					t.Fatal(err)
				}
				if plan.Selection.SQLComplete() || plan.Selection.CanAggregate(*plan.Metric) || plan.Selection.GuardedAggregate(*plan.Metric) {
					t.Fatal("regex residual bypassed by SQL aggregation")
				}
			})
		}
	}
}
