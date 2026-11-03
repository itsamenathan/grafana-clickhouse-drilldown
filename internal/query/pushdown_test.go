// SPDX-License-Identifier: AGPL-3.0-only
package query

import "testing"

func TestMetricPushdownEligibility(t *testing.T) {
	for _, tc := range []struct {
		expression          string
		complete, aggregate bool
	}{
		{`sum(count_over_time({service_name="api"}[1m]))`, true, true},
		{`sum by (service_name,detected_level)(rate({service_name="api"} |= "a" != "b" [1m]))`, true, true},
		{`count_over_time({service_name="api"}[1m])`, true, false},
		{`sum without (service_name)(count_over_time({service_name="api"}[1m]))`, true, false},
		{`sum by (log_x)(count_over_time({service_name="api"}[1m]))`, true, false},
		{`sum(count_over_time({service_name="api"} | log_x="a"[1m]))`, false, false},
		{`sum(count_over_time({service_name="api"} |= "a" or "b" [1m]))`, false, false},
		{`sum(count_over_time({service_name="api"} |> "<_> a" [1m]))`, false, false},
		{`sum(count_over_time({service_name="api"} | json | logfmt | drop __error__, __error_details__ [1m]))`, false, false},
		{`sum by (detected_level)(count_over_time({service_name="api"} | json | drop __error__, __error_details__ [1m]))`, false, false},
		{`sum(count_over_time({service_name="api"} | logfmt --strict | drop __error__, __error_details__ [1m]))`, false, false},
		{`sum(count_over_time({service_name="api"} | json | drop __error__ [1m]))`, false, false},
		{`sum(count_over_time({service_name="api"} | drop __error__, __error_details__ | json [1m]))`, false, false},
		{`sum(count_over_time({service_name="api"} | json | drop __error__="JSONParserErr", __error_details__ [1m]))`, false, false},
		{`sum(count_over_time({service_name="api"} | json | __error__="" [1m]))`, false, false},
	} {
		p, err := ParseQuery(tc.expression)
		if err != nil {
			t.Fatal(err)
		}
		if p.Selection.SQLComplete() != tc.complete || p.Selection.CanAggregate(*p.Metric) != tc.aggregate {
			t.Errorf("wrong eligibility for %s: complete=%v aggregate=%v", tc.expression, p.Selection.SQLComplete(), p.Selection.CanAggregate(*p.Metric))
		}
	}
}

func TestGuardedAggregateEligibility(t *testing.T) {
	for _, tc := range []struct {
		stages string
		want   bool
	}{
		{`| json | logfmt | drop __error__, __error_details__`, true},
		{`| logfmt | json | drop __error_details__, __error__`, true},
		{`|= "x" | json | drop __error__, __error_details__`, true},
		{`| json | drop __error__, __error_details__`, true},
		{`| logfmt | drop __error__, __error_details__`, true},
		{`| json | drop __error__`, false},
		{`| json | drop __error__, __error__`, false},
		{`| json | drop __error__, __error_details__, field`, false},
		{`| json | drop __error__="", __error_details__`, false},
		{`| json | drop __error__, __error_details__ | json`, false},
		{`| json | drop __error__, __error_details__ |= "x"`, false},
		{`| logfmt --strict | drop __error__, __error_details__`, false},
		{`| logfmt --keep-empty | drop __error__, __error_details__`, false},
		{`| json x="x" | drop __error__, __error_details__`, false},
		{`| logfmt x="x" | drop __error__, __error_details__`, false},
		{`| drop __error__, __error_details__`, false},
	} {
		p, err := ParseQuery(`sum(count_over_time({service_name="api"} ` + tc.stages + ` [1m]))`)
		if err != nil {
			t.Fatal(err)
		}
		if got := p.Selection.GuardedAggregate(*p.Metric); got != tc.want {
			t.Errorf("%s: got %v", tc.stages, got)
		}
		p.Metric.Without = true
		if p.Selection.GuardedAggregate(*p.Metric) {
			t.Fatal("without was eligible")
		}
	}
}

func TestEmptyMetricExtractorFailsExplicitly(t *testing.T) {
	if _, err := (Metric{}).NewExtractor(); err == nil {
		t.Fatal("empty metric has an extractor")
	}
}
