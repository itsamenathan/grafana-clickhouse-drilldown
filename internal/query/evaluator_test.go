// SPDX-License-Identifier: AGPL-3.0-only
package query

import (
	"context"
	"testing"
)

func TestResidualPipelineSemantics(t *testing.T) {
	for _, tc := range []struct {
		stages, body string
		want         bool
		field, value string
	}{
		{`| json | nested_value="雪"`, `{"nested":{"value":"雪"},"n":12}`, true, "n", "12"},
		{`| json | n > 10`, `{"n":12}`, true, "n", "12"},
		{`| json | n > 10`, `{"n":2}`, false, "", ""},
		{`| json | __error__=""`, `not json`, false, "", ""},
		{`| json`, `not json`, true, "__error__", "JSONParserErr"},
		{`| json | logfmt | drop __error__, __error_details__`, `n=12 text="hello world"`, true, "text", "hello world"},
		{`| logfmt --strict | __error__=""`, `not="unterminated`, false, "", ""},
		{`| logfmt | n > 10`, `n=12`, true, "n", "12"},
		{`| log_x="metadata"`, `plain`, true, "", ""},
		{`| missing=""`, `plain`, true, "", ""},
		{`| missing!=""`, `plain`, false, "", ""},
		{`|= "first" or "second"`, `second`, true, "", ""},
		{`|> "<_> error <_>"`, `an error here`, true, "", ""},
	} {
		t.Run(tc.stages+tc.body, func(t *testing.T) {
			selection, err := Parse(`{service_name="fixture-api"} ` + tc.stages)
			if err != nil {
				t.Fatal(err)
			}
			eval, err := selection.NewEvaluator()
			if err != nil {
				t.Fatal(err)
			}
			result, matches, err := eval.Process(context.Background(), 1, tc.body, map[string]string{"service_name": "fixture-api"}, map[string]string{"log_x": "metadata"})
			if err != nil || matches != tc.want {
				t.Fatalf("match=%v want=%v err=%v", matches, tc.want, err)
			}
			if tc.field != "" && result.Parsed[tc.field] != tc.value {
				t.Fatalf("parsed %q = %q, want %q", tc.field, result.Parsed[tc.field], tc.value)
			}
			if matches && tc.stages == `| json | logfmt | drop __error__, __error_details__` {
				if result.Parsed["__error__"] != "" || result.Origins["text"] != "logfmt" {
					t.Fatalf("drop/origin semantics lost: %+v", result)
				}
			}
		})
	}
}

func TestMetricValidation(t *testing.T) {
	for _, text := range []string{
		`sum by (detected_level) (count_over_time({service_name="fixture-api"} | json | logfmt | drop __error__, __error_details__ [1m]))`,
		`sum(rate({service_name="fixture-api"}[30s]))`,
		`bytes_over_time({service_name="fixture-api"}[1m])`,
	} {
		plan, err := ParseQuery(text)
		if err != nil || plan.Metric == nil {
			t.Fatalf("query %s: %+v %v", text, plan, err)
		}
	}
	for _, text := range []string{
		`avg(count_over_time({service_name="fixture-api"}[1m]))`,
		`count_over_time({service_name="fixture-api"}[1m] offset 1m)`,
		`sum(sum(count_over_time({service_name="fixture-api"}[1m])))`,
		`sum_over_time({service_name="fixture-api"} | json | unwrap n [1m])`,
		`count_over_time({service_name="fixture-api"}[2h])`,
	} {
		if _, err := ParseQuery(text); err == nil {
			t.Fatalf("unsupported metric accepted: %s", text)
		}
	}
}
