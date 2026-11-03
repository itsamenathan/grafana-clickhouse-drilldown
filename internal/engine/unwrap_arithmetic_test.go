// SPDX-License-Identifier: AGPL-3.0-only
package engine

import (
	"context"
	"strconv"
	"testing"
	"time"

	"grafana-clickhouse-drilldown/internal/query"
	"grafana-clickhouse-drilldown/internal/record"
)

func TestUnwrapArithmeticMatchesLoki(t *testing.T) {
	for _, tc := range []struct {
		name                 string
		values               []string
		avg, groupedAvg, sum string
	}{
		{"overflow", []string{"1e308", "1e308"}, "1e+308", "+Inf", "+Inf"},
		{"rounding", []string{"1", "2", "2"}, "1.6666666666666665", "1.6666666666666667", "5"},
		{"positive infinity", []string{"+Inf", "+Inf", "1"}, "+Inf", "+Inf", "+Inf"},
		{"negative infinity", []string{"-Inf", "-Inf", "1"}, "-Inf", "-Inf", "-Inf"},
		{"opposite infinities", []string{"+Inf", "-Inf"}, "NaN", "NaN", "NaN"},
		{"infinity then NaN", []string{"+Inf", "NaN"}, "NaN", "NaN", "NaN"},
		{"NaN then infinity", []string{"NaN", "+Inf"}, "NaN", "NaN", "NaN"},
		{"cancellation", []string{"1e16", "1", "-1e16"}, "0", "0", "0"},
		{"conversion", []string{"2", "4", "oops"}, "3", "2", "6"},
		{"missing value", []string{"2", "4", "oops", ""}, "3", "1.5", "6"},
	} {
		for _, form := range []struct{ function, grouping, want string }{
			{"avg_over_time", "", tc.avg},
			{"avg_over_time", " by ()", tc.groupedAvg},
			{"avg_over_time", " without ()", tc.avg},
			{"sum_over_time", "", tc.sum},
		} {
			t.Run(tc.name+"/"+form.function+form.grouping, func(t *testing.T) {
				want, err := strconv.ParseFloat(form.want, 64)
				if err != nil {
					t.Fatal(err)
				}
				encoded := strconv.FormatFloat(want, 'f', -1, 64)
				store := &fakeStore{}
				for i, value := range tc.values {
					store.rows = append(store.rows, record.Raw{Timestamp: 60e9 + int64(i+1)*1e6, Body: "v=" + value, Service: "api", Level: "info"})
				}
				plan, err := query.ParseQuery(form.function + `({service_name="api"} | logfmt | unwrap v | __error__="" [1m])` + form.grouping)
				if err != nil {
					t.Fatal(err)
				}
				src, err := Compile(plan.Selection)
				if err != nil {
					t.Fatal(err)
				}
				series, err := New(store).Metric(context.Background(), src, *plan.Metric, 61e9, 121e9, int64(time.Minute), false)
				if err != nil || len(series) != 1 || len(series[0].Values) != 1 || series[0].Values[0][1] != encoded {
					t.Fatalf("got %+v, %v; want %s in the first window and no sample in the next", series, err, form.want)
				}
			})
		}
	}
}
