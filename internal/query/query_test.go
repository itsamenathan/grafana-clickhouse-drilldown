// SPDX-License-Identifier: AGPL-3.0-only
package query

import (
	"errors"
	"strings"
	"testing"
)

func TestLogLimitCannotCrossAResidualFilter(t *testing.T) {
	for _, tc := range []struct {
		stages string
		want   bool
	}{
		{"", true},
		{`| json | logfmt | drop __error__, __error_details__`, true},
		{`| json n="nested.n"`, true},
		{`| missing=""`, false},
		{`| json | n > 10`, false},
		{`| json | __error__=""`, false},
		{`|= "needle"`, false},
		{`|= "first" or "second"`, false},
		{`|> "<_> error <_>"`, false},
	} {
		selection, err := Parse(`{service_name="fixture-api"} ` + tc.stages)
		if err != nil || selection.CanLimitLogs() != tc.want {
			t.Fatalf("unsafe limit plan for %q: %v", tc.stages, err)
		}
	}
}

func TestSelectorsAndFilters(t *testing.T) {
	for _, input := range []string{
		"{service_name=`fixture-api`}",
		`{service_name="fixture-api"}`,
		`{service_name="fixture-api",detected_level!="debug",service_name=~".+",detected_level!~"warn|error"}`,
		`{service_name="fixture-api"} |= "" != "" |~ "(?i)héllo" !~ "health.*"`,
		"{service_name=\"a\\\"b\"} |= `quote\"and ☃`",
	} {
		t.Run(input, func(t *testing.T) {
			if _, err := Parse(input); err != nil {
				t.Fatal(err)
			}
		})
	}
	selection, err := Parse(`{service_name="fixture-api"} |= "first" != "second" |~ "third" !~ "fourth"`)
	if err != nil {
		t.Fatal(err)
	}
	if len(selection.Filters) != 4 {
		t.Fatalf("lost filters: %+v", selection.Filters)
	}
	for i, value := range []string{"first", "second", "third", "fourth"} {
		if selection.Filters[i].Value != value || selection.Filters[i].Op != Operator(i) {
			t.Fatalf("wrong filter %d: %+v", i, selection.Filters[i])
		}
	}
}

func TestRejectsEveryUnsupportedStage(t *testing.T) {
	for _, suffix := range []string{
		`| line_format "{{.service_name}}"`,
		`| label_format x="value"`, `| keep service_name`, `| decolorize`,
		`| unpack`, `| regexp "(?P<name>.+)"`,
	} {
		t.Run(suffix, func(t *testing.T) {
			_, err := Parse(`{service_name="fixture-api"} |= "before" ` + suffix + ` |= "after"`)
			if !errors.Is(err, ErrUnsupported) {
				t.Fatalf("must explicitly reject unsupported stage: %v", err)
			}
		})
	}
}

func TestRejectsMetricExpressions(t *testing.T) {
	for _, input := range []string{
		`sum by (service_name) (count_over_time({service_name="fixture-api"}[1m]))`,
		`rate({service_name="fixture-api"}[1m])`, `vector(1)`, `1+2`,
		`{unknown="value"}`,
	} {
		if _, err := Parse(input); !errors.Is(err, ErrUnsupported) {
			t.Fatalf("must reject %s: %v", input, err)
		}
	}
}

func TestRejectsInvalidOrExcessiveInput(t *testing.T) {
	for _, input := range []string{
		"", "{service_name=`unterminated}", `{service_name="fixture-api"} trailing`,
		`{service_name="fixture-api"} |~ "["`,
		`{service_name=~"["}`, `{service_name="fixture-api"} |=`,
		strings.Repeat("x", MaxQueryBytes+1),
		`{service_name="fixture-api"}` + strings.Repeat(` |= "x"`, MaxFilters+1),
	} {
		if _, err := Parse(input); err == nil {
			t.Fatalf("accepted invalid input: %.80s", input)
		}
	}
}

func FuzzParse(f *testing.F) {
	for _, input := range []string{
		"{service_name=`fixture-api`}", `{service_name=~".+"} !~ "(?i)error"`,
		`{service_name="fixture-api"} | json | drop __error__`, `vector(1)`, "",
	} {
		f.Add(input)
	}
	f.Fuzz(func(t *testing.T, input string) {
		selection, err := Parse(input)
		if err == nil && (len(selection.Matchers) == 0 || len(selection.Filters) > MaxFilters) {
			t.Fatalf("invalid selection: %+v", selection)
		}
	})
}
