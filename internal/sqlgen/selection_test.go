// SPDX-License-Identifier: AGPL-3.0-only
package sqlgen

import (
	"encoding/hex"
	"regexp"
	"strings"
	"testing"

	"grafana-clickhouse-drilldown/internal/query"
)

func TestParametersNeverEnterSQL(t *testing.T) {
	input := "{service_name=`'; DROP TABLE default.otel_logs; --`} |= `雪 ' \\` != `secret` |~ `(?i)error` !~ `health.*`"
	selection, err := query.Parse(input)
	if err != nil {
		t.Fatal(err)
	}
	predicate, err := Selection(selection)
	if err != nil {
		t.Fatal(err)
	}
	for _, term := range []string{"DROP TABLE", "secret", "error", "health", "雪"} {
		if strings.Contains(predicate.SQL, term) {
			t.Fatalf("filter value leaked into SQL: %s", predicate.SQL)
		}
	}
	if len(predicate.Parameters) != 3 || strings.Count(predicate.SQL, "{filter_") != 3 {
		t.Fatalf("lost parameter: %+v", predicate)
	}
	if decodeParameter(t, predicate.Parameters["filter_0"]) != "'; DROP TABLE default.otel_logs; --" {
		t.Fatal("parameter changed")
	}
	if !strings.Contains(predicate.SQL, ServiceExpression) || !strings.Contains(predicate.SQL, "NOT (") {
		t.Fatalf("wrong predicate: %s", predicate.SQL)
	}
}

func TestRegexTranslation(t *testing.T) {
	for _, tc := range []struct {
		name, pattern, value string
		label, want          bool
	}{
		{"anchored label", "api", "my-api", true, false},
		{"exact label", "api", "api", true, true},
		{"label dot all", ".+", "a\nb", true, true},
		{"label explicit no dot all", "(?-s).+", "a\nb", true, false},
		{"line substring regex", "api", "my-api", false, true},
		{"line no dot all", "a.b", "a\nb", false, false},
		{"line explicit dot all", "(?s)a.b", "a\nb", false, true},
		{"unicode", "(?i)é", "É", false, true},
		{"empty missing label", ".*", "", true, true},
		{"nonempty missing label", ".+", "", true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := Predicate{Parameters: map[string]string{}}
			condition, err := p.condition("Body", query.Regex, tc.pattern, tc.label)
			if err != nil {
				t.Fatal(err)
			}
			if !tc.label {
				if condition != "" || len(p.Parameters) != 0 {
					t.Fatal("line regex must be residual-only")
				}
				return
			}
			re := regexp.MustCompile(decodeParameter(t, p.Parameters["filter_0"]))
			if got := re.MatchString(tc.value); got != tc.want {
				t.Fatalf("match %q against %q = %v, want %v", re, tc.value, got, tc.want)
			}
		})
	}
}

func TestCompilerRejectsForgedIR(t *testing.T) {
	for _, s := range []query.Selection{
		{},
		{Matchers: []query.Matcher{{Name: "service_name); DROP TABLE logs; --", Op: query.Equal}}},
		{Matchers: []query.Matcher{{Name: "service_name", Op: 255}}},
		{Matchers: []query.Matcher{{Name: "service_name", Op: query.Regex, Value: "["}}},
		{Matchers: []query.Matcher{{Name: "service_name", Op: query.Equal}}, Filters: []query.LineFilter{{Op: 255}}},
	} {
		if _, err := Selection(s); err == nil {
			t.Fatalf("accepted invalid IR: %+v", s)
		}
	}
}

func TestAllMatcherOperatorsAndEmptyValues(t *testing.T) {
	for _, op := range []query.Operator{query.Equal, query.NotEqual, query.Regex, query.NotRegex} {
		p, err := Selection(query.Selection{
			Matchers: []query.Matcher{{Name: "service_name", Op: query.Equal, Value: "api"}, {Name: "detected_level", Op: op, Value: ""}},
			Filters:  []query.LineFilter{{Op: op, Value: ""}},
		})
		wantParameters := 3
		if op == query.Regex || op == query.NotRegex {
			wantParameters = 2
		}
		if err != nil || len(p.Parameters) != wantParameters {
			t.Fatalf("operator %d: %+v, %v", op, p, err)
		}
		if !strings.Contains(p.SQL, LevelExpression) {
			t.Fatal("level filter did not use canonical mapping")
		}
	}
}

func decodeParameter(t *testing.T, value string) string {
	t.Helper()
	decoded, err := hex.DecodeString(value)
	if err != nil {
		t.Fatal(err)
	}
	return string(decoded)
}
