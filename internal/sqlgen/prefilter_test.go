// SPDX-License-Identifier: AGPL-3.0-only
package sqlgen

import (
	"grafana-clickhouse-drilldown/internal/query"
	"strings"
	"testing"
)

func TestMetadataPrefilterSafety(t *testing.T) {
	for _, tc := range []struct {
		stages                 string
		mapFilter, levelFilter bool
	}{
		{`| log_a_db="dot"`, true, false},
		{`| resource_host_dname!=""`, true, false},
		{`| detected_level=~"info|error"`, false, false},
		{`| log_a_db=~"d[oa]t"`, false, false},
		{`| log_a_db!~"(?s)d.*t"`, false, false},
		{`| log_a_db=~"a(b|cd)"`, false, false},
		{`| log_a_db!~"(?i)k"`, false, false},
		{`| log_a_db=~"dot"`, false, false},
		{`| log_a_db=~".*dot.*"`, false, false},
		{`| log_a_db=~""`, false, false},
		{`| log_a_db=~"d{0}|dot"`, false, false},
		{`| log_a_db!~"d{0}|dot"`, false, false},
		{`| json | log_a_db="parsed"`, false, false},
		{`| drop log_a_db | log_a_db=""`, false, false},
		{`| log_a_db="dot" | json`, true, false},
		{`| log_a_db="dot" and duration>10`, true, false},
		{`| log_a_db="dot" or duration>10`, false, false},
		{`| (log_a_db="dot" or detected_level="info") and duration>10`, true, true},
		{`| (log_a_db="dot" and duration>10) or detected_level="info"`, true, true},
		{`| log_a_db="dot" or missing=""`, false, false},
		{`| log_noncanonical_="dot"`, false, false},
	} {
		s, err := query.Parse(`{service_name="api"} ` + tc.stages)
		if err != nil {
			t.Fatal(err)
		}
		p, err := Selection(s)
		if err != nil {
			t.Fatal(err)
		}
		gotMap := strings.Contains(p.SQL, "LogAttributes[unhex({") || strings.Contains(p.SQL, "ResourceAttributes[unhex({")
		gotLevel := strings.Contains(p.SQL, LevelExpression)
		if gotMap != tc.mapFilter || gotLevel != tc.levelFilter {
			t.Errorf("%s: map=%v level=%v SQL=%s", tc.stages, gotMap, gotLevel, p.SQL)
		}
	}
}

func TestAttributeKeysAndValuesAreParameters(t *testing.T) {
	s, err := query.Parse(`{service_name="api"} | log_quote_x27="private'value"`)
	if err != nil {
		t.Fatal(err)
	}
	p, err := Selection(s)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(p.SQL, "quote'") || strings.Contains(p.SQL, "private") {
		t.Fatal("attribute data entered SQL")
	}
	foundKey, foundValue := false, false
	for _, binding := range p.Parameters {
		v := decodeParameter(t, binding)
		foundKey = foundKey || v == "quote'"
		foundValue = foundValue || v == "private'value"
	}
	if !foundKey || !foundValue {
		t.Fatal("lost attribute key/value parameter")
	}
}
