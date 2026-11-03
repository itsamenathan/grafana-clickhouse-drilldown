// SPDX-License-Identifier: AGPL-3.0-only
package sqlgen

import (
	"grafana-clickhouse-drilldown/internal/query"
	"strings"
	"testing"
)

func TestServiceMapping(t *testing.T) {
	for _, raw := range []string{"", "journald.CONTAINER_NAME,journald._SYSTEMD_UNIT", "resource:service.name,log:app/name", "x-1:y"} {
		m, err := ParseServiceMapping(raw)
		if err != nil {
			t.Fatal(err)
		}
		selection, _ := query.Parse(`{service_name="ServiceName'); SELECT 1", detected_level="info"}`)
		p, _ := Selection(selection)
		bound := m.Bind(p)
		if strings.Contains(bound.SQL, "SELECT 1") || len(bound.Parameters) != 2 {
			t.Fatal("mapping changed parameter safety")
		}
		if raw == "" && (m.Expression() != "ServiceName" || strings.Contains(bound.SQL, "LogAttributes")) {
			t.Fatal("default reads attribute maps")
		}
		if raw == "resource:service.name,log:app/name" && m.Expression() != "coalesce(nullIf(ResourceAttributes['service.name'], ''), nullIf(LogAttributes['app/name'], ''), ServiceName)" {
			t.Fatal(m.Expression())
		}
	}
	for _, raw := range []string{",", "x,", "log:", "resource:", "x'", "x\\", "a b", "x\nSETTINGS", "雪", strings.Repeat("a", 257), strings.Repeat("x,", 16) + "x"} {
		if _, err := ParseServiceMapping(raw); err == nil {
			t.Fatalf("accepted %q", raw)
		}
	}
}
