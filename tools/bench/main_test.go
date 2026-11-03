// SPDX-License-Identifier: AGPL-3.0-only
package main

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestMatrixMatchesDrilldownAndAnchorsRanges(t *testing.T) {
	end := time.Date(2026, 10, 27, 12, 0, 0, 0, time.UTC)
	queries, err := matrix(end.Add(-7*24*time.Hour), end, "15m,1h,6h,24h,7d", "")
	if err != nil {
		t.Fatal(err)
	}
	if len(queries) != 185 {
		t.Fatalf("got %d queries, want 185", len(queries))
	}
	names := map[string]int{}
	for _, q := range queries {
		names[q.Name]++
		if !q.End.Equal(end) || !q.Start.Before(end) {
			t.Fatal("invalid anchored range", q)
		}
		p := params(q)
		if p.Get("query") != q.Query {
			t.Fatal("query changed")
		}
		if q.Name == "metadata_filter" && !strings.Contains(q.Query, `log_http_droute="/api/v1/orders"`) {
			t.Fatal("wrong scoped metadata alias", q.Query)
		}
		if q.Name == "logs" && (p.Get("direction") != "backward" || p.Get("limit") != "100") {
			t.Fatal("wrong log paging parameters", p)
		}
		if q.Name == "patterns" && (strings.Contains(q.Query, " | ") || p.Get("step") == "") {
			t.Fatal("patterns needs selector and step")
		}
		if q.Name == "level_count" && !strings.Contains(q.Query, "["+q.Step+"]") {
			t.Fatal("window must match step")
		}
	}
	if names["volume"] != 5 || names["unwrap"] != 20 {
		t.Fatal(names)
	}
	short, err := matrix(end.Add(-time.Hour), end, "15m,1h,7d", "logs")
	if err != nil || len(short) != 8 {
		t.Fatalf("coverage filter: %d %v", len(short), err)
	}
}

func TestResultCounts(t *testing.T) {
	cases := []struct {
		body                         string
		rows, series, samples, items int
	}{
		{`{"data":{"resultType":"streams","result":[{"values":[["1","a"],["2","b"]]}]}}`, 2, 1, 0, 0},
		{`{"data":{"resultType":"matrix","result":[{"values":[[1,"3"],[2,"4"]]},{"values":[[1,"5"]]}]}}`, 0, 2, 3, 0},
		{`{"data":{"resultType":"vector","result":[{"value":[1,"4"]}]}}`, 0, 1, 1, 0},
		{`{"detectedLabels":[{"label":"service_name"}]}`, 0, 0, 0, 1},
		{`{"fields":[{},{}]}`, 0, 0, 0, 2},
		{`{"data":[{"pattern":"x <*> y","samples":[{},{}]}]}`, 0, 1, 2, 1},
	}
	for _, c := range cases {
		got, err := parseResponse([]byte(c.body), 200)
		if err != "" || got.Rows != c.rows || got.Series != c.series || got.Samples != c.samples || got.Items != c.items || got.Bytes != len(c.body) {
			t.Fatalf("%s: %+v %s", c.body, got, err)
		}
	}
	_, message := parseResponse([]byte(`{"status":"error","error":"maximum range exceeded"}`), 400)
	if classify(400, message) != "time_range" {
		t.Fatal(message)
	}
}

func TestLocalTargetsOnly(t *testing.T) {
	for _, target := range []string{"http://localhost:3100", "http://observability.n.vpn:3100", "http://192.168.1.1:3100", "https://127.0.0.1:3100", "http://user:pass@127.0.0.1:3100"} {
		if localURL(target) == nil {
			t.Fatal("accepted", target)
		}
	}
	for _, target := range []string{"http://127.0.0.1:29100", "http://[::1]:29100"} {
		if err := localURL(target); err != nil {
			t.Fatal(err)
		}
	}
}

func TestErrorClassesDistinguishCostFromRangeAdvice(t *testing.T) {
	for message, want := range map[string]string{
		"selected log data exceeds the bounded scan budget; narrow the time range or selector": "scan_rows",
		"time range must be ordered and at most 1h0m0s":                                        "time_range",
		"Query memory limit exceeded":                                                          "memory",
		"Limit for rows (or bytes) to read exceeded, max_rows_to_read = 2000000":               "read_rows",
		"selected result exceeds the bounded memory budget; narrow the selection":              "retained_bytes",
	} {
		if got := classify(400, message); got != want {
			t.Fatalf("%q: %s != %s", message, got, want)
		}
	}
}

func TestQueryLogCollectsTerminalEventsAndUnderlyingLimits(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		body, _ := io.ReadAll(r.Body)
		if strings.Contains(string(body), "FROM system.processes") {
			io.WriteString(w, "0")
			return
		}
		if string(body) == "SYSTEM FLUSH LOGS" {
			return
		}
		if !strings.Contains(string(body), "ExceptionBeforeStart") || !strings.Contains(string(body), "query_start_time_microseconds") || !strings.Contains(string(body), "user = 'bench_reader'") {
			t.Error("query-log attribution missing", string(body))
		}
		io.WriteString(w, `{"queries":"2","read_rows":"1234567","read_bytes":"9876543","memory":"123456","duration_ms":"400","codes":[158],"exceptions":["max_rows_to_read exceeded"]}`)
	}))
	defer server.Close()
	got := collect(context.Background(), admin{server.URL, server.Client()}, "bench_reader", "2026-10-27 00:00:00.000000", "2026-10-27 00:00:01.000000")
	if calls != 3 || got.Queries != 2 || got.ReadRows != 1234567 || got.ReadBytes != 9876543 || got.Memory != 123456 || got.DurationMS != 400 || len(got.Exceptions) != 1 || got.Codes[0] != 158 || got.Error != "" {
		t.Fatalf("incorrect query costs: %+v", got)
	}
	if classify(400, got.Exceptions[0]) != "read_rows" {
		t.Fatal(got.Exceptions)
	}
}
