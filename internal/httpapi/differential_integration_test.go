//go:build integration

// SPDX-License-Identifier: AGPL-3.0-only
package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/grafana/loki/v3/pkg/logql/syntax"
	"github.com/prometheus/prometheus/model/labels"

	"grafana-clickhouse-drilldown/internal/query"
	"grafana-clickhouse-drilldown/internal/record"
	"grafana-clickhouse-drilldown/internal/sqlgen"
	"grafana-clickhouse-drilldown/internal/storage"
)

func fixtureServer(t *testing.T) (*storage.Client, *httptest.Server) {
	return fixtureServerTable(t, "otel_logs")
}

func fixtureServerTable(t *testing.T, table string) (*storage.Client, *httptest.Server) {
	t.Helper()
	db, err := storage.Open(storage.Config{ServiceNameAttributes: "journald.CONTAINER_NAME,journald._SYSTEMD_UNIT", Address: "127.0.0.1:19000", Database: "fixture", Table: table, User: "fixture_reader", Password: "fixture-reader-not-production"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	server := httptest.NewServer(New(db, "integration-test"))
	t.Cleanup(server.Close)
	return db, server
}

func getJSON(t *testing.T, target string, params url.Values, categorize bool) (int, map[string]any) {
	t.Helper()
	request, _ := http.NewRequest("GET", target+"?"+params.Encode(), nil)
	if categorize {
		request.Header.Set("X-Loki-Response-Encoding-Flags", "categorize-labels")
	}
	client := &http.Client{Timeout: 20 * time.Second}
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var result map[string]any
	body, err := io.ReadAll(io.LimitReader(response.Body, 5<<20))
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != 200 && !json.Valid(body) {
		return response.StatusCode, map[string]any{"status": "error", "error": string(body)}
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	if err := decoder.Decode(&result); err != nil {
		t.Fatalf("status %d: %v", response.StatusCode, err)
	}
	return response.StatusCode, result
}

func canonical(t *testing.T, response map[string]any) []string {
	t.Helper()
	data := response["data"].(map[string]any)
	result := []string{}
	for _, raw := range data["result"].([]any) {
		series := raw.(map[string]any)
		if data["resultType"] == "streams" {
			for _, rawValue := range series["values"].([]any) {
				value := rawValue.([]any)
				all := map[string]any{}
				for key, value := range series["stream"].(map[string]any) {
					if key != "fixture_run" {
						all[key] = value
					}
				}
				if len(value) == 3 {
					for _, category := range value[2].(map[string]any) {
						for key, value := range category.(map[string]any) {
							all[key] = value
						}
					}
				}
				encoded, _ := json.Marshal([]any{value[0], value[1], all})
				result = append(result, string(encoded))
			}
		} else {
			metric := series["metric"].(map[string]any)
			delete(metric, "fixture_run")
			points := []any{series["value"]}
			if values, ok := series["values"]; ok {
				points = values.([]any)
			}
			for _, point := range points {
				encoded, _ := json.Marshal([]any{metric, point})
				result = append(result, string(encoded))
			}
		}
	}
	sort.Strings(result)
	return result
}

func testDifferentialLoki(t *testing.T, table string, texts []string, wantError bool) {
	db, adapter := fixtureServerTable(t, table)
	readyDeadline := time.Now().Add(60 * time.Second)
	for {
		client := &http.Client{Timeout: 2 * time.Second}
		response, err := client.Get("http://127.0.0.1:13100/ready")
		ready := err == nil && response.StatusCode == http.StatusOK
		if err == nil {
			response.Body.Close()
		}
		if ready {
			break
		}
		if time.Now().After(readyDeadline) {
			t.Fatal("local synthetic Loki fixture did not become ready")
		}
		time.Sleep(250 * time.Millisecond)
	}
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano()
	selection, _ := query.Parse(`{detected_level=~".+"}`)
	predicate, _ := sqlgen.Selection(selection)
	run := strconv.FormatInt(time.Now().UnixNano(), 10)
	streams := map[string]map[string]any{}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := db.Scan(ctx, predicate, base-int64(time.Second), base+int64(time.Second), record.ScanOptions{Metadata: true}, func(row record.Raw) error {
		stream := map[string]string{"fixture_run": run}
		if row.Service != "" {
			stream["service_name"] = row.Service
		}
		metadata := row.Metadata()
		metadata["detected_level"] = row.Level
		key, _ := json.Marshal(stream)
		entry := streams[string(key)]
		if entry == nil {
			entry = map[string]any{"stream": stream, "values": [][]any{}}
			streams[string(key)] = entry
		}
		entry["values"] = append(entry["values"].([][]any), []any{strconv.FormatInt(row.Timestamp, 10), row.Body, metadata})
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	payloadStreams := []any{}
	for _, stream := range streams {
		payloadStreams = append(payloadStreams, stream)
	}
	payload, _ := json.Marshal(map[string]any{"streams": payloadStreams})
	response, err := http.Post("http://127.0.0.1:13100/loki/api/v1/push", "application/json", bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != 204 {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		t.Fatalf("fixture ingestion status %d: %s", response.StatusCode, body)
	}
	for _, text := range texts {
		t.Run(text, func(t *testing.T) {
			expr, err := syntax.ParseExpr(text)
			if err != nil {
				t.Fatal(err)
			}
			// Modify the AST, never extract/rewrite executable LogQL with regex.
			expr.Walk(func(node syntax.Expr) bool {
				if matchers, ok := node.(*syntax.MatchersExpr); ok {
					matchers.AppendMatchers([]*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "fixture_run", run)})
				}
				return true
			})
			params := url.Values{"query": {text}, "start": {strconv.FormatInt(base-1, 10)}, "end": {strconv.FormatInt(base+int64(time.Minute), 10)}, "step": {"15"}}
			plan, _ := query.ParseQuery(text)
			if plan.Metric != nil {
				params.Set("start", strconv.FormatInt(base, 10))
			}
			for _, categorized := range []bool{false, true} {
				adapterStatus, actual := getJSON(t, adapter.URL+"/loki/api/v1/query_range", params, categorized)
				params.Set("query", expr.String())
				lokiStatus, expected := getJSON(t, "http://127.0.0.1:13100/loki/api/v1/query_range", params, categorized)
				params.Set("query", text)
				if wantError {
					if adapterStatus == 200 || lokiStatus == 200 {
						t.Fatalf("expected metric errors: adapter=%d %v Loki=%d %v", adapterStatus, actual, lokiStatus, expected)
					}
					continue
				}
				if adapterStatus != 200 || lokiStatus != 200 {
					t.Fatalf("adapter=%d %v; Loki=%d %v", adapterStatus, actual, lokiStatus, expected)
				}
				got, want := canonical(t, actual), canonical(t, expected)
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("categorized=%v\nadapter=%s\nLoki=%s", categorized, fmt.Sprint(got), fmt.Sprint(want))
				}
			}
		})
	}
}

func TestDifferentialLoki(t *testing.T) {
	testDifferentialLoki(t, "otel_logs_unique", []string{
		"{service_name=`fixture-api`}",
		`{service_name="fixture-api"} |= "duplicate"`,
		`{service_name=~"fixture-.+",service_name!="fixture-worker"} != "duplicate"`,
		`{service_name="fixture-api"} |~ "(?i)error"`,
		`{service_name="fixture-api"} !~ "(?i)error"`,
		`{service_name="fixture-api"} |~ "^a.b$"`,
		`{service_name="fixture-api"} |~ "(?s)^a.b$"`,
		`{service_name="fixture-api"} | json | __error__=""`,
		`{service_name="fixture-api"} | json | logfmt | drop __error__, __error_details__`,
		`{service_name="fixture-api"} | logfmt | duration > 10 | __error__=""`,
		`{service_name="fixture-api"} | detected_level="error"`,
		`{service_name="fixture-api"} | missing=""`,
		`{service_name="fixture-api"} |= "ERROR" or "hello"`,
		`{service_name="fixture-api"} |> "<_> café <_>"`,
		`sum by (detected_level) (count_over_time({service_name="fixture-api"} | json | logfmt | drop __error__, __error_details__ [30s]))`,
		`sum(rate({service_name=~"fixture-.+"}[30s]))`,
		`sum(bytes_over_time({service_name="fixture-api"}[30s]))`,
		`avg_over_time({service_name="fixture-api"} | logfmt | duration!="" | unwrap duration | __error__="" [30s]) by ()`,
		`sum_over_time({service_name="fixture-api"} | json | unwrap duration | __error__="" [30s])`,
		`avg_over_time({service_name="fixture-api"} | json | logfmt | drop __error__, __error_details__ | duration!="" | unwrap duration | __error__="" [30s]) by (detected_level)`,
		`avg_over_time({service_name="fixture-api"} | logfmt | duration!="" | unwrap duration(duration) | __error__="" [30s]) by ()`,
	}, false)
}

func TestDifferentialGuardedParsers(t *testing.T) {
	for _, function := range []string{"count_over_time", "rate", "bytes_over_time", "bytes_rate"} {
		testDifferentialLoki(t, "otel_logs_parser_guards", []string{
			`sum by (service_name,detected_level)(` + function + `({service_name="fixture-guard-safe"} | json | logfmt | drop __error__, __error_details__ [30s]))`,
		}, false)
	}
	for _, service := range []string{"reserved", "sanitized", "nested", "escaped"} {
		testDifferentialLoki(t, "otel_logs_parser_guards", []string{
			`sum(count_over_time({service_name="fixture-guard-` + service + `"} | json | logfmt | drop __error__, __error_details__ [30s]))`,
		}, false)
		testDifferentialLoki(t, "otel_logs_parser_guards", []string{
			`count_over_time({service_name="fixture-guard-` + service + `"} | json | logfmt | drop __error__, __error_details__ [30s])`,
		}, true)
	}
}

// regressionFixture creates only synthetic rows in the fixed local fixture.
// Each run owns its table and grant; existing fixture data is never modified.
func regressionFixture(t *testing.T, rows []record.Raw) string {
	t.Helper()
	conn, err := clickhouse.Open(&clickhouse.Options{Addr: []string{"127.0.0.1:19000"}, Auth: clickhouse.Auth{Database: "fixture", Username: "fixture_admin", Password: "fixture-admin-not-production"}, DialTimeout: 3 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	ctx := context.Background()
	table := "otel_logs_regression_" + strconv.FormatInt(time.Now().UnixNano(), 10)
	if err := conn.Exec(ctx, "CREATE TABLE fixture."+table+" AS fixture.otel_logs ENGINE=Memory"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := conn.Exec(ctx, "DROP TABLE fixture."+table); err != nil {
			t.Error(err)
		}
		if err := conn.Exec(ctx, "REVOKE SELECT ON fixture."+table+" FROM fixture_reader"); err != nil {
			t.Error(err)
		}
	})
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO fixture."+table)
	if err != nil {
		t.Fatal(err)
	}
	for i, row := range rows {
		timestamp := time.Date(2026, 1, 1, 0, 0, 0, (i+1)*int(time.Millisecond), time.UTC)
		if err := batch.Append(timestamp, row.Body, row.Service, "info", uint8(9), row.LogAttributes, map[string]string{}, "", ""); err != nil {
			batch.Abort()
			t.Fatal(err)
		}
	}
	if err := batch.Send(); err != nil {
		t.Fatal(err)
	}
	if err := conn.Exec(ctx, "GRANT SELECT ON fixture."+table+" TO fixture_reader"); err != nil {
		t.Fatal(err)
	}
	return table
}

func TestDifferentialRegexResidualRegressions(t *testing.T) {
	var rows []record.Raw
	for _, value := range []string{"xxabxx", "K", "K", "k", "ſ", "S", "s", "\n", "a\nb", ""} {
		rows = append(rows, record.Raw{Body: value, Service: "regex-api", LogAttributes: map[string]string{"x": value}})
	}
	rows = append(rows, record.Raw{Body: "K", Service: "regex-kelvin"}, record.Raw{Body: "\n", Service: "regex-newline"})
	texts := []string{
		`{service_name="regex-api"} | log_x=~"a(b|cd)"`,
		`{service_name="regex-api"} | log_x!~"a(b|cd)"`,
		`{service_name="regex-api"} | log_x=~"(?i)k"`,
		`{service_name="regex-api"} | log_x!~"(?i)k"`,
		`{service_name="regex-api"} | log_x!~"(?i)s"`,
	}
	for _, filter := range []string{`|~ "(?i)k"`, `!~ "(?i)k"`, `|~ "(?i)s"`, `!~ "(?i)s"`, `|~ ".+"`, `!~ ".+"`, `|~ "a(b|cd)"`, `|~ "(?s)a.b"`} {
		texts = append(texts, `{service_name="regex-api"} `+filter)
		for _, parsers := range []string{"", ` | json | logfmt | drop __error__, __error_details__`} {
			texts = append(texts, `sum(count_over_time({service_name="regex-api"} `+filter+parsers+` [1m]))`)
		}
	}
	for _, parsers := range []string{"", ` | json | logfmt | drop __error__, __error_details__`} {
		texts = append(texts, `sum(count_over_time({service_name="regex-kelvin"} |~ "(?i)k"`+parsers+` [1m]))`, `sum(count_over_time({service_name="regex-newline"} !~ ".+"`+parsers+` [1m]))`)
	}
	testDifferentialLoki(t, regressionFixture(t, rows), texts, false)
}

func TestDifferentialUnwrapArithmeticRegressions(t *testing.T) {
	var rows []record.Raw
	var texts []string
	for name, values := range map[string][]string{
		"overflow": {"1e308", "1e308"}, "rounding": {"1", "2", "2"}, "positive-inf": {"+Inf", "+Inf", "1"},
		"negative-inf": {"-Inf", "-Inf", "1"}, "opposite-inf": {"+Inf", "-Inf"}, "inf-nan": {"+Inf", "NaN"},
		"nan-inf": {"NaN", "+Inf"}, "cancellation": {"1e16", "1", "-1e16"}, "conversion": {"2", "4", "oops", ""},
	} {
		for _, value := range values {
			rows = append(rows, record.Raw{Body: "v=" + value, Service: "avg-" + name})
		}
		for _, form := range []struct{ function, grouping string }{{"avg_over_time", ""}, {"avg_over_time", " by ()"}, {"avg_over_time", " without ()"}, {"avg_over_time", " by (detected_level)"}, {"sum_over_time", ""}} {
			texts = append(texts, form.function+`({service_name="avg-`+name+`"} | logfmt | unwrap v | __error__="" [1m])`+form.grouping)
		}
		texts = append(texts, `avg_over_time({service_name="avg-`+name+`"} | logfmt | drop unused | unwrap v | __error__="" [1m])`)
	}
	testDifferentialLoki(t, regressionFixture(t, rows), texts, false)
}
