// SPDX-License-Identifier: AGPL-3.0-only
// bench measures Grafana Logs Drilldown requests against synthetic local data.
package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"grafana-clickhouse-drilldown/internal/record"
)

type config struct {
	target, listen, native, adminURL, reader, binary, output, dataset, sourceStat, ranges, shapes string
	pid                                                                                           int
	cgroup                                                                                        string
	timeout                                                                                       time.Duration
}
type request struct {
	Name, Scope, Range, Path, Query, Step string
	Start, End                            time.Time
}
type resultSize struct {
	Rows    int `json:"rows"`
	Series  int `json:"series"`
	Samples int `json:"samples"`
	Items   int `json:"items"`
	Bytes   int `json:"bytes"`
}
type queryStats struct {
	Queries    uint64   `json:"queries"`
	ReadRows   uint64   `json:"read_rows"`
	ReadBytes  uint64   `json:"read_bytes"`
	Memory     uint64   `json:"peak_query_memory_bytes"`
	DurationMS uint64   `json:"sum_query_duration_ms"`
	Codes      []uint64 `json:"exception_codes,omitempty"`
	Exceptions []string `json:"exceptions,omitempty"`
	Error      string   `json:"collection_error,omitempty"`
}
type measurement struct {
	Name      string     `json:"name"`
	Scope     string     `json:"scope"`
	Range     string     `json:"range"`
	Path      string     `json:"path"`
	Query     string     `json:"query"`
	Step      string     `json:"step"`
	Start     time.Time  `json:"start"`
	End       time.Time  `json:"end"`
	HTTP      int        `json:"http_status"`
	Class     string     `json:"error_class"`
	Error     string     `json:"error,omitempty"`
	LatencyMS float64    `json:"latency_ms"`
	Size      resultSize `json:"result"`
	RSS       uint64     `json:"adapter_process_lifetime_peak_rss_bytes"`
	CH        queryStats `json:"clickhouse"`
}
type report struct {
	Started           time.Time       `json:"started"`
	Finished          time.Time       `json:"finished"`
	Reader            string          `json:"reader"`
	Target            string          `json:"target"`
	BinarySHA         string          `json:"adapter_binary_sha256,omitempty"`
	GitHead           string          `json:"git_head"`
	DiffStat          string          `json:"git_diff_stat_start"`
	DiffStatEnd       string          `json:"git_diff_stat_end"`
	BuildDiffStat     string          `json:"build_diff_stat,omitempty"`
	Dataset           json.RawMessage `json:"dataset"`
	Notes             []string        `json:"notes"`
	Results           []measurement   `json:"results"`
	ReadinessError    string          `json:"readiness_error,omitempty"`
	ReaderMaxReadRows uint64          `json:"reader_max_rows_to_read"`
	GitStatus         string          `json:"git_status_start"`
}
type admin struct {
	endpoint string
	client   *http.Client
}

func main() {
	var c config
	flag.StringVar(&c.target, "target", "", "target an existing local adapter instead of launching one")
	flag.StringVar(&c.listen, "listen", "127.0.0.1:29100", "adapter listen address when launched")
	flag.StringVar(&c.native, "clickhouse-address", "127.0.0.1:29000", "local ClickHouse native address")
	flag.StringVar(&c.adminURL, "admin-url", "http://127.0.0.1:28123", "local benchmark ClickHouse HTTP URL")
	flag.StringVar(&c.reader, "reader", "bench_reader", "bench_reader or bench_reader_wide")
	flag.StringVar(&c.binary, "adapter-binary", "", "launch this binary; otherwise build ./cmd/adapter")
	flag.StringVar(&c.output, "output", "testdata/bench/results/baseline", "report prefix, writes .json and .md")
	flag.StringVar(&c.dataset, "dataset", "testdata/bench/results/dataset.json", "generator manifest, defines anchored ranges")
	flag.StringVar(&c.sourceStat, "source-diffstat", "", "optional diffstat saved when the binary was built")
	flag.StringVar(&c.ranges, "ranges", "15m,1h,6h,24h,7d", "comma-separated matrix ranges")
	flag.StringVar(&c.shapes, "shapes", "", "optional comma-separated shape names")
	flag.IntVar(&c.pid, "pid", 0, "existing adapter PID for /proc VmHWM")
	flag.StringVar(&c.cgroup, "cgroup-memory-peak", "", "existing container memory.peak file, instead of PID")
	flag.DurationVar(&c.timeout, "timeout", 25*time.Second, "per HTTP request timeout")
	flag.Parse()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, c); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func localAddress(address string) error {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return err
	}
	ip := net.ParseIP(host)
	if ip == nil || !ip.IsLoopback() {
		return fmt.Errorf("only numeric loopback addresses allowed: %s", address)
	}
	return nil
}
func localURL(raw string) error {
	u, err := url.Parse(raw)
	if err != nil {
		return err
	}
	if u.Scheme != "http" || u.User != nil || u.RawQuery != "" || (u.Path != "" && u.Path != "/") {
		return errors.New("need a plain loopback HTTP URL")
	}
	address := u.Host
	if u.Port() == "" {
		address = net.JoinHostPort(u.Hostname(), "80")
	}
	return localAddress(address)
}
func (a admin) sql(ctx context.Context, sql string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, "POST", strings.TrimRight(a.endpoint, "/")+"/", strings.NewReader(sql))
	if err != nil {
		return nil, err
	}
	req.SetBasicAuth("bench_admin", "bench-admin-not-production")
	resp, err := a.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("admin query HTTP %d: %s", resp.StatusCode, b)
	}
	return b, nil
}
func commandText(args ...string) string {
	b, err := exec.Command(args[0], args[1:]...).Output()
	if err != nil {
		return err.Error()
	}
	return strings.TrimSpace(string(b))
}

func run(ctx context.Context, c config) error {
	if c.reader != "bench_reader" && c.reader != "bench_reader_wide" {
		return errors.New("only the two public synthetic bench readers are allowed")
	}
	for _, addr := range []string{c.listen, c.native} {
		if err := localAddress(addr); err != nil {
			return err
		}
	}
	if err := localURL(c.adminURL); err != nil {
		return err
	}
	if c.target != "" {
		if err := localURL(c.target); err != nil {
			return err
		}
	}
	if c.timeout <= 0 {
		return errors.New("timeout must be positive")
	}
	data, err := os.ReadFile(c.dataset)
	if err != nil {
		return err
	}
	var dataset struct{ Start, End string }
	if err = json.Unmarshal(data, &dataset); err != nil {
		return err
	}
	start, err := time.Parse(time.RFC3339Nano, dataset.Start)
	if err != nil {
		return err
	}
	end, err := time.Parse(time.RFC3339Nano, dataset.End)
	if err != nil {
		return err
	}
	requests, err := matrix(start, end, c.ranges, c.shapes)
	if err != nil {
		return err
	}
	if err = os.MkdirAll(filepath.Dir(c.output), 0755); err != nil {
		return err
	}
	r := report{Results: []measurement{}, Started: time.Now().UTC(), Reader: c.reader, Dataset: data, GitHead: commandText("git", "rev-parse", "HEAD"), DiffStat: commandText("git", "diff", "--stat")}
	r.ReaderMaxReadRows = 2000000
	if c.reader == "bench_reader_wide" {
		r.ReaderMaxReadRows = 200000000
	}
	r.GitStatus = commandText("git", "status", "--short")
	if c.sourceStat != "" {
		b, e := os.ReadFile(c.sourceStat)
		if e != nil {
			return e
		}
		r.BuildDiffStat = string(b)
	}
	r.Notes = []string{
		"Synthetic local data only. One request at a time. No production credentials or data.",
		"A host-launched adapter has no production cgroup CPU or memory cap. The bench ClickHouse service is capped at two CPUs and 3 GiB.",
		"Adapter peak is the process-lifetime high-water mark after each request, not a per-request allocation measurement. Zero means unavailable.",
		"ClickHouse stats sum terminal QueryFinish/Exception rows for this reader within server-clock boundaries; query memory is the maximum across those rows. No QueryStart double counting.",
		"The benchmark must be the only client using the selected bench reader. Readiness is outside query measurement intervals.",
		"No cache flushing: results reflect normal warm/cold and background merge effects in matrix order. HTTP status and empty result counts are preserved without assuming success means correctness.",
	}
	if c.target == "" {
		if c.binary == "" {
			c.binary = c.output + ".adapter"
			build := exec.CommandContext(ctx, "go", "build", "-o", c.binary, "./cmd/adapter")
			build.Stdout = os.Stderr
			build.Stderr = os.Stderr
			if err = build.Run(); err != nil {
				return fmt.Errorf("build adapter: %w", err)
			}
		}
		blob, e := os.ReadFile(c.binary)
		if e != nil {
			return e
		}
		sum := sha256.Sum256(blob)
		r.BinarySHA = hex.EncodeToString(sum[:])
		c.target = "http://" + c.listen
		log, e := os.Create(c.output + ".adapter.log")
		if e != nil {
			return e
		}
		defer log.Close()
		cmd := exec.Command(c.binary)
		// Strip inherited credentials/settings before supplying the fixed synthetic target.
		for _, v := range os.Environ() {
			k, _, _ := strings.Cut(v, "=")
			if !strings.HasPrefix(k, "CLICKHOUSE_") && !strings.HasPrefix(k, "ADAPTER_") && k != "SOURCE_PATH" && k != "SOURCE_OFFER_URL" {
				cmd.Env = append(cmd.Env, v)
			}
		}
		cmd.Env = append(cmd.Env, "CLICKHOUSE_ADDRESS="+c.native, "CLICKHOUSE_DATABASE=bench", "CLICKHOUSE_TABLE=otel_logs", "CLICKHOUSE_USER="+c.reader, "CLICKHOUSE_PASSWORD=bench-reader-not-production", "ADAPTER_LISTEN="+c.listen, "ADAPTER_MAX_READ_ROWS="+strconv.FormatUint(r.ReaderMaxReadRows, 10))
		cmd.Stdout = log
		cmd.Stderr = log
		if err = cmd.Start(); err != nil {
			return err
		}
		c.pid = cmd.Process.Pid
		done := make(chan error, 1)
		go func() { done <- cmd.Wait() }()
		defer func() {
			_ = cmd.Process.Signal(syscall.SIGTERM)
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				_ = cmd.Process.Kill()
				<-done
			}
		}()
	}
	r.Target = c.target
	client := &http.Client{Timeout: c.timeout}
	if err = ready(ctx, client, c.target); err != nil {
		r.ReadinessError = err.Error()
		r.Notes = append(r.Notes, "No matrix ran because readiness failed. Old adapter binaries hardcode max_rows_to_read=2000000; readonly=1 rejects their connection settings against the wider reader. Use a configurable build with matching ADAPTER_MAX_READ_ROWS.")
		r.Finished = time.Now().UTC()
		r.DiffStatEnd = commandText("git", "diff", "--stat")
		if e := save(c.output, r); e != nil {
			return e
		}
		return err
	}
	a := admin{c.adminURL, &http.Client{Timeout: 30 * time.Second}}
	// Check the database identity and query-log availability before running the matrix.
	if _, err = a.sql(ctx, "SELECT count() FROM bench.otel_logs"); err != nil {
		return err
	}
	if _, err = a.sql(ctx, "SYSTEM FLUSH LOGS"); err != nil {
		return err
	}
	for i, q := range requests {
		if err = ctx.Err(); err != nil {
			return err
		}
		m, e := measure(ctx, client, a, c, q)
		if e != nil {
			return e
		}
		r.Results = append(r.Results, m)
		r.Finished = time.Now().UTC()
		r.DiffStatEnd = commandText("git", "diff", "--stat")
		if err = save(c.output, r); err != nil {
			return err
		}
		fmt.Fprintf(os.Stderr, "%d/%d %-10s %-8s %-18s HTTP %d %-18s %8.1f ms CH rows %d peak %.1f MiB\n", i+1, len(requests), q.Range, q.Scope, q.Name, m.HTTP, m.Class, m.LatencyMS, m.CH.ReadRows, float64(m.RSS)/(1<<20))
	}
	fmt.Print(markdown(r))
	return nil
}

func ready(ctx context.Context, client *http.Client, target string) error {
	deadline := time.NewTimer(30 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		req, _ := http.NewRequestWithContext(ctx, "GET", target+"/ready", nil)
		resp, err := client.Do(req)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == 200 {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			return errors.New("adapter /ready did not become healthy in 30s")
		case <-ticker.C:
		}
	}
}

func niceStep(span time.Duration) string {
	seconds := math.Ceil(span.Seconds() / 500)
	for _, n := range []int{1, 2, 5, 10, 15, 30, 60, 120, 300, 600, 900, 1800, 3600, 7200, 21600, 43200, 86400} {
		if float64(n) >= seconds {
			return strconv.Itoa(n) + "s"
		}
	}
	return strconv.FormatInt(int64(seconds), 10) + "s"
}
func matrix(dataStart, end time.Time, ranges, shapes string) ([]request, error) {
	wanted := map[string]bool{}
	if shapes != "" {
		for _, s := range strings.Split(shapes, ",") {
			wanted[s] = true
		}
	}
	var out []request
	for _, label := range strings.Split(ranges, ",") {
		value := label
		if strings.HasSuffix(value, "d") {
			n, e := strconv.Atoi(strings.TrimSuffix(value, "d"))
			if e != nil {
				return nil, e
			}
			value = strconv.Itoa(n*24) + "h"
		}
		span, e := time.ParseDuration(value)
		if e != nil || span <= 0 {
			return nil, fmt.Errorf("invalid range %q", label)
		}
		from := end.Add(-span)
		if from.Before(dataStart) {
			fmt.Fprintln(os.Stderr, "skip", label, "outside dataset coverage")
			continue
		}
		step := niceStep(span)
		scopes := []struct{ name, selector string }{{"hot", `{service_name="edge-gateway"}`}, {"medium", `{service_name="checkout"}`}, {"tail", `{service_name="archive-sweeper"}`}, {"overview", "{service_name=~`.+`}"}}
		for _, s := range scopes {
			pipeline := s.selector + ` | json | logfmt | drop __error__, __error_details__`
			definitions := []struct{ name, path, query string }{
				{"detected_labels", "detected_labels", s.selector},
				{"logs", "query_range", pipeline},
				{"level_count", "query_range", `sum by (detected_level) (count_over_time(` + s.selector + ` [` + step + `]))`},
				{"parsed_count", "query_range", `sum(count_over_time(` + pipeline + ` [` + step + `]))`},
				{"metadata_filter", "query_range", s.selector + ` | ` + record.Alias("log", "http.route") + `="/api/v1/orders"`},
				{"parsed_filter", "query_range", pipeline + ` | endpoint="/api/v1/orders"`},
				{"detected_fields", "detected_fields", pipeline},
				{"patterns", "patterns", s.selector},
				{"unwrap", "query_range", `avg_over_time(` + s.selector + ` | logfmt | duration!="" | unwrap duration(duration) | __error__="" [` + step + `]) by ()`},
			}
			if s.name == "overview" {
				definitions = append(definitions, struct{ name, path, query string }{"volume", "index/volume", s.selector})
			}
			for _, d := range definitions {
				if len(wanted) > 0 && !wanted[d.name] {
					continue
				}
				out = append(out, request{d.name, s.name, label, d.path, d.query, step, from, end})
			}
		}
	}
	if len(out) == 0 {
		return nil, errors.New("no matching queries/ranges")
	}
	return out, nil
}
func params(q request) url.Values {
	v := url.Values{"query": {q.Query}, "start": {strconv.FormatInt(q.Start.UnixNano(), 10)}, "end": {strconv.FormatInt(q.End.UnixNano(), 10)}}
	switch q.Path {
	case "query_range":
		v.Set("step", q.Step)
		if q.Name == "logs" || q.Name == "metadata_filter" || q.Name == "parsed_filter" {
			v.Set("limit", "100")
			v.Set("direction", "backward")
		}
	case "patterns":
		v.Set("step", q.Step)
	case "detected_fields":
		v.Set("step", q.Step)
		v.Set("limit", "100")
		v.Set("line_limit", "1000")
	case "index/volume":
		v.Set("limit", "100")
	}
	return v
}

func serverTime(ctx context.Context, a admin) (string, error) {
	b, e := a.sql(ctx, "SELECT toString(now64(6, 'UTC'))")
	return strings.TrimSpace(string(b)), e
}
func measure(ctx context.Context, client *http.Client, a admin, c config, q request) (measurement, error) {
	m := measurement{Name: q.Name, Scope: q.Scope, Range: q.Range, Path: q.Path, Query: q.Query, Step: q.Step, Start: q.Start, End: q.End}
	begin, err := serverTime(ctx, a)
	if err != nil {
		return m, err
	}
	req, err := http.NewRequestWithContext(ctx, "GET", c.target+"/loki/api/v1/"+q.Path+"?"+params(q).Encode(), nil)
	if err != nil {
		return m, err
	}
	now := time.Now()
	resp, err := client.Do(req)
	var body []byte
	if err == nil {
		m.HTTP = resp.StatusCode
		body, err = io.ReadAll(io.LimitReader(resp.Body, (8<<20)+1))
		resp.Body.Close()
		if len(body) > 8<<20 {
			err = errors.New("benchmark response exceeds 8 MiB capture limit")
		}
	}
	m.LatencyMS = float64(time.Since(now).Microseconds()) / 1000
	m.Size = resultSize{Bytes: len(body)}
	if err != nil {
		m.Error = err.Error()
		m.Class = "transport"
	} else {
		m.Size, m.Error = parseResponse(body, m.HTTP)
		m.Class = classify(m.HTTP, m.Error)
	}
	m.RSS = peakRSS(c.pid, c.cgroup)
	finish, e := serverTime(ctx, a)
	if e != nil {
		m.CH.Error = e.Error()
		return m, nil
	}
	m.CH = collect(ctx, a, c.reader, begin, finish)
	if m.Class == "scan_rows" || m.Class == "other" {
		for _, message := range m.CH.Exceptions {
			if class := classify(m.HTTP, message); class != "other" {
				m.Class = class
				break
			}
		}
	}
	return m, nil
}

func collect(ctx context.Context, a admin, user, begin, end string) queryStats {
	var result queryStats
	// A timed-out HTTP request can return before native cancellation finishes.
	// Wait outside measured HTTP latency, then flush its terminal query event.
	until := time.Now().Add(2 * time.Second)
	for {
		active, err := a.sql(ctx, fmt.Sprintf("SELECT count() FROM system.processes WHERE user='%s'", user))
		if err != nil {
			result.Error = err.Error()
			return result
		}
		if strings.TrimSpace(string(active)) == "0" {
			break
		}
		if time.Now().After(until) {
			result.Error = "reader still active after cancellation wait; costs may be incomplete"
			break
		}
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			result.Error = ctx.Err().Error()
			return result
		case <-timer.C:
		}
	}
	if _, err := a.sql(ctx, "SYSTEM FLUSH LOGS"); err != nil {
		result.Error = err.Error()
		return result
	}
	// These strings are generated by ClickHouse, and user is a fixed enum validated above.
	query := fmt.Sprintf(`SELECT count() AS queries, sum(read_rows) AS read_rows, sum(read_bytes) AS read_bytes,
 max(memory_usage) AS memory, sum(query_duration_ms) AS duration_ms,
 groupUniqArrayIf(toUInt64(exception_code), exception_code != 0) AS codes,
 groupUniqArrayIf(substring(exception,1,700), exception_code != 0) AS exceptions
 FROM system.query_log WHERE user = '%s' AND type IN ('QueryFinish','ExceptionBeforeStart','ExceptionWhileProcessing')
 AND query_start_time_microseconds >= toDateTime64('%s',6,'UTC')
 AND query_start_time_microseconds < toDateTime64('%s',6,'UTC') FORMAT JSONEachRow`, user, begin, end)
	b, err := a.sql(ctx, query)
	if err != nil {
		result.Error = err.Error()
		return result
	}
	var row struct {
		Queries    json.Number `json:"queries"`
		ReadRows   json.Number `json:"read_rows"`
		ReadBytes  json.Number `json:"read_bytes"`
		Memory     json.Number `json:"memory"`
		Duration   json.Number `json:"duration_ms"`
		Codes      []uint64    `json:"codes"`
		Exceptions []string    `json:"exceptions"`
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	if err = dec.Decode(&row); err != nil {
		result.Error = err.Error()
		return result
	}
	parse := func(n json.Number) uint64 { v, _ := strconv.ParseUint(n.String(), 10, 64); return v }
	result.Queries = parse(row.Queries)
	result.ReadRows = parse(row.ReadRows)
	result.ReadBytes = parse(row.ReadBytes)
	result.Memory = parse(row.Memory)
	result.DurationMS = parse(row.Duration)
	result.Codes = row.Codes
	result.Exceptions = row.Exceptions
	return result
}
func parseResponse(body []byte, status int) (resultSize, string) {
	size := resultSize{Bytes: len(body)}
	var root map[string]any
	if err := json.Unmarshal(body, &root); err != nil {
		return size, strings.TrimSpace(string(body))
	}
	if message, ok := root["error"].(string); ok && message != "" {
		return size, message
	}
	if status < 200 || status >= 300 {
		return size, strings.TrimSpace(string(body))
	}
	for _, key := range []string{"detectedLabels", "fields", "values"} {
		if values, ok := root[key].([]any); ok {
			size.Items = len(values)
			return size, ""
		}
	}
	switch data := root["data"].(type) {
	case map[string]any:
		entries, _ := data["result"].([]any)
		size.Series = len(entries)
		kind, _ := data["resultType"].(string)
		for _, entry := range entries {
			item, ok := entry.(map[string]any)
			if !ok {
				continue
			}
			values, _ := item["values"].([]any)
			if kind == "streams" {
				size.Rows += len(values)
			} else {
				size.Samples += len(values)
				if _, ok := item["value"]; ok {
					size.Samples++
				}
			}
		}
	case []any:
		size.Items = len(data)
		for _, entry := range data {
			if item, ok := entry.(map[string]any); ok {
				if samples, ok := item["samples"].([]any); ok {
					size.Series++
					size.Samples += len(samples)
				}
			}
		}
	}
	return size, ""
}
func classify(status int, message string) string {
	if status >= 200 && status < 300 && message == "" {
		return "ok"
	}
	text := strings.ToLower(message)
	for _, pair := range [][2]string{
		{"max_rows_to_read", "read_rows"}, {"rows to read", "read_rows"},
		{"memory limit", "memory"}, {"too many bytes", "result_bytes"},
		{"result bytes", "result_bytes"}, {"result rows", "result_rows"},
		{"bounded scan budget", "scan_rows"}, {"scan limit", "scan_rows"},
		{"200000", "scan_rows"}, {"bounded memory budget", "retained_bytes"},
		{"retained", "retained_bytes"}, {"too many rows", "row_limit"},
		{"deadline", "timeout"}, {"timeout", "timeout"}, {"time limit", "timeout"},
		{"too many series", "series_limit"}, {"too many metric", "series_limit"},
		{"unsupported", "unsupported"}, {"parse", "parse"}, {"overload", "overload"},
		{"cost", "cost_limit"}, {"time range", "time_range"},
		{"maximum range", "time_range"}, {"maximum query range", "time_range"},
		{"range must", "time_range"}, {"range exceeds", "time_range"},
	} {
		if strings.Contains(text, pair[0]) {
			return pair[1]
		}
	}
	return "other"
}
func peakRSS(pid int, cgroup string) uint64 {
	if cgroup != "" {
		b, err := os.ReadFile(cgroup)
		if err == nil {
			v, _ := strconv.ParseUint(strings.TrimSpace(string(b)), 10, 64)
			return v
		}
		return 0
	}
	if pid <= 0 {
		return 0
	}
	f, err := os.Open(fmt.Sprintf("/proc/%d/status", pid))
	if err != nil {
		return 0
	}
	defer f.Close()
	s := bufio.NewScanner(f)
	for s.Scan() {
		fields := strings.Fields(s.Text())
		if len(fields) >= 2 && fields[0] == "VmHWM:" {
			n, _ := strconv.ParseUint(fields[1], 10, 64)
			return n * 1024
		}
	}
	return 0
}
func save(prefix string, r report) error {
	b, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err
	}
	// Atomic checkpoint after every request preserves a usable partial run on interruption.
	for ext, body := range map[string][]byte{".json": append(b, '\n'), ".md": []byte(markdown(r))} {
		name := prefix + ext
		if err = os.WriteFile(name+".tmp", body, 0644); err != nil {
			return err
		}
		if err = os.Rename(name+".tmp", name); err != nil {
			return err
		}
	}
	return nil
}
func markdown(r report) string {
	var b strings.Builder
	fmt.Fprintf(&b, "# Local synthetic benchmark\n\nReader `%s`, %d requests, binary SHA-256 `%s`.\n\n", r.Reader, len(r.Results), r.BinarySHA)
	if r.ReadinessError != "" {
		fmt.Fprintf(&b, "No matrix ran: %s. See JSON notes for startup diagnostics.\n\n", r.ReadinessError)
	}
	b.WriteString("RSS is the adapter process-lifetime high-water mark. ClickHouse memory is the largest query peak in each group. Latency excludes query-log collection.\n\n")
	b.WriteString("| Range | Scope | OK / total | Error classes | Median / max ms | CH read rows | CH peak MiB | Adapter peak MiB |\n|---|---|---:|---|---:|---:|---:|---:|\n")
	type group struct {
		key     string
		results []measurement
	}
	var groups []group
	for _, m := range r.Results {
		key := m.Range + "|" + m.Scope
		index := -1
		for i, g := range groups {
			if g.key == key {
				index = i
				break
			}
		}
		if index < 0 {
			groups = append(groups, group{key: key})
			index = len(groups) - 1
		}
		groups[index].results = append(groups[index].results, m)
	}
	for _, g := range groups {
		ok := 0
		classes := map[string]int{}
		var latencies []float64
		var rows, mem, rss uint64
		for _, m := range g.results {
			if m.Class == "ok" {
				ok++
			} else {
				classes[m.Class]++
			}
			latencies = append(latencies, m.LatencyMS)
			rows += m.CH.ReadRows
			mem = max(mem, m.CH.Memory)
			rss = max(rss, m.RSS)
		}
		sort.Float64s(latencies)
		keys := make([]string, 0, len(classes))
		for k := range classes {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		var failures []string
		for _, k := range keys {
			failures = append(failures, fmt.Sprintf("%s:%d", k, classes[k]))
		}
		parts := strings.Split(g.key, "|")
		fmt.Fprintf(&b, "| %s | %s | %d / %d | %s | %.1f / %.1f | %d | %.1f | %.1f |\n", parts[0], parts[1], ok, len(g.results), strings.Join(failures, ", "), latencies[len(latencies)/2], latencies[len(latencies)-1], rows, float64(mem)/(1<<20), float64(rss)/(1<<20))
	}
	b.WriteString("\nFull per-request queries, status, errors, sizes, timings, read bytes, query counts and provenance are in the matching JSON report.\n")
	return b.String()
}
