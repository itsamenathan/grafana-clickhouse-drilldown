// SPDX-License-Identifier: AGPL-3.0-only
package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/maphash"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"grafana-clickhouse-drilldown/internal/engine"
	"grafana-clickhouse-drilldown/internal/limits"
	"grafana-clickhouse-drilldown/internal/query"
	"grafana-clickhouse-drilldown/internal/record"
	"grafana-clickhouse-drilldown/internal/sqlgen"
)

// memoryStore serves ascending rows within [start,end) and ignores the SQL
// predicate; tests use selectors that match every row.
type memoryStore struct {
	mu            sync.Mutex
	rows          []record.Raw
	beforeScan    func(int64, int64, record.ScanOptions) error
	afterSnapshot func(record.ScanOptions)
}

func (*memoryStore) Ready(context.Context) error { return nil }

func (s *memoryStore) add(rows ...record.Raw) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rows = append(s.rows, rows...)
	slices.SortStableFunc(s.rows, func(a, b record.Raw) int { return int(a.Timestamp - b.Timestamp) })
}

func (s *memoryStore) Scan(ctx context.Context, _ sqlgen.Predicate, start, end int64, options record.ScanOptions, visit func(record.Raw) error) error {
	if s.beforeScan != nil {
		if err := s.beforeScan(start, end, options); err != nil {
			return err
		}
	}
	s.mu.Lock()
	rows := slices.Clone(s.rows)
	s.mu.Unlock()
	if s.afterSnapshot != nil {
		s.afterSnapshot(options)
	}
	if options.Backward {
		slices.Reverse(rows)
	}
	sent := 0
	for _, row := range rows {
		if row.Timestamp < start || row.Timestamp >= end || options.Limit > 0 && sent >= options.Limit {
			continue
		}
		sent++
		if err := visit(row); err == record.ErrStop {
			return nil
		} else if err != nil {
			return err
		}
	}
	return nil
}

func row(timestamp int64, body string) record.Raw {
	return record.Raw{Timestamp: timestamp, Body: body, Service: "api", Level: "info"}
}

func dialTail(t *testing.T, server *httptest.Server, header http.Header) (*websocket.Conn, *http.Response, error) {
	t.Helper()
	target := "ws" + strings.TrimPrefix(server.URL, "http") + tailPath + "?" + url.Values{"query": {`{service_name="api"}`}, "limit": {"2"}}.Encode()
	return websocket.DefaultDialer.Dial(target, header)
}

// messages streams tailed bodies; gorilla connections cannot be read again
// after a read deadline expires, so a single reader feeds the channel.
func messages(t *testing.T, conn *websocket.Conn) <-chan []string {
	t.Helper()
	result := make(chan []string, 64)
	go func() {
		defer close(result)
		for {
			var response tailResponse
			if err := conn.ReadJSON(&response); err != nil {
				return
			}
			var bodies []string
			for _, stream := range response.Streams {
				if stream.Labels["service_name"] != "api" {
					bodies = append(bodies, "lost labels")
				}
				for _, value := range stream.Values {
					bodies = append(bodies, value[1].(string))
				}
			}
			result <- bodies
		}
	}()
	return result
}

// receive collects tailed bodies until quiet passes without a message.
func receive(stream <-chan []string, quiet time.Duration) []string {
	var bodies []string
	for {
		select {
		case batch, ok := <-stream:
			if !ok {
				return bodies
			}
			bodies = append(bodies, batch...)
		case <-time.After(quiet):
			return bodies
		}
	}
}

func TestTailDeliversEachRecordOnceIncludingLateAndDuplicateRows(t *testing.T) {
	store := &memoryStore{}
	now := time.Now().UnixNano()
	store.add(row(now-int64(10*time.Second), "old beyond backlog"), row(now-int64(9*time.Second), "backlog 1"), row(now-int64(8*time.Second), "backlog 2"))
	server := httptest.NewServer(New(store, "test"))
	defer server.Close()
	conn, _, err := dialTail(t, server, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	stream := messages(t, conn)
	if got := receive(stream, time.Second); !slices.Equal(got, []string{"backlog 1", "backlog 2"}) {
		t.Fatalf("backlog: %v", got)
	}
	live := time.Now().UnixNano()
	store.add(row(live, "duplicate"), row(live, "duplicate"), row(live+1, "new"),
		row(live-int64(30*time.Second), "late arrival"))
	got := receive(stream, 5*time.Second)
	slices.Sort(got)
	if !slices.Equal(got, []string{"duplicate", "duplicate", "late arrival", "new"}) {
		t.Fatalf("live records lost, repeated or collapsed: %v", got)
	}
}

func TestTailRejectsForeignOriginsAndBoundsConcurrency(t *testing.T) {
	store := &memoryStore{}
	server := httptest.NewServer(New(store, "test", Options{AllowedOrigins: []string{"https://grafana.example"}}))
	defer server.Close()
	if _, response, err := dialTail(t, server, http.Header{"Origin": {"https://attacker.example"}}); err == nil || response.StatusCode != http.StatusForbidden {
		t.Fatal("foreign browser origin opened a tail")
	}
	var conns []*websocket.Conn
	defer func() {
		for _, conn := range conns {
			conn.Close()
		}
	}()
	for i := 0; i < maxTails; i++ {
		conn, _, err := dialTail(t, server, http.Header{"Origin": {"https://grafana.example"}})
		if err != nil {
			t.Fatalf("tail %d: %v", i, err)
		}
		conns = append(conns, conn)
	}
	if _, response, err := dialTail(t, server, nil); err == nil || response.StatusCode != http.StatusTooManyRequests {
		t.Fatal("tail concurrency is unbounded")
	}
}

func TestTailValidatesBeforeUpgrading(t *testing.T) {
	server := httptest.NewServer(New(&memoryStore{}, "test"))
	defer server.Close()
	for _, raw := range []string{
		"query=" + url.QueryEscape(`sum(count_over_time({service_name="api"}[1m]))`),
		"query=" + url.QueryEscape(`{service_name="api"}`) + "&delay_for=6",
		"query=" + url.QueryEscape(`{service_name="api"}`) + "&start=1",
		"query=" + url.QueryEscape(`{service_name="api"}`) + "&unknown=1",
	} {
		response, err := http.Get(server.URL + tailPath + "?" + raw)
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		if response.StatusCode != http.StatusBadRequest {
			t.Fatalf("invalid tail accepted: %s %d", raw, response.StatusCode)
		}
	}
}

func TestPatternsEndpointShapeAndSelectorOnly(t *testing.T) {
	store := &memoryStore{}
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano()
	for i := 0; i < 30; i++ {
		store.add(row(base+int64(i)*int64(time.Second), fmt.Sprintf("job %d finished after %d ms", i, i*3)))
	}
	server := httptest.NewServer(New(store, "test"))
	defer server.Close()
	params := url.Values{"query": {`{service_name="api"}`}, "start": {fmt.Sprint(base)}, "end": {fmt.Sprint(base + int64(time.Minute))}, "step": {"10s"}}
	response, err := http.Get(server.URL + "/loki/api/v1/patterns?" + params.Encode())
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var body struct {
		Status string
		Data   []struct {
			Pattern string
			Level   string
			Samples [][2]int64
		}
	}
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil || response.StatusCode != 200 || body.Status != "success" || len(body.Data) != 1 {
		t.Fatalf("pattern response: %d %+v %v", response.StatusCode, body, err)
	}
	if body.Data[0].Pattern != "job <_> finished after <_> ms" || body.Data[0].Level != "info" || body.Data[0].Samples[0] != [2]int64{base / 1e9, 10} {
		t.Fatalf("wrong pattern series: %+v", body.Data[0])
	}
	params.Set("query", `{service_name="api"} |= "job"`)
	response, err = http.Get(server.URL + "/loki/api/v1/patterns?" + params.Encode())
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusBadRequest {
		t.Fatal("pattern request with a pipeline was accepted")
	}
}

func TestPatternsSampleNewestLinesAndReportCoverage(t *testing.T) {
	store := &memoryStore{}
	base := int64(1767225600) * int64(time.Second)
	for i := 0; i < 100; i++ {
		body := fmt.Sprintf("recent request %d completed for user %d", i, i*3)
		if i < 40 {
			body = fmt.Sprintf("old payment %d declined for card %d", i, i*7)
		}
		store.add(row(base+int64(i)*int64(time.Second), body))
	}
	response := httptest.NewRecorder()
	params := url.Values{"query": {`{service_name="api"}`}, "start": {fmt.Sprint(base - int64(7*24*time.Hour))}, "end": {fmt.Sprint(base + int64(100*time.Second))}, "step": {"1m"}}
	New(store, "test", Options{Limits: limits.Config{PatternLines: 60}}).ServeHTTP(response, httptest.NewRequest("GET", "/loki/api/v1/patterns?"+params.Encode(), nil))
	if response.Code != 200 || response.Header().Get("X-Patterns-Covered-Start") != fmt.Sprint(base+int64(40*time.Second)) || response.Header().Get("X-Patterns-Sampled") != "true" || response.Header().Get("X-Patterns-Lines") != "60" {
		t.Fatalf("wrong coverage: %d %v %s", response.Code, response.Header(), response.Body.String())
	}
	var body struct {
		Data []struct {
			Pattern string     `json:"pattern"`
			Samples [][2]int64 `json:"samples"`
		} `json:"data"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	total := int64(0)
	for _, pattern := range body.Data {
		if strings.Contains(pattern.Pattern, "old") {
			t.Fatal("trained on old lines")
		}
		for _, sample := range pattern.Samples {
			total += sample[1]
		}
	}
	if total != 60 {
		t.Fatalf("newest input was lost or trained out of order: %d", total)
	}
}

func TestPatternsMemoryBoundFailsExplicitly(t *testing.T) {
	store := &memoryStore{}
	store.add(row(1, strings.Repeat("many words ", 100)))
	response := httptest.NewRecorder()
	params := url.Values{"query": {`{service_name="api"}`}, "start": {"0"}, "end": {"100"}}
	New(store, "test", Options{Limits: limits.Config{MaxRetainedBytes: 100}}).ServeHTTP(response, httptest.NewRequest("GET", "/loki/api/v1/patterns?"+params.Encode(), nil))
	if response.Code < 400 || !strings.Contains(response.Body.String(), `"status":"error"`) || !strings.Contains(response.Body.String(), "memory budget") {
		t.Fatalf("memory limit yielded partial success: %d %s", response.Code, response.Body.String())
	}
}

func TestTailUsesConfiguredLookback(t *testing.T) {
	for _, lookback := range []time.Duration{time.Hour, 2 * time.Hour} {
		server := httptest.NewServer(New(&memoryStore{}, "test", Options{Limits: limits.Config{TailLookback: lookback}}))
		target := "ws" + strings.TrimPrefix(server.URL, "http") + tailPath + "?" + url.Values{"query": {`{service_name="api"}`}, "start": {fmt.Sprint(time.Now().Add(-90 * time.Minute).UnixNano())}}.Encode()
		conn, response, err := websocket.DefaultDialer.Dial(target, nil)
		if lookback == time.Hour {
			if err == nil || response == nil || response.StatusCode != 400 {
				t.Fatal("tail exceeded default lookback")
			}
			response.Body.Close()
		} else {
			if err != nil {
				t.Fatalf("configured lookback ignored: %v", err)
			}
			conn.Close()
		}
		server.Close()
	}
}

func newTestTailer(t *testing.T, store *memoryStore, budget int) *tailer {
	t.Helper()
	selection, err := query.Parse(`{service_name="api"}`)
	if err != nil {
		t.Fatal(err)
	}
	source, err := engine.Compile(selection)
	if err != nil {
		t.Fatal(err)
	}
	config := (limits.Config{MaxRetainedBytes: budget}).WithDefaults()
	return &tailer{
		api:    &queryAPI{engine: engine.NewWithLimits(store, config), limits: config},
		source: source, seed: maphash.MakeSeed(), sent: map[uint64]tailSent{},
	}
}

func tailBodies(events []engine.Event) []string {
	var bodies []string
	for _, event := range events {
		bodies = append(bodies, event.Body)
	}
	return bodies
}

func TestTailStartupHandoffDeliversRowsArrivingAfterSnapshot(t *testing.T) {
	cutoff := int64(120 * time.Second)
	floor := cutoff - int64(tailLateness)
	store := &memoryStore{}
	store.add(row(cutoff-int64(10*time.Second), "old beyond backlog"),
		row(cutoff-int64(9*time.Second), "backlog 1"),
		row(cutoff-int64(8*time.Second), "backlog 2"))
	var once sync.Once
	scans := 0
	store.afterSnapshot = func(record.ScanOptions) {
		scans++
		once.Do(func() {
			// These were absent from the backlog snapshot. The duplicate must
			// increase its occurrence count, rather than being collapsed.
			store.add(row(cutoff-int64(30*time.Second), "handoff late row"),
				row(cutoff-int64(8*time.Second), "backlog 2"), row(cutoff, "at cutoff"))
		})
	}
	tailer := newTestTailer(t, store, 0)
	backlog, err := tailer.initialize(context.Background(), 0, cutoff, floor, 2)
	if err != nil || !slices.Equal(tailBodies(backlog), []string{"backlog 1", "backlog 2"}) || scans != 1 {
		t.Fatalf("startup must use one snapshot: %v, %v, scans=%d", tailBodies(backlog), err, scans)
	}
	covered := cutoff
	got, err := tailer.poll(context.Background(), floor, &covered, cutoff+int64(2*time.Second))
	if err != nil || !slices.Equal(tailBodies(got), []string{"handoff late row", "backlog 2", "at cutoff"}) {
		t.Fatalf("handoff records lost or reordered: %v, %v", tailBodies(got), err)
	}
	got, err = tailer.poll(context.Background(), floor, &covered, cutoff+int64(4*time.Second))
	if err != nil || len(got) != 0 {
		t.Fatalf("handoff records repeated: %v, %v", tailBodies(got), err)
	}
}

func TestTailStartupBaselineDoesNotApplyPollLimit(t *testing.T) {
	store := &memoryStore{}
	var rows []record.Raw
	for i := 0; i < tailMaxPoll+505; i++ {
		rows = append(rows, row(int64(i+1), fmt.Sprintf("%04d %s", i, strings.Repeat("x", 1024))))
	}
	store.add(rows...)
	// The historical bodies alone exceed this budget. Only two backlog
	// records and the historical fingerprint counts should be retained.
	tailer := newTestTailer(t, store, 1<<20)
	cutoff := int64(60 * time.Second)
	backlog, err := tailer.initialize(context.Background(), 0, cutoff, 0, 2)
	if err != nil || len(backlog) != 2 || len(tailer.sent) != len(rows) {
		t.Fatalf("historical baseline rejected: backlog=%d tracked=%d err=%v", len(backlog), len(tailer.sent), err)
	}
	if backlog[0].Body != rows[len(rows)-2].Body || backlog[1].Body != rows[len(rows)-1].Body {
		t.Fatal("backlog must contain the latest records")
	}
	store.add(rows[0]) // A new stored duplicate of an intentionally excluded row.
	got, err := tailer.fresh(context.Background(), 0, cutoff)
	if err != nil || len(got) != 1 || got[0].Body != rows[0].Body {
		t.Fatalf("baseline history resent or new duplicate lost: count=%d err=%v", len(got), err)
	}
}

func TestTailStartupBoundsTrackedFingerprints(t *testing.T) {
	store := &memoryStore{}
	var rows []record.Raw
	for i := 0; i <= tailMaxTracked; i++ {
		rows = append(rows, row(int64(i+1), "history"))
	}
	store.add(rows...)
	tailer := newTestTailer(t, store, 0)
	backlog, err := tailer.initialize(context.Background(), 0, int64(time.Minute), 0, 2)
	if !errors.Is(err, errTailRate) || backlog != nil || len(tailer.sent) > tailMaxTracked {
		t.Fatalf("startup tracking bound: events=%d tracked=%d err=%v", len(backlog), len(tailer.sent), err)
	}
}

func TestTailPollRetainedBudgetIncludesBodiesLabelsAndSerialization(t *testing.T) {
	for _, name := range []string{"bodies", "labels", "serialization", "fingerprints"} {
		t.Run(name, func(t *testing.T) {
			store := &memoryStore{}
			budget := 3 << 20
			switch name {
			case "bodies":
				var rows []record.Raw
				for i := 0; i < 1000; i++ {
					rows = append(rows, row(int64(i+1), strings.Repeat("x", 6000)))
				}
				store.add(rows...)
			case "labels":
				budget = 64 << 10
				r := row(1, "small body")
				r.LogAttributes = map[string]string{"large": strings.Repeat("x", 48<<10)}
				store.add(r)
			case "serialization":
				budget = 64 << 10
				// Raw data fits; JSON escaping and its buffers do not.
				store.add(row(1, strings.Repeat("\x00", 8<<10)))
			case "fingerprints":
				budget = 64 << 10
				var rows []record.Raw
				for i := 0; i < 180; i++ {
					rows = append(rows, row(int64(i+1), "history"))
				}
				store.add(rows...)
			}
			tailer := newTestTailer(t, store, budget)
			if name == "fingerprints" {
				// Baseline fits, but the simultaneous old and new count maps
				// must also fit when the next poll returns no new events.
				if _, err := tailer.initialize(context.Background(), 0, 1001, 0, 2); err != nil {
					t.Fatalf("initialize: %v", err)
				}
			}
			tracked := len(tailer.sent)
			got, err := tailer.fresh(context.Background(), 0, 1001)
			if !errors.Is(err, engine.ErrRetainedLimit) || got != nil || len(tailer.sent) != tracked {
				t.Fatalf("retained budget bypassed or failed poll committed: count=%d tracked=%d err=%v", len(got), len(tailer.sent), err)
			}
		})
	}
}

func TestTailJSONSizeMatchesEncoding(t *testing.T) {
	for _, value := range []string{"", "plain café 😀", "\x00\b\f\n\r\t\"\\<>&", "\u2028\u2029", string([]byte{0xff, 0xe2})} {
		encoded, err := json.Marshal(value)
		if err != nil || tailJSONBytes(value) != len(encoded) {
			t.Fatalf("string size for %q: got=%d want=%d err=%v", value, tailJSONBytes(value), len(encoded), err)
		}
	}
}

func TestTailPollCatchesUpAfterProlongedOverload(t *testing.T) {
	store := &memoryStore{}
	cutoff := int64(120 * time.Second)
	floor := cutoff - int64(tailLateness)
	store.add(row(cutoff-int64(time.Second), "backlog"))
	tailer := newTestTailer(t, store, 0)
	if _, err := tailer.initialize(context.Background(), 0, cutoff, floor, 2); err != nil {
		t.Fatal(err)
	}
	store.add(row(cutoff-int64(30*time.Second), "late during overload"),
		row(cutoff+int64(time.Second), "gap row"))
	covered := cutoff
	overloaded := true
	var starts []int64
	store.beforeScan = func(start, end int64, options record.ScanOptions) error {
		starts = append(starts, start)
		if overloaded {
			return limits.ErrOverloaded
		}
		return nil
	}
	for _, elapsed := range []time.Duration{2 * time.Second, 40 * time.Second, 90 * time.Second} {
		got, err := tailer.poll(context.Background(), floor, &covered, cutoff+int64(elapsed))
		if !errors.Is(err, limits.ErrOverloaded) || got != nil || covered != cutoff {
			t.Fatalf("overload advanced coverage: count=%d covered=%d err=%v", len(got), covered, err)
		}
	}
	overloaded = false
	got, err := tailer.poll(context.Background(), floor, &covered, cutoff+int64(92*time.Second))
	if err != nil || !slices.Equal(tailBodies(got), []string{"late during overload", "gap row"}) {
		t.Fatalf("overload gap lost records: %v, %v", tailBodies(got), err)
	}
	for _, start := range starts {
		if start != floor {
			t.Fatalf("retry advanced reread start: %d, want %d", start, floor)
		}
	}
	got, err = tailer.poll(context.Background(), floor, &covered, cutoff+int64(94*time.Second))
	if err != nil || len(got) != 0 {
		t.Fatalf("catch-up repeated records: %v, %v", tailBodies(got), err)
	}
}

func TestTailOverloadCatchupFailsExplicitlyWhenBoundExceeded(t *testing.T) {
	store := &memoryStore{}
	tailer := newTestTailer(t, store, 0)
	cutoff := int64(120 * time.Second)
	covered := cutoff
	store.beforeScan = func(int64, int64, record.ScanOptions) error { return limits.ErrOverloaded }
	if _, err := tailer.poll(context.Background(), 0, &covered, cutoff+int64(90*time.Second)); !errors.Is(err, limits.ErrOverloaded) {
		t.Fatal(err)
	}
	var rows []record.Raw
	for i := 0; i <= tailMaxPoll; i++ {
		rows = append(rows, row(cutoff+int64(i), "gap"))
	}
	store.add(rows...)
	store.beforeScan = nil
	got, err := tailer.poll(context.Background(), 0, &covered, cutoff+int64(92*time.Second))
	if !errors.Is(err, errTailRate) || got != nil || covered != cutoff {
		t.Fatalf("catch-up silently truncated: count=%d covered=%d err=%v", len(got), covered, err)
	}
}

func TestTailRetainedBudgetClosesWith1011(t *testing.T) {
	store := &memoryStore{}
	var once sync.Once
	store.afterSnapshot = func(record.ScanOptions) {
		once.Do(func() {
			var rows []record.Raw
			for i := 0; i < 1000; i++ {
				rows = append(rows, row(time.Now().Add(-time.Second).UnixNano()+int64(i), strings.Repeat("x", 6000)))
			}
			store.add(rows...)
		})
	}
	handler := New(store, "test", Options{Limits: limits.Config{MaxRetainedBytes: 3 << 20}})
	done := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(done)
		handler.ServeHTTP(w, r)
	}))
	defer server.Close()
	conn, _, err := dialTail(t, server, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	_, _, err = conn.ReadMessage()
	var closeErr *websocket.CloseError
	if !errors.As(err, &closeErr) || closeErr.Code != websocket.CloseInternalServerErr || !strings.Contains(closeErr.Text, "memory budget") {
		t.Fatalf("expected explicit 1011 memory error, got %v", err)
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("tail handler or websocket reader leaked after server-side close")
	}
}

func TestTailDisconnectJoinsReaderAndReleasesSlot(t *testing.T) {
	store := &memoryStore{}
	store.add(row(time.Now().Add(-time.Second).UnixNano(), "backlog"))
	config := (limits.Config{}).WithDefaults()
	api := &queryAPI{engine: engine.NewWithLimits(store, config), limits: config, tails: make(chan struct{}, maxTails)}
	completed := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() { completed <- struct{}{} }()
		api.tail(w, r)
	}))
	defer server.Close()
	for i := 0; i < 2*maxTails; i++ {
		conn, _, err := dialTail(t, server, nil)
		if err != nil {
			t.Fatal(err)
		}
		_ = conn.SetReadDeadline(time.Now().Add(time.Second))
		_, _, err = conn.ReadMessage()
		conn.Close()
		if err != nil {
			t.Fatal(err)
		}
		select {
		case <-completed: // tail joins its reader before the handler returns.
		case <-time.After(time.Second):
			t.Fatal("tail handler or reader leaked after disconnect")
		}
		if len(api.tails) != 0 {
			t.Fatal("disconnected tail still owns an admission slot")
		}
	}
}
