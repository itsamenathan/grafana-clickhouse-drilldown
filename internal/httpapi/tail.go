// SPDX-License-Identifier: AGPL-3.0-only
package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/maphash"
	"log/slog"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"time"
	"unicode/utf8"

	"github.com/gorilla/websocket"

	"grafana-clickhouse-drilldown/internal/engine"
	"grafana-clickhouse-drilldown/internal/limits"
	"grafana-clickhouse-drilldown/internal/query"
	"grafana-clickhouse-drilldown/internal/record"
)

const (
	tailPath = "/loki/api/v1/tail"
	maxTails = 4
	// Rows reach ClickHouse up to about 20 seconds after their timestamp.
	// Every poll rereads this window and sends only rows it has not sent.
	// A row arriving later than this is not tailed.
	tailLateness    = 60 * time.Second
	tailInterval    = 2 * time.Second
	tailMaxDuration = time.Hour
	tailMaxDelay    = 5
	tailMaxPoll     = 2000
	tailMaxTracked  = 20000
	tailBatch       = 500
	tailWriteWait   = 10 * time.Second
	tailPingPeriod  = 30 * time.Second
	// Conservative allowances for websocket buffers and fingerprint map entries.
	tailBaseBytes  = 32 << 10
	tailPrintBytes = 128
)

var errTailRate = errors.New("tailed selection exceeds the bounded rate; narrow the selector")

type tailResponse struct {
	Streams        []engine.Stream `json:"streams"`
	DroppedEntries []any           `json:"dropped_entries"`
}

type tailSent struct {
	timestamp int64
	count     int
}

// tailer delivers each matching record once, preserving stored duplicates:
// a poll that sees a fingerprint more often than it was sent sends only the
// difference.
type tailer struct {
	api    *queryAPI
	source engine.Source
	conn   *websocket.Conn
	seed   maphash.Seed
	sent   map[uint64]tailSent
}

// tail implements Loki's websocket tail, used by Grafana Explore's live mode.
// It sends up to limit records from [start, now-delay), then polls. It never
// silently drops records: exceeding a bound closes the stream with an error.
func (a *queryAPI) tail(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		fail(w, errors.New("tail requires a websocket GET"))
		return
	}
	if err := parameters(r, "query limit start delay_for"); err != nil {
		fail(w, err)
		return
	}
	selection, err := query.Parse(r.Form.Get("query"))
	if err != nil {
		fail(w, err)
		return
	}
	limit, err := integer(r, "limit", 100, 2000)
	if err != nil {
		fail(w, err)
		return
	}
	delay := 0
	if value := r.Form.Get("delay_for"); value != "" {
		if delay, err = strconv.Atoi(value); err != nil || delay < 0 || delay > tailMaxDelay {
			fail(w, errors.New("delay_for must be 0 to 5 seconds"))
			return
		}
	}
	now := time.Now().UnixNano()
	start, err := timestamp(r.Form.Get("start"), max(0, now-int64(a.limits.TailLookback)))
	if err != nil || start > now || now-start > int64(a.limits.TailLookback) {
		fail(w, fmt.Errorf("tail start must be within the last %s", a.limits.TailLookback))
		return
	}
	source, err := engine.Compile(selection)
	if err != nil {
		fail(w, err)
		return
	}
	select {
	case a.tails <- struct{}{}:
		defer func() { <-a.tails }()
	default:
		fail(w, limits.ErrOverloaded)
		return
	}
	upgrader := websocket.Upgrader{WriteBufferSize: 16384, CheckOrigin: a.allowedOrigin}
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(r.Context(), tailMaxDuration)
	defer cancel()
	// Grafana sends nothing but control frames; reading processes them and
	// detects the client closing.
	conn.SetReadLimit(4096)
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		defer cancel()
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}()
	defer func() {
		// Unblock ReadMessage on every exit and join the sole reader.
		_ = conn.Close()
		<-readerDone
	}()
	t := &tailer{api: a, source: source, conn: conn, seed: maphash.MakeSeed(), sent: map[uint64]tailSent{}}
	err = t.run(ctx, start, max(start, now-int64(delay)*int64(time.Second)), int64(delay)*int64(time.Second), limit)
	code, reason := websocket.CloseNormalClosure, "maximum tail duration reached"
	switch {
	case errors.Is(ctx.Err(), context.Canceled):
		return // The client closed the stream.
	case errors.Is(ctx.Err(), context.DeadlineExceeded):
	default:
		slog.Warn("tail stopped", "error", err.Error())
		code, reason = websocket.CloseInternalServerErr, err.Error()
		if len(reason) > 120 {
			reason = reason[:120]
		}
	}
	_ = conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(code, reason), time.Now().Add(tailWriteWait))
}

func (a *queryAPI) allowedOrigin(r *http.Request) bool {
	origin := r.Header.Get("Origin")
	if origin == "" {
		return true
	}
	parsed, err := url.Parse(origin)
	if err == nil && parsed.Host == r.Host {
		return true
	}
	return slices.Contains(a.origins, origin)
}

func (t *tailer) run(ctx context.Context, start, cutoff, delay int64, limit int) error {
	floor := max(start, cutoff-int64(tailLateness))
	backlog, err := t.initialize(ctx, start, cutoff, floor, limit)
	if err != nil {
		return err
	}
	if err := t.send(backlog); err != nil {
		return err
	}
	backlog = nil
	covered := cutoff
	ticker := time.NewTicker(tailInterval)
	defer ticker.Stop()
	lastWrite := time.Now()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
		end := time.Now().UnixNano() - delay
		events, err := t.poll(ctx, floor, &covered, end)
		if errors.Is(err, limits.ErrOverloaded) {
			continue // Another query holds the reader; retry on the next tick.
		}
		if err != nil {
			return err
		}
		if len(events) != 0 {
			if err := t.send(events); err != nil {
				return err
			}
			lastWrite = time.Now()
		} else if time.Since(lastWrite) >= tailPingPeriod {
			if err := t.conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(tailWriteWait)); err != nil {
				return err
			}
			lastWrite = time.Now()
		}
	}
}

// withDeadline gives one tail query the same deadline as an ordinary request.
func (a *queryAPI) withDeadline(ctx context.Context, run func(context.Context) error) error {
	ctx, cancel := context.WithTimeout(ctx, a.limits.RequestTimeout)
	defer cancel()
	return run(ctx)
}

// initialize selects the latest backlog and counts the historical baseline in
// the same backward scan. Rows excluded by limit are older than the backlog;
// rows appearing after this scan remain unseen and are delivered by a poll.
func (t *tailer) initialize(ctx context.Context, start, cutoff, floor int64, limit int) ([]engine.Event, error) {
	var backlog []engine.Event
	retained := tailBaseBytes
	if err := t.reserve(retained, 0); err != nil {
		return nil, err
	}
	err := t.api.withDeadline(ctx, func(ctx context.Context) error {
		return t.api.engine.Each(ctx, t.source, start, cutoff, record.ScanOptions{Backward: true, Metadata: true, EdgeWindows: true}, func(event engine.Event) error {
			if event.Timestamp < floor && len(backlog) >= limit {
				return record.ErrStop
			}
			if event.Timestamp >= floor {
				if err := t.reserve(retained, tailLabelBytes(event)); err != nil {
					return err
				}
				print := t.fingerprint(event)
				previous, exists := t.sent[print]
				if !exists {
					if len(t.sent) >= tailMaxTracked {
						return errTailRate
					}
					if err := t.reserve(retained, tailPrintBytes); err != nil {
						return err
					}
					retained += tailPrintBytes
				}
				t.sent[print] = tailSent{event.Timestamp, previous.count + 1}
			}
			if len(backlog) < limit {
				size := tailEventBytes(event)
				if err := t.reserve(retained, size); err != nil {
					return err
				}
				retained += size
				backlog = append(backlog, event)
			}
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	slices.Reverse(backlog)
	return backlog, nil
}

// poll advances coverage only after a complete successful scan. Admission
// failures retain the previous boundary, even if retries take over a minute.
func (t *tailer) poll(ctx context.Context, floor int64, covered *int64, end int64) ([]engine.Event, error) {
	if end <= *covered {
		return nil, nil
	}
	events, err := t.fresh(ctx, max(floor, *covered-int64(tailLateness)), end)
	if err == nil {
		*covered = end
	}
	return events, err
}

// fresh rereads [start,end) and returns matching records not yet sent, in
// ascending order. It commits fingerprint counts only after a complete scan.
func (t *tailer) fresh(ctx context.Context, start, end int64) ([]engine.Event, error) {
	if end <= start {
		return nil, nil
	}
	// Keep the previous map intact until replacement. Deleting expired keys
	// would keep its allocated buckets while undercounting them in the budget.
	occurrences := map[uint64]tailSent{}
	var events []engine.Event
	retained := tailBaseBytes + len(t.sent)*tailPrintBytes
	if err := t.reserve(retained, 0); err != nil {
		return nil, err
	}
	err := t.api.withDeadline(ctx, func(ctx context.Context) error {
		return t.api.engine.Each(ctx, t.source, start, end, record.ScanOptions{Metadata: true}, func(event engine.Event) error {
			if err := t.reserve(retained, tailLabelBytes(event)); err != nil {
				return err
			}
			print := t.fingerprint(event)
			previous, exists := occurrences[print]
			if !exists {
				if len(occurrences) >= tailMaxTracked {
					return errTailRate
				}
				if err := t.reserve(retained, tailPrintBytes); err != nil {
					return err
				}
				retained += tailPrintBytes
			}
			occurrences[print] = tailSent{event.Timestamp, previous.count + 1}
			if previous.count+1 > t.sent[print].count {
				if len(events) >= tailMaxPoll {
					return errTailRate
				}
				size := tailEventBytes(event)
				if err := t.reserve(retained, size); err != nil {
					return err
				}
				retained += size
				events = append(events, event)
			}
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	t.sent = occurrences
	return events, nil
}

func (t *tailer) reserve(retained, size int) error {
	if size > t.api.limits.MaxRetainedBytes-retained {
		return engine.ErrRetainedLimit
	}
	return nil
}

// Allow for merged labels, canonical fingerprint keys and JSON encoder
// growth/copy buffers before allocating any of them.
func tailLabelBytes(event engine.Event) int {
	size := 512
	for _, labels := range []map[string]string{event.Stream, event.Metadata, event.Parsed} {
		for key, value := range labels {
			size += 512 + len(key) + len(value) + 3*(tailJSONBytes(key)+tailJSONBytes(value))
		}
	}
	return size
}

// The allowance includes the event and slice, response stream/value objects,
// labels and simultaneously live JSON staging/output buffers.
func tailEventBytes(event engine.Event) int {
	size := 1024 + len(event.Body) + 3*tailJSONBytes(event.Body) + tailLabelBytes(event)
	// Parser origins are retained with the event even though tail responses
	// flatten the label categories and do not serialize those origins.
	for key, value := range event.Origins {
		size += 128 + len(key) + len(value)
	}
	return size
}

// tailJSONBytes is encoding/json's quoted string size, without allocating a
// serialized copy. Invalid UTF-8 and U+2028/U+2029 use six-byte escapes.
func tailJSONBytes(value string) int {
	size := 2
	for i := 0; i < len(value); {
		c := value[i]
		switch {
		case c == '"' || c == '\\' || c == '\b' || c == '\f' || c == '\n' || c == '\r' || c == '\t':
			size += 2
			i++
		case c < 0x20 || c == '<' || c == '>' || c == '&':
			size += 6
			i++
		case c < utf8.RuneSelf:
			size++
			i++
		default:
			r, n := utf8.DecodeRuneInString(value[i:])
			if r == utf8.RuneError && n == 1 || r == '\u2028' || r == '\u2029' {
				size += 6
			} else {
				size += n
			}
			i += n
		}
	}
	return size
}

func (t *tailer) fingerprint(event engine.Event) uint64 {
	var h maphash.Hash
	h.SetSeed(t.seed)
	h.WriteString(strconv.FormatInt(event.Timestamp, 10))
	h.WriteByte(0)
	h.WriteString(event.Body)
	h.WriteByte(0)
	h.WriteString(engine.Key(event.Labels()))
	return h.Sum64()
}

// send writes records in ascending time order. Consecutive records with the
// same labels share a stream entry so the client appends them in order.
func (t *tailer) send(events []engine.Event) error {
	retained := tailBaseBytes + len(t.sent)*tailPrintBytes
	if err := t.reserve(retained, 0); err != nil {
		return err
	}
	for _, event := range events {
		size := tailEventBytes(event)
		if err := t.reserve(retained, size); err != nil {
			return err
		}
		retained += size
	}
	for len(events) != 0 {
		batch := events[:min(len(events), tailBatch)]
		events = events[len(batch):]
		response := tailResponse{Streams: []engine.Stream{}}
		previous := ""
		for _, event := range batch {
			labels := event.Labels()
			key := engine.Key(labels)
			if key != previous || len(response.Streams) == 0 {
				response.Streams = append(response.Streams, engine.Stream{Labels: labels, Values: [][]any{}})
				previous = key
			}
			last := &response.Streams[len(response.Streams)-1]
			last.Values = append(last.Values, []any{strconv.FormatInt(event.Timestamp, 10), event.Body})
		}
		encoded, err := json.Marshal(response)
		if err != nil {
			return err
		}
		if err := t.conn.SetWriteDeadline(time.Now().Add(tailWriteWait)); err != nil {
			return err
		}
		if err := t.conn.WriteMessage(websocket.TextMessage, encoded); err != nil {
			return err
		}
	}
	return nil
}
