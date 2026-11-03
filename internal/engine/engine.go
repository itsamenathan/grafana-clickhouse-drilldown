// SPDX-License-Identifier: AGPL-3.0-only
package engine

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strconv"
	"strings"
	"time"

	"grafana-clickhouse-drilldown/internal/limits"
	"grafana-clickhouse-drilldown/internal/query"
	"grafana-clickhouse-drilldown/internal/record"
	"grafana-clickhouse-drilldown/internal/sqlgen"
)

const MaxRange = time.Hour

// Grafana refines one edge of its two-hour context window to nanoseconds
// while leaving the far edge at millisecond precision.
const MaxLogRange = 2*time.Hour + time.Millisecond
const MaxPoints = 2000
const MaxSamples = 10000

// MaxRetainedBytes bounds what one request holds after streaming, such as a
// log page or detected-field values. Scanned records are not retained.
const MaxRetainedBytes = 32 << 20

var ErrRetainedLimit = errors.New("selected result exceeds the bounded memory budget; narrow the selection")

type Store interface {
	Scan(context.Context, sqlgen.Predicate, int64, int64, record.ScanOptions, func(record.Raw) error) error
}

type Engine struct {
	store         Store
	retainedBytes int
}

func New(store Store) *Engine { return NewWithLimits(store, limits.Config{}) }

// NewWithLimits keeps result retention under the caller's configured budget.
func NewWithLimits(store Store, config limits.Config) *Engine {
	return &Engine{store: store, retainedBytes: config.WithDefaults().MaxRetainedBytes}
}

type Event struct {
	Timestamp int64
	Body      string
	query.Processed
}

func (e Event) Labels() map[string]string {
	result := map[string]string{}
	for _, group := range []map[string]string{e.Stream, e.Metadata, e.Parsed} {
		for key, value := range group {
			result[key] = value
		}
	}
	return result
}

// size approximates an event's retained memory.
func (e Event) size() int {
	size := len(e.Body) + 128
	for _, labels := range []map[string]string{e.Stream, e.Metadata, e.Parsed} {
		for key, value := range labels {
			size += len(key) + len(value) + 64
		}
	}
	return size
}

func Key(labels map[string]string) string {
	encoded, _ := json.Marshal(labels)
	return string(encoded)
}

// Source is one compiled selection shared by every endpoint: a single query,
// or a union of repeated match[] stream selectors.
type Source struct {
	compiled      sqlgen.Compiled
	rowPreserving bool
	labelFilters  bool
	selection     query.Selection
}

func Compile(selection query.Selection) (Source, error) {
	compiled, err := sqlgen.Compile(selection)
	if err != nil {
		return Source{}, err
	}
	return Source{compiled: compiled, rowPreserving: selection.CanLimitLogs(), labelFilters: selection.FiltersOnLabels(), selection: selection}, nil
}

// CompileUnion compiles repeated match[] selectors into one OR predicate.
// Reading once avoids duplicating events that match multiple selectors while
// retaining genuinely duplicated stored records.
func CompileUnion(selections []query.Selection) (Source, error) {
	if len(selections) == 0 || len(selections) > 16 {
		return Source{}, errors.New("invalid selector count")
	}
	if len(selections) == 1 {
		return Compile(selections[0])
	}
	combined := sqlgen.Predicate{Parameters: map[string]string{}}
	var conditions []string
	for i, selection := range selections {
		if !selection.IsSelector() {
			return Source{}, errors.New("match[] accepts stream selectors only")
		}
		predicate, err := sqlgen.Selection(selection)
		if err != nil {
			return Source{}, err
		}
		statement := predicate.SQL
		for key, value := range predicate.Parameters {
			name := "selector_" + strconv.Itoa(i) + "_" + key
			statement = strings.ReplaceAll(statement, "{"+key+":String}", "{"+name+":String}")
			combined.Parameters[name] = value
		}
		conditions = append(conditions, "("+statement+")")
	}
	combined.SQL = strings.Join(conditions, " OR ")
	residual, err := selections[0].NewEvaluator()
	if err != nil {
		return Source{}, err
	}
	return Source{compiled: sqlgen.Compiled{Predicate: combined, Residual: residual}, rowPreserving: true}, nil
}

// Each streams every event matching the source's SQL predicate and exact
// residual pipeline, in scan order. Records are read in bounded pages and are
// not retained. Without options.Metadata, events carry only service_name and
// detected_level; label filters always force complete metadata.
func (e *Engine) Each(ctx context.Context, source Source, start, end int64, options record.ScanOptions, visit func(Event) error) error {
	if options.Limit > 0 && !source.rowPreserving {
		return errors.New("a SQL limit cannot be pushed through a filtering pipeline")
	}
	options.Metadata = options.Metadata || source.labelFilters
	return e.store.Scan(ctx, source.compiled.Predicate, start, end, options, func(row record.Raw) error {
		stream, metadata := rowLabels(row)
		processed, matches, err := source.compiled.Residual.Process(ctx, row.Timestamp, row.Body, stream, metadata)
		if err != nil || !matches {
			return err
		}
		return visit(Event{row.Timestamp, row.Body, processed})
	})
}

// rowLabels maps a stored record to its stream labels and structured metadata.
func rowLabels(row record.Raw) (map[string]string, map[string]string) {
	stream := map[string]string{}
	if row.Service != "" {
		stream["service_name"] = row.Service
	}
	metadata := row.Metadata()
	metadata["detected_level"] = row.Level
	return stream, metadata
}

// Logs returns at most limit events in ascending complete-row order: the
// earliest for forward and the latest for backward. A row-preserving pipeline
// pushes the limit into SQL; a filtering pipeline streams until it has
// enough matches. The limit is never pushed through a filter.
func (e *Engine) Logs(ctx context.Context, source Source, start, end int64, direction string, limit int) ([]Event, error) {
	if limit < 1 || direction != "forward" && direction != "backward" {
		return nil, errors.New("invalid log page")
	}
	options := record.ScanOptions{Backward: direction == "backward", Metadata: true, EdgeWindows: true}
	if source.rowPreserving {
		options.Limit = limit
	}
	var events []Event
	retained := 0
	err := e.Each(ctx, source, start, end, options, func(event Event) error {
		if retained += event.size(); retained > e.retainedBytes {
			return ErrRetainedLimit
		}
		events = append(events, event)
		if len(events) >= limit {
			return record.ErrStop
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if options.Backward {
		slices.Reverse(events)
	}
	return events, nil
}

type ServiceSummary = record.ServiceSummary

type summaryStore interface {
	Summarize(context.Context, sqlgen.Predicate, int64, int64) ([]record.ServiceSummary, error)
}

// Summarize is exact only for selectors: every match is already represented
// by the shared SQL predicate. A pipeline must use the full residual path.
func (e *Engine) Summarize(ctx context.Context, selections []query.Selection, start, end int64, countOnly bool) ([]ServiceSummary, bool, error) {
	store, ok := e.store.(summaryStore)
	if !ok {
		return nil, false, nil
	}
	for _, selection := range selections {
		if !selection.IsSelector() {
			return nil, false, nil
		}
	}
	source, err := CompileUnion(selections)
	if err != nil {
		return nil, true, err
	}
	source.compiled.Predicate.CountOnly = countOnly
	result, err := store.Summarize(ctx, source.compiled.Predicate, start, end)
	return result, true, err
}

func Seconds(ns int64) json.Number {
	seconds, fraction := ns/1e9, ns%1e9
	if fraction == 0 {
		return json.Number(strconv.FormatInt(seconds, 10))
	}
	return json.Number(strconv.FormatInt(seconds, 10) + "." + strings.TrimRight(strconv.FormatInt(fraction+1e9, 10)[1:], "0"))
}

func RangeLimit(start, end int64, maximum time.Duration) error {
	if start < 0 || end < start || end-start > int64(maximum) {
		return errors.New("time range must be ordered and at most " + maximum.String())
	}
	return nil
}
