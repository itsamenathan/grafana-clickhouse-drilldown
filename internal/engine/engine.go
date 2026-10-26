// SPDX-License-Identifier: AGPL-3.0-only
package engine

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"strconv"
	"strings"
	"time"

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
const MaxBytes = 8 << 20

type Store interface {
	Scan(context.Context, sqlgen.Predicate, int64, int64, func(record.Raw) error) error
}

type pageStore interface {
	ScanPage(context.Context, sqlgen.Predicate, int64, int64, string, int, func(record.Raw) error) error
}

type logPage struct {
	store     pageStore
	direction string
	limit     int
}

func (p logPage) Scan(ctx context.Context, predicate sqlgen.Predicate, start, end int64, visit func(record.Raw) error) error {
	return p.store.ScanPage(ctx, predicate, start, end, p.direction, p.limit, visit)
}

type Engine struct{ store Store }

func New(store Store) *Engine { return &Engine{store} }

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

func Key(labels map[string]string) string {
	encoded, _ := json.Marshal(labels)
	return string(encoded)
}

// Load uses the same compiled SQL and residual pipeline for logs, metrics,
// labels, fields, values and volume queries. It never silently truncates scans.
func (e *Engine) Load(ctx context.Context, selection query.Selection, start, end int64) ([]Event, error) {
	compiled, err := sqlgen.Compile(selection)
	if err != nil {
		return nil, err
	}
	return e.load(ctx, compiled, start, end)
}

func (e *Engine) LoadLogs(ctx context.Context, selection query.Selection, start, end int64, direction string, limit int) ([]Event, error) {
	if store, ok := e.store.(pageStore); ok && selection.CanLimitLogs() {
		paged := New(logPage{store, direction, limit})
		return paged.Load(ctx, selection, start, end)
	}
	return e.Load(ctx, selection, start, end)
}

// LoadUnion compiles repeated match[] selectors into one OR predicate. Reading
// once avoids duplicating events that match multiple selectors while retaining
// genuinely duplicated stored records.
func (e *Engine) LoadUnion(ctx context.Context, selections []query.Selection, start, end int64) ([]Event, error) {
	compiled, err := compileUnion(selections)
	if err != nil {
		return nil, err
	}
	return e.load(ctx, compiled, start, end)
}

func compileUnion(selections []query.Selection) (sqlgen.Compiled, error) {
	if len(selections) == 0 || len(selections) > 16 {
		return sqlgen.Compiled{}, errors.New("invalid selector count")
	}
	combined := sqlgen.Predicate{Parameters: map[string]string{}}
	var conditions []string
	for i, selection := range selections {
		if !selection.IsSelector() {
			return sqlgen.Compiled{}, errors.New("match[] accepts stream selectors only")
		}
		predicate, err := sqlgen.Selection(selection)
		if err != nil {
			return sqlgen.Compiled{}, err
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
	residual, _ := selections[0].NewEvaluator()
	return sqlgen.Compiled{Predicate: combined, Residual: residual}, nil
}

type ServiceSummary = record.ServiceSummary

type summaryStore interface {
	Summarize(context.Context, sqlgen.Predicate, int64, int64) ([]record.ServiceSummary, error)
}

// Summarize is exact only for selectors: every match is already represented
// by the shared SQL predicate. A pipeline must use the full residual path.
func (e *Engine) Summarize(ctx context.Context, selections []query.Selection, start, end int64) ([]ServiceSummary, bool, error) {
	store, ok := e.store.(summaryStore)
	if !ok {
		return nil, false, nil
	}
	for _, selection := range selections {
		if !selection.IsSelector() {
			return nil, false, nil
		}
	}
	compiled, err := compileUnion(selections)
	if err != nil {
		return nil, true, err
	}
	result, err := store.Summarize(ctx, compiled.Predicate, start, end)
	return result, true, err
}

func (e *Engine) load(ctx context.Context, compiled sqlgen.Compiled, start, end int64) ([]Event, error) {
	result := []Event{}
	size := 0
	err := e.store.Scan(ctx, compiled.Predicate, start, end, func(row record.Raw) error {
		stream := map[string]string{}
		if row.Service != "" {
			stream["service_name"] = row.Service
		}
		metadata := row.Metadata()
		metadata["detected_level"] = row.Level
		processed, matches, err := compiled.Residual.Process(ctx, row.Timestamp, row.Body, stream, metadata)
		if err != nil {
			return err
		}
		if !matches {
			return nil
		}
		size += len(row.Body) + 128
		for _, labels := range []map[string]string{processed.Stream, processed.Metadata, processed.Parsed} {
			for key, value := range labels {
				size += len(key) + len(value) + 64
			}
		}
		if size > MaxBytes || len(result) >= 10000 {
			return errors.New("pipeline result exceeds the bounded memory budget; narrow the selection")
		}
		result = append(result, Event{row.Timestamp, row.Body, processed})
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.SliceStable(result, func(i, j int) bool { return result[i].Timestamp < result[j].Timestamp })
	return result, nil
}

func Seconds(ns int64) json.Number {
	seconds, fraction := ns/1e9, ns%1e9
	if fraction == 0 {
		return json.Number(strconv.FormatInt(seconds, 10))
	}
	return json.Number(strconv.FormatInt(seconds, 10) + "." + strings.TrimRight(strconv.FormatInt(fraction+1e9, 10)[1:], "0"))
}

func Range(start, end int64) error {
	return RangeLimit(start, end, MaxRange)
}

func RangeLimit(start, end int64, maximum time.Duration) error {
	if start < 0 || end < start || end-start > int64(maximum) {
		return errors.New("time range must be ordered and at most " + maximum.String())
	}
	return nil
}
