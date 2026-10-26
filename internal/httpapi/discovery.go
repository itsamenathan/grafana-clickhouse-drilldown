// SPDX-License-Identifier: AGPL-3.0-only
package httpapi

import (
	"errors"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"grafana-clickhouse-drilldown/internal/engine"
	"grafana-clickhouse-drilldown/internal/query"
)

func (a *queryAPI) selected(r *http.Request, start, end int64) ([]engine.Event, error) {
	selections, err := requestSelections(r)
	if err != nil {
		return nil, err
	}
	if r.Form.Has("match[]") {
		return a.engine.LoadUnion(r.Context(), selections, start, end)
	}
	return a.engine.Load(r.Context(), selections[0], start, end)
}

func (a *queryAPI) summarized(r *http.Request, start, end int64) ([]engine.ServiceSummary, bool, error) {
	selections, err := requestSelections(r)
	if err != nil {
		return nil, true, err
	}
	return a.engine.Summarize(r.Context(), selections, start, end)
}

func requestSelections(r *http.Request) ([]query.Selection, error) {
	if matches, exists := r.Form["match[]"]; exists {
		if r.Form.Has("query") || len(matches) > 16 {
			return nil, errors.New("use query or at most sixteen match[] selectors, not both")
		}
		selections := []query.Selection{}
		for _, text := range matches {
			selection, err := query.Parse(text)
			if err != nil {
				return nil, err
			}
			selections = append(selections, selection)
		}
		return selections, nil
	}
	text := r.Form.Get("query")
	if text == "" {
		text = `{detected_level=~".+"}`
	}
	selection, err := query.Parse(text)
	if err != nil {
		return nil, err
	}
	return []query.Selection{selection}, nil
}

func (a *queryAPI) labels(w http.ResponseWriter, r *http.Request) {
	if err := parameters(r, "query match[] start end limit"); err != nil {
		fail(w, err)
		return
	}
	start, end, err := bounds(r)
	if err != nil {
		fail(w, err)
		return
	}
	// Grafana's context provider discovers stream labels at the selected
	// row's millisecond timestamp with start == end. That timestamp omits
	// the record's sub-millisecond remainder. Resolve its containing bucket
	// instead of returning an empty label set and inducing an invalid {} query.
	// This affects metadata discovery only; log bounds remain exact [start,end).
	if start == end {
		if start%int64(time.Millisecond) != 0 || end > math.MaxInt64-int64(time.Millisecond) {
			fail(w, errors.New("instant label discovery requires a millisecond-aligned timestamp"))
			return
		}
		end += int64(time.Millisecond)
	}
	limit, err := integer(r, "limit", 5000, 5000)
	if err != nil {
		fail(w, err)
		return
	}
	summaries, summarized, err := a.summarized(r, start, end)
	if err != nil {
		fail(w, err)
		return
	}
	var events []engine.Event
	if summarized {
		for _, summary := range summaries {
			event := engine.Event{}
			event.Stream = map[string]string{}
			if summary.Service != "" {
				event.Stream["service_name"] = summary.Service
			}
			events = append(events, event)
		}
	} else {
		events, err = a.selected(r, start, end)
		if err != nil {
			fail(w, err)
			return
		}
	}
	labels := map[string]map[string]bool{}
	series := map[string]map[string]string{}
	for _, event := range events {
		series[engine.Key(event.Stream)] = event.Stream
		for name, value := range event.Stream {
			if value == "" {
				continue
			}
			if labels[name] == nil {
				labels[name] = map[string]bool{}
			}
			labels[name][value] = true
		}
	}
	if r.URL.Path == "/loki/api/v1/series" {
		keys := sortedKeys(series)
		if len(keys) > limit {
			keys = keys[:limit]
		}
		result := []map[string]string{}
		for _, key := range keys {
			result = append(result, series[key])
		}
		respond(w, http.StatusOK, map[string]any{"status": "success", "data": result})
		return
	}
	if r.URL.Path == "/loki/api/v1/detected_labels" {
		result := []map[string]any{}
		for _, name := range sortedKeys(labels) {
			result = append(result, map[string]any{"label": name, "cardinality": len(labels[name])})
		}
		respond(w, http.StatusOK, map[string]any{"status": "success", "detectedLabels": result})
		return
	}
	values := sortedKeys(labels)
	if r.URL.Path != "/loki/api/v1/labels" {
		name := strings.TrimPrefix(r.URL.Path, "/loki/api/v1/label/")
		if !strings.HasSuffix(name, "/values") || strings.Contains(strings.TrimSuffix(name, "/values"), "/") {
			fail(w, errors.New("invalid label values path"))
			return
		}
		name = strings.TrimSuffix(name, "/values")
		if name != "service_name" {
			fail(w, errors.New("unsupported indexed label"))
			return
		}
		values = sortedKeys(labels[name])
	}
	if len(values) > limit {
		values = values[:limit]
	}
	respond(w, http.StatusOK, map[string]any{"status": "success", "data": values})
}

type field struct {
	Label       string   `json:"label"`
	Type        string   `json:"type"`
	Cardinality int      `json:"cardinality"`
	Parsers     []string `json:"parsers"`
}

type fieldInfo struct {
	typeName string
	values   map[string]bool
	parsers  map[string]bool
}

func fieldType(value string) string {
	if _, err := strconv.ParseInt(value, 10, 64); err == nil {
		return "int"
	}
	if n, err := strconv.ParseFloat(value, 64); err == nil && !math.IsNaN(n) && !math.IsInf(n, 0) {
		return "float"
	}
	if value == "true" || value == "false" {
		return "boolean"
	}
	if d, err := time.ParseDuration(value); err == nil && d != 0 {
		return "duration"
	}
	return "string"
}

func (a *queryAPI) fields(w http.ResponseWriter, r *http.Request) {
	if err := parameters(r, "query start end limit line_limit step"); err != nil {
		fail(w, err)
		return
	}
	start, end, err := bounds(r)
	if err != nil {
		fail(w, err)
		return
	}
	limit, err := integer(r, "limit", 100, 1000)
	if err != nil {
		fail(w, err)
		return
	}
	lines, err := integer(r, "line_limit", 1000, 10000)
	if err != nil {
		fail(w, err)
		return
	}
	if _, err := duration(r.Form.Get("step"), time.Second); err != nil {
		fail(w, err)
		return
	}
	events, err := a.selected(r, start, end)
	if err != nil {
		fail(w, err)
		return
	}
	if len(events) > lines {
		events = events[len(events)-lines:]
	}
	fields := map[string]*fieldInfo{}
	for _, event := range events {
		for i, values := range []map[string]string{event.Metadata, event.Parsed} {
			for name, value := range values {
				if strings.HasPrefix(name, "__") {
					continue
				}
				info, exists := fields[name]
				if !exists {
					if len(fields) >= 1000 {
						fail(w, errors.New("too many detected fields"))
						return
					}
					info = &fieldInfo{typeName: fieldType(value), values: map[string]bool{}, parsers: map[string]bool{}}
					fields[name] = info
				}
				typ := fieldType(value)
				if info.typeName != typ {
					if info.typeName == "int" && typ == "float" || info.typeName == "float" && typ == "int" {
						info.typeName = "float"
					} else {
						info.typeName = "string"
					}
				}
				info.values[value] = true
				if i == 1 && event.Origins[name] != "" {
					info.parsers[event.Origins[name]] = true
				}
			}
		}
	}
	if r.URL.Path != "/loki/api/v1/detected_fields" {
		name := strings.TrimPrefix(r.URL.Path, "/loki/api/v1/detected_field/")
		if !strings.HasSuffix(name, "/values") || strings.Contains(strings.TrimSuffix(name, "/values"), "/") {
			fail(w, errors.New("invalid field values path"))
			return
		}
		name = strings.TrimSuffix(name, "/values")
		values := []string{}
		if info := fields[name]; info != nil {
			values = sortedKeys(info.values)
		}
		if len(values) > limit {
			values = values[:limit]
		}
		respond(w, http.StatusOK, map[string]any{"status": "success", "values": values})
		return
	}
	result := []field{}
	names := sortedKeys(fields)
	if len(names) > limit {
		names = names[:limit]
	}
	for _, name := range names {
		info := fields[name]
		var parsers []string
		if len(info.parsers) != 0 {
			parsers = sortedKeys(info.parsers)
		}
		result = append(result, field{name, info.typeName, len(info.values), parsers})
	}
	respond(w, http.StatusOK, map[string]any{"status": "success", "fields": result, "limit": limit})
}

func sortedKeys[T any](values map[string]T) []string {
	result := make([]string, 0, len(values))
	for key := range values {
		result = append(result, key)
	}
	sort.Strings(result)
	return result
}
