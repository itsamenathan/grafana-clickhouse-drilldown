// SPDX-License-Identifier: AGPL-3.0-only
package engine

import (
	"context"
	"errors"
	"sort"
	"strconv"

	"grafana-clickhouse-drilldown/internal/query"
)

type Stream struct {
	Labels map[string]string `json:"stream"`
	Values [][]any           `json:"values"`
}

// Logs applies the requested global limit before grouping into streams. Exact
// duplicate events remain separate. Equal timestamps use the deterministic
// complete-row order established by the storage query, reversed for backward.
// Timestamp-only pagination with a tie larger than the limit is unsupported;
// clients must not infer a unique event cursor from a timestamp.
func Logs(events []Event, direction string, limit int, categorize bool) []Stream {
	result := []Stream{}
	indices := map[string]int{}
	for n := 0; n < len(events) && n < limit; n++ {
		i := n
		if direction == "backward" {
			i = len(events) - 1 - n
		}
		event := events[i]
		labels := event.Stream
		if !categorize {
			labels = event.Labels()
		}
		key := Key(labels)
		index, exists := indices[key]
		if !exists {
			index = len(result)
			indices[key] = index
			result = append(result, Stream{labels, [][]any{}})
		}
		value := []any{strconv.FormatInt(event.Timestamp, 10), event.Body}
		if categorize {
			categories := map[string]any{}
			if len(event.Metadata) != 0 {
				categories["structuredMetadata"] = event.Metadata
			}
			if len(event.Parsed) != 0 {
				categories["parsed"] = event.Parsed
			}
			value = append(value, categories)
		}
		result[index].Values = append(result[index].Values, value)
	}
	sort.Slice(result, func(i, j int) bool { return Key(result[i].Labels) < Key(result[j].Labels) })
	return result
}

type Series struct {
	Metric map[string]string `json:"metric"`
	Values [][]any           `json:"values,omitempty"`
	Value  []any             `json:"value,omitempty"`
}

type metricGroup struct {
	labels map[string]string
	count  int
	bytes  int
}

// Metrics evaluates real sliding windows (t-window,t]. It does not substitute
// disjoint histogram buckets. Empty windows produce no sample, as in Loki.
func Metrics(ctx context.Context, events []Event, metric query.Metric, start, end, step int64, instant bool) ([]Series, error) {
	if metric.Window <= 0 || metric.Window > MaxRange || (metric.Function != "count_over_time" && metric.Function != "rate" && metric.Function != "bytes_over_time" && metric.Function != "bytes_rate") {
		return nil, errors.New("unsupported metric plan")
	}
	if start < 0 || end < start || step <= 0 || (end-start)/step >= MaxPoints {
		return nil, errors.New("invalid metric evaluation range or too many points")
	}
	keys := make([]string, len(events))
	groups := map[string]*metricGroup{}
	wanted := map[string]bool{}
	for _, name := range metric.Groups {
		wanted[name] = true
	}
	for i, event := range events {
		labels := event.Labels()
		if metric.Sum {
			for key := range labels {
				if wanted[key] == metric.Without {
					delete(labels, key)
				}
			}
		}
		key := Key(labels)
		keys[i] = key
		if _, found := groups[key]; !found {
			if len(groups) >= 1000 {
				return nil, errors.New("too many metric series; add grouping or narrow the selection")
			}
			groups[key] = &metricGroup{labels: labels}
		}
	}
	output := map[string]*Series{}
	left, right, samples := 0, 0, 0
	for timestamp := start; timestamp <= end; {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		for right < len(events) && events[right].Timestamp <= timestamp {
			group := groups[keys[right]]
			group.count++
			group.bytes += len(events[right].Body)
			right++
		}
		for left < right && events[left].Timestamp <= timestamp-int64(metric.Window) {
			group := groups[keys[left]]
			group.count--
			group.bytes -= len(events[left].Body)
			left++
		}
		for i := left; i < right; i++ {
			if events[i].Parsed["__error__"] != "" {
				return nil, errors.New("metric pipeline contains parser/conversion errors; filter or drop error labels explicitly")
			}
		}
		for key, group := range groups {
			if group.count == 0 {
				continue
			}
			samples++
			if samples > MaxSamples {
				return nil, errors.New("metric result exceeds the sample budget")
			}
			value := float64(group.count)
			if metric.Function == "bytes_over_time" || metric.Function == "bytes_rate" {
				value = float64(group.bytes)
			}
			if metric.Function == "rate" || metric.Function == "bytes_rate" {
				value /= metric.Window.Seconds()
			}
			series, found := output[key]
			if !found {
				series = &Series{Metric: group.labels}
				output[key] = series
			}
			point := []any{Seconds(timestamp), strconv.FormatFloat(value, 'g', -1, 64)}
			if instant {
				series.Value = point
			} else {
				series.Values = append(series.Values, point)
			}
		}
		if end-timestamp < step {
			break
		}
		timestamp += step
	}
	result := []Series{}
	for _, series := range output {
		result = append(result, *series)
	}
	sort.Slice(result, func(i, j int) bool { return Key(result[i].Metric) < Key(result[j].Metric) })
	return result, nil
}
