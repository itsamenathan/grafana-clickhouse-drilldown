// SPDX-License-Identifier: AGPL-3.0-only
package httpapi

import (
	"errors"
	"net/http"
	"sort"
	"strconv"
	"time"

	"grafana-clickhouse-drilldown/internal/engine"
	"grafana-clickhouse-drilldown/internal/record"
)

func (a *queryAPI) volume(w http.ResponseWriter, r *http.Request) {
	if err := parameters(r, "query start end limit targetLabels aggregateBy step"); err != nil {
		fail(w, err)
		return
	}
	if target := r.Form.Get("targetLabels"); target != "" && target != "service_name" {
		fail(w, errors.New("volume supports the indexed service_name target only"))
		return
	}
	if aggregate := r.Form.Get("aggregateBy"); aggregate != "" && aggregate != "series" {
		fail(w, errors.New("unsupported volume aggregation mode"))
		return
	}
	start, end, err := a.bounds(r)
	if err != nil {
		fail(w, err)
		return
	}
	limit, err := integer(r, "limit", 100, 5000)
	if err != nil {
		fail(w, err)
		return
	}
	if r.URL.Path != "/loki/api/v1/index/volume_range" {
		summaries, supported, err := a.summarized(r, start, end, false)
		if err != nil {
			fail(w, err)
			return
		}
		if supported {
			if r.URL.Path == "/loki/api/v1/index/stats" {
				var entries, bytes uint64
				for _, summary := range summaries {
					entries += summary.Entries
					bytes += summary.Bytes
				}
				respond(w, http.StatusOK, map[string]any{"streams": len(summaries), "chunks": 0, "entries": entries, "bytes": bytes})
				return
			}
			sort.Slice(summaries, func(i, j int) bool {
				if summaries[i].Bytes == summaries[j].Bytes {
					return summaries[i].Service < summaries[j].Service
				}
				return summaries[i].Bytes > summaries[j].Bytes
			})
			if len(summaries) > limit {
				summaries = summaries[:limit]
			}
			result := []engine.Series{}
			for _, summary := range summaries {
				result = append(result, engine.Series{Metric: map[string]string{"service_name": summary.Service}, Value: []any{engine.Seconds(end), strconv.FormatUint(summary.Bytes, 10)}})
			}
			respond(w, http.StatusOK, success("vector", result))
			return
		}
	}
	step := int64(0)
	if r.URL.Path == "/loki/api/v1/index/volume_range" {
		d, err := duration(r.Form.Get("step"), time.Minute)
		if err != nil || (end-start)/int64(d) >= engine.MaxPoints {
			fail(w, errors.New("invalid volume step or too many points"))
			return
		}
		step = int64(d)
	}
	source, err := requestSource(r)
	if err != nil {
		fail(w, err)
		return
	}
	// Volumes need only the SQL-computed service and the message length.
	counts := map[string]int{}
	buckets := map[string]map[int64]int{}
	streams := map[string]bool{}
	entries, retained := 0, 0
	err = a.engine.Each(r.Context(), source, start, end, record.ScanOptions{}, func(event engine.Event) error {
		name := event.Stream["service_name"]
		key := engine.Key(event.Stream)
		if _, exists := counts[name]; !exists {
			if len(counts) >= 5000 {
				return errors.New("too many volume series; narrow the selection")
			}
			if size := len(name) + len(key) + 512; size > a.limits.MaxRetainedBytes-retained {
				return engine.ErrRetainedLimit
			} else {
				retained += size
			}
		}
		counts[name] += len(event.Body)
		streams[key] = true
		entries++
		if step > 0 {
			if buckets[name] == nil {
				buckets[name] = map[int64]int{}
			}
			index := (event.Timestamp - start) / step
			if _, exists := buckets[name][index]; !exists {
				if 128 > a.limits.MaxRetainedBytes-retained {
					return engine.ErrRetainedLimit
				}
				retained += 128
			}
			buckets[name][index] += len(event.Body)
		}
		return nil
	})
	if err != nil {
		fail(w, err)
		return
	}
	if r.URL.Path == "/loki/api/v1/index/stats" {
		bytes := 0
		for _, count := range counts {
			bytes += count
		}
		// ClickHouse has no Loki chunks. Message bytes and event counts are
		// exact over this selection, not Loki index-size estimates.
		respond(w, http.StatusOK, map[string]any{"streams": len(streams), "chunks": 0, "entries": entries, "bytes": bytes})
		return
	}
	names := sortedKeys(counts)
	sort.Slice(names, func(i, j int) bool {
		if counts[names[i]] == counts[names[j]] {
			return names[i] < names[j]
		}
		return counts[names[i]] > counts[names[j]]
	})
	if len(names) > limit {
		names = names[:limit]
	}
	result := []engine.Series{}
	if r.URL.Path == "/loki/api/v1/index/volume" {
		for _, name := range names {
			result = append(result, engine.Series{Metric: map[string]string{"service_name": name}, Value: []any{engine.Seconds(end), strconv.Itoa(counts[name])}})
		}
		respond(w, http.StatusOK, success("vector", result))
		return
	}
	sampleCount := 0
	for _, name := range names {
		series := engine.Series{Metric: map[string]string{"service_name": name}, Values: [][]any{}}
		for t := start; t < end; {
			next := t + step
			if next > end || next < t {
				next = end
			}
			index := (t - start) / step
			if count, found := buckets[name][index]; found {
				sampleCount++
				series.Values = append(series.Values, []any{engine.Seconds(next), strconv.Itoa(count)})
			}
			if sampleCount > engine.MaxSamples {
				fail(w, errors.New("volume result exceeds sample budget"))
				return
			}
			t = next
		}
		result = append(result, series)
	}
	respond(w, http.StatusOK, success("matrix", result))
}
