// SPDX-License-Identifier: AGPL-3.0-only
package httpapi

import (
	"errors"
	"net/http"
	"sort"
	"strconv"
	"time"

	"grafana-clickhouse-drilldown/internal/engine"
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
	start, end, err := bounds(r)
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
		summaries, supported, err := a.summarized(r, start, end)
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
	events, err := a.selected(r, start, end)
	if err != nil {
		fail(w, err)
		return
	}
	if r.URL.Path == "/loki/api/v1/index/stats" {
		streams := map[string]bool{}
		bytes := 0
		for _, event := range events {
			streams[engine.Key(event.Stream)] = true
			bytes += len(event.Body)
		}
		// ClickHouse has no Loki chunks. Message bytes and event counts are
		// exact over this bounded selection, not Loki index-size estimates.
		respond(w, http.StatusOK, map[string]any{"streams": len(streams), "chunks": 0, "entries": len(events), "bytes": bytes})
		return
	}
	counts := map[string]int{}
	for _, event := range events {
		counts[event.Stream["service_name"]] += len(event.Body)
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
	step, err := duration(r.Form.Get("step"), time.Minute)
	if err != nil || (end-start)/int64(step) >= engine.MaxPoints {
		fail(w, errors.New("invalid volume step or too many points"))
		return
	}
	sampleCount := 0
	for _, name := range names {
		series := engine.Series{Metric: map[string]string{"service_name": name}, Values: [][]any{}}
		buckets := map[int64]int{}
		for _, event := range events {
			if event.Stream["service_name"] == name {
				bucket := (event.Timestamp - start) / int64(step)
				buckets[bucket] += len(event.Body)
			}
		}
		for t := start; t < end; {
			next := t + int64(step)
			if next > end || next < t {
				next = end
			}
			index := (t - start) / int64(step)
			if count, found := buckets[index]; found {
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
