// SPDX-License-Identifier: AGPL-3.0-only
package httpapi

import (
	"context"
	"errors"
	"math"
	"net/http"
	"strings"
	"time"

	"grafana-clickhouse-drilldown/internal/engine"
	"grafana-clickhouse-drilldown/internal/limits"
	"grafana-clickhouse-drilldown/internal/query"
)

type queryAPI struct {
	engine *engine.Engine
	db     Readiness
}

func fail(w http.ResponseWriter, err error) {
	status, kind := http.StatusBadRequest, "bad_data"
	if errors.Is(err, limits.ErrOverloaded) {
		status, kind = http.StatusTooManyRequests, "overloaded"
	} else if errors.Is(err, context.DeadlineExceeded) {
		status, kind = http.StatusGatewayTimeout, "timeout"
	} else if errors.Is(err, context.Canceled) {
		status, kind = 499, "canceled"
	}
	respond(w, status, apiError{"error", kind, err.Error()})
}

func success(resultType string, result any) map[string]any {
	return map[string]any{"status": "success", "data": map[string]any{"resultType": resultType, "result": result}}
}

func (a *queryAPI) query(w http.ResponseWriter, r *http.Request) {
	if err := parameters(r, "query start end time step limit direction"); err != nil {
		fail(w, err)
		return
	}
	plan, err := query.ParseQuery(r.Form.Get("query"))
	if err != nil {
		fail(w, err)
		return
	}
	instant := r.URL.Path == "/loki/api/v1/query"
	var start, end int64
	if instant {
		end, err = timestamp(r.Form.Get("time"), time.Now().Truncate(time.Millisecond).UnixNano())
		start = end
		if r.Form.Has("start") || r.Form.Has("end") || r.Form.Has("step") {
			err = errors.New("instant queries use time, not start/end/step")
		}
	} else {
		maximum := engine.MaxRange
		if plan.Metric == nil && !plan.Health {
			maximum = engine.MaxLogRange
		}
		start, end, err = boundsLimit(r, maximum)
		if r.Form.Has("time") {
			err = errors.New("range queries use start/end, not time")
		}
	}
	if err != nil {
		fail(w, err)
		return
	}
	if plan.Health {
		if !instant {
			fail(w, errors.New("the Grafana health expression is supported only as an instant query"))
			return
		}
		if err := a.db.Ready(r.Context()); err != nil {
			respond(w, http.StatusServiceUnavailable, apiError{"error", "unavailable", "database is not ready"})
			return
		}
		respond(w, http.StatusOK, success("vector", []engine.Series{{Metric: map[string]string{}, Value: []any{engine.Seconds(end), "2"}}}))
		return
	}
	if plan.Metric != nil {
		if !instant && !r.Form.Has("end") {
			end = end / int64(time.Millisecond) * int64(time.Millisecond)
		}
		if !instant && !r.Form.Has("start") {
			start = start / int64(time.Millisecond) * int64(time.Millisecond)
		}
		// Grafana may include common log parameters for metric requests. Loki
		// defines their effect only for log queries; validate but do not apply
		// them to vectors or matrices.
		if _, err := integer(r, "limit", 100, 5000); err != nil {
			fail(w, err)
			return
		}
		if direction := r.Form.Get("direction"); direction != "" && direction != "forward" && direction != "backward" {
			fail(w, errors.New("invalid direction"))
			return
		}
		step, stepErr := duration(r.Form.Get("step"), time.Second)
		if stepErr != nil || (end-start)/int64(step) >= engine.MaxPoints {
			fail(w, errors.New("invalid step or too many evaluation points"))
			return
		}
		if start%int64(time.Millisecond) != 0 || end%int64(time.Millisecond) != 0 || step%time.Millisecond != 0 {
			fail(w, errors.New("metric timestamps and steps must be millisecond-aligned; log timestamps retain nanoseconds"))
			return
		}
		if start < int64(plan.Metric.Window) || end == math.MaxInt64 {
			fail(w, errors.New("metric bounds exceed supported timestamps"))
			return
		}
		events, err := a.engine.Load(r.Context(), plan.Selection, start-int64(plan.Metric.Window), end+1)
		if err != nil {
			fail(w, err)
			return
		}
		result, err := engine.Metrics(r.Context(), events, *plan.Metric, start, end, int64(step), instant)
		if err != nil {
			fail(w, err)
			return
		}
		kind := "matrix"
		if instant {
			kind = "vector"
		}
		respond(w, http.StatusOK, success(kind, result))
		return
	}
	if instant {
		fail(w, errors.New("instant log queries are not supported; use query_range"))
		return
	}
	limit, err := integer(r, "limit", 100, 2000)
	if err != nil {
		fail(w, err)
		return
	}
	direction := r.Form.Get("direction")
	if direction == "" {
		direction = "backward"
	}
	if direction != "backward" && direction != "forward" {
		fail(w, errors.New("invalid log direction"))
		return
	}
	if r.Form.Has("step") {
		// Loki does not apply step to log rows. Context nevertheless sends a
		// step equal to its two-hour window; validate against the log bound.
		if _, err := durationLimit(r.Form.Get("step"), time.Second, engine.MaxLogRange); err != nil {
			fail(w, err)
			return
		}
	}
	events, err := a.engine.LoadLogs(r.Context(), plan.Selection, start, end, direction, limit)
	if err != nil {
		fail(w, err)
		return
	}
	flags := r.Header.Get("X-Loki-Response-Encoding-Flags")
	if flags != "" && flags != "categorize-labels" {
		fail(w, errors.New("unsupported response encoding flag"))
		return
	}
	response := success("streams", engine.Logs(events, direction, limit, flags != ""))
	if flags != "" {
		response["data"].(map[string]any)["encodingFlags"] = []string{"categorize-labels"}
	}
	respond(w, http.StatusOK, response)
}

func (a *queryAPI) route(w http.ResponseWriter, r *http.Request) {
	switch path := r.URL.Path; {
	case path == "/loki/api/v1/drilldown-limits":
		if err := parameters(r, ""); err != nil {
			fail(w, err)
			return
		}
		// This is the installed Drilldown 3.0.0 capability contract, not a
		// claim that a pattern ingester or a Loki deployment exists. Declaring
		// patterns disabled prevents unsupported background requests per source,
		// without changing Grafana's global plugin settings.
		respond(w, http.StatusOK, map[string]any{
			"version":                  "grafana-clickhouse-drilldown",
			"pattern_ingester_enabled": false,
			"limits": map[string]any{
				"max_entries_limit_per_query": 2000, "max_query_length": "1h", "max_query_range": "1h",
				"max_query_lookback": "0s", "query_timeout": "15s", "max_query_series": 1000,
				"volume_enabled": true, "volume_max_series": 5000, "retention_period": "0s",
				"pattern_persistence_enabled": false, "metric_aggregation_enabled": false,
				"discover_log_levels": true, "max_line_size_truncate": false,
			},
		})
	case path == "/loki/api/v1/query" || path == "/loki/api/v1/query_range":
		a.query(w, r)
	case path == "/loki/api/v1/labels" || path == "/loki/api/v1/series" || path == "/loki/api/v1/detected_labels" ||
		strings.HasPrefix(path, "/loki/api/v1/label/"):
		a.labels(w, r)
	case path == "/loki/api/v1/detected_fields" || strings.HasPrefix(path, "/loki/api/v1/detected_field/"):
		a.fields(w, r)
	case path == "/loki/api/v1/index/volume" || path == "/loki/api/v1/index/volume_range" || path == "/loki/api/v1/index/stats":
		a.volume(w, r)
	default:
		respond(w, http.StatusNotImplemented, apiError{"error", "unsupported", "endpoint is not implemented"})
	}
}
