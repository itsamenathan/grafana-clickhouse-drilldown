// SPDX-License-Identifier: AGPL-3.0-only
package httpapi

import (
	"errors"
	"net/http"
	"strconv"
	"time"

	"grafana-clickhouse-drilldown/internal/engine"
	"grafana-clickhouse-drilldown/internal/query"
)

// Loki's patterns API (pkg/loghttp/patterns.go) accepts only stream matchers
// and at most 11,000 points per series.
const maxPatternPoints = 11000

func (a *queryAPI) patterns(w http.ResponseWriter, r *http.Request) {
	if err := parameters(r, "query start end step"); err != nil {
		fail(w, err)
		return
	}
	start, end, err := a.bounds(r)
	if err != nil {
		fail(w, err)
		return
	}
	selection, err := query.Parse(r.Form.Get("query"))
	if err != nil {
		fail(w, err)
		return
	}
	if !selection.IsSelector() {
		fail(w, errors.New("patterns accept a stream selector only, as in Loki"))
		return
	}
	// Loki's default step targets about 250 points, at least one second.
	fallback := max(time.Duration((end-start)/250).Truncate(time.Second), time.Second)
	step, err := duration(r.Form.Get("step"), fallback)
	if err != nil || (end-start)/int64(step) > maxPatternPoints {
		fail(w, errors.New("invalid pattern step or too many points"))
		return
	}
	source, err := engine.Compile(selection)
	if err != nil {
		fail(w, err)
		return
	}
	patterns, err := a.engine.PatternsRecent(r.Context(), source, start, end, int64(step), engine.PatternOptions{Lines: a.limits.PatternLines, RetainedBytes: a.limits.MaxRetainedBytes})
	if err != nil {
		fail(w, err)
		return
	}
	w.Header().Set("X-Patterns-Covered-Start", strconv.FormatInt(patterns.CoveredStart, 10))
	w.Header().Set("X-Patterns-Sampled", strconv.FormatBool(patterns.Sampled))
	w.Header().Set("X-Patterns-Lines", strconv.Itoa(patterns.Lines))
	respond(w, http.StatusOK, map[string]any{"status": "success", "data": patterns.Patterns})
}
