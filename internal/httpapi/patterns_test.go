// SPDX-License-Identifier: AGPL-3.0-only
package httpapi

import (
	"encoding/json"
	"net/http/httptest"
	"net/url"
	"testing"

	"grafana-clickhouse-drilldown/internal/engine"
	"grafana-clickhouse-drilldown/internal/limits"
)

func TestPatternsEmptyCutoffCohortKeepsCoverageContract(t *testing.T) {
	store := &memoryStore{}
	for i := 0; i < 100; i++ {
		store.add(row(5, "request 1 completed for user 2"))
	}
	response := httptest.NewRecorder()
	params := url.Values{"query": {`{service_name="api"}`}, "start": {"0"}, "end": {"10"}, "step": {"10s"}}
	New(store, "test", Options{Limits: limits.Config{PatternLines: 60}}).ServeHTTP(response, httptest.NewRequest("GET", "/loki/api/v1/patterns?"+params.Encode(), nil))
	var body struct {
		Status string           `json:"status"`
		Data   []engine.Pattern `json:"data"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if response.Code != 200 || body.Status != "success" || body.Data == nil || len(body.Data) != 0 || response.Header().Get("X-Patterns-Covered-Start") != "6" || response.Header().Get("X-Patterns-Sampled") != "true" || response.Header().Get("X-Patterns-Lines") != "0" {
		t.Fatalf("empty cohort changed response or coverage: %d %v %s", response.Code, response.Header(), response.Body.String())
	}
}
