// SPDX-License-Identifier: AGPL-3.0-only
// query-inspect is a test helper. Browser acceptance uses the real AST parser,
// not regular-expression extraction, to associate results with their service.
package main

import (
	"encoding/json"
	"io"
	"os"
	"strings"

	"grafana-clickhouse-drilldown/internal/query"
)

func main() {
	var input []string
	if err := json.NewDecoder(io.LimitReader(os.Stdin, 1<<20)).Decode(&input); err != nil {
		os.Exit(1)
	}
	output := []map[string]any{}
	for _, text := range input {
		plan, err := query.ParseQuery(strings.ReplaceAll(text, "$__auto", "1m"))
		item := map[string]any{"valid": err == nil, "metric": plan.Metric != nil || plan.Health}
		if err == nil {
			for _, matcher := range plan.Selection.Matchers {
				if matcher.Name == "service_name" && matcher.Op == query.Equal {
					item["service"] = matcher.Value
				}
			}
		}
		output = append(output, item)
	}
	json.NewEncoder(os.Stdout).Encode(output)
}
