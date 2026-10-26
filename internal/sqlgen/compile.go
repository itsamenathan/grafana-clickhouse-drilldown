// SPDX-License-Identifier: AGPL-3.0-only
package sqlgen

import "grafana-clickhouse-drilldown/internal/query"

// Compiled is one shared selection plan for every endpoint. SQL performs time,
// indexed-label and safe line-filter selection. The exact Loki implementations
// handle JSON/logfmt, error labels and field predicates as bounded residuals.
// They are not approximated with ClickHouse's different JSON/logfmt semantics.
type Compiled struct {
	Predicate Predicate
	Residual  *query.Evaluator
}

func Compile(selection query.Selection) (Compiled, error) {
	predicate, err := Selection(selection)
	if err != nil {
		return Compiled{}, err
	}
	residual, err := selection.NewEvaluator()
	if err != nil {
		return Compiled{}, err
	}
	return Compiled{predicate, residual}, nil
}
