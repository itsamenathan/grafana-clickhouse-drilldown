// SPDX-License-Identifier: AGPL-3.0-only

// Package sqlgen owns the shared, parameterized selection compiler. All future
// endpoint queries must use this compiler rather than extracting query text.
package sqlgen

import (
	"errors"
	"fmt"
	"regexp"
	"strings"

	"grafana-clickhouse-drilldown/internal/query"
)

// ServiceExpression matches the live SQL Logs Explorer, checked 2026-10-22.
// Historical stored ServiceName values are not rewritten.
const ServiceExpression = "coalesce(nullIf(LogAttributes['journald.CONTAINER_NAME'], ''), nullIf(LogAttributes['journald._SYSTEMD_UNIT'], ''), ServiceName)"

// LevelExpression uses known severity text first, then standard OTel numbers.
// Unknown values stay unknown, rather than guessing from message substrings.
const LevelExpression = `multiIf(
 lowerUTF8(SeverityText) IN ('trace', 'debug', 'info', 'warn', 'error', 'critical', 'fatal'), lowerUTF8(SeverityText),
 lowerUTF8(SeverityText) = 'warning', 'warn',
 lowerUTF8(SeverityText) IN ('information', 'notice'), 'info',
 lowerUTF8(SeverityText) IN ('err', 'severe'), 'error',
 lowerUTF8(SeverityText) IN ('crit', 'alert'), 'critical',
 lowerUTF8(SeverityText) IN ('panic', 'emerg', 'emergency'), 'fatal',
 SeverityNumber BETWEEN 21 AND 24, 'fatal',
 SeverityNumber BETWEEN 17 AND 20, 'error',
 SeverityNumber BETWEEN 13 AND 16, 'warn',
 SeverityNumber BETWEEN 9 AND 12, 'info',
 SeverityNumber BETWEEN 5 AND 8, 'debug',
 SeverityNumber BETWEEN 1 AND 4, 'trace', 'unknown')`

type Predicate struct {
	SQL        string
	Parameters map[string]string
}

func Selection(selection query.Selection) (Predicate, error) {
	if len(selection.Matchers) == 0 || len(selection.Matchers) > query.MaxMatchers || len(selection.Filters) > query.MaxFilters {
		return Predicate{}, errors.New("invalid selection size")
	}
	result := Predicate{Parameters: make(map[string]string)}
	var conditions []string
	for _, m := range selection.Matchers {
		var expression string
		switch m.Name {
		case "service_name":
			expression = ServiceExpression
		case "detected_level":
			expression = LevelExpression
		default:
			return Predicate{}, errors.New("unsupported stream label")
		}
		condition, err := result.condition(expression, m.Op, m.Value, true)
		if err != nil {
			return Predicate{}, err
		}
		conditions = append(conditions, condition)
	}
	for _, f := range selection.Filters {
		condition, err := result.condition("Body", f.Op, f.Value, false)
		if err != nil {
			return Predicate{}, err
		}
		conditions = append(conditions, condition)
	}
	result.SQL = strings.Join(conditions, " AND ")
	return result, nil
}

func (p *Predicate) condition(expression string, op query.Operator, value string, label bool) (string, error) {
	if len(value) > query.MaxQueryBytes {
		return "", errors.New("filter value too large")
	}
	if op != query.Equal && op != query.NotEqual && op != query.Regex && op != query.NotRegex {
		return "", errors.New("unsupported filter operator")
	}
	if op == query.Regex || op == query.NotRegex {
		if label {
			// Prometheus label regexes are fully anchored and default to dot-all.
			// Strict text anchors avoid multiline flags weakening the outer bounds.
			value = `(?s:\A(?:` + value + `)\z)`
		} else {
			// ClickHouse's match() defaults to dot-all, Loki line filters do not.
			// User inline flags inside the group retain their normal semantics.
			value = `(?-s:` + value + `)`
		}
		if _, err := regexp.Compile(value); err != nil {
			return "", errors.New("invalid filter regex")
		}
	}
	name := fmt.Sprintf("filter_%d", len(p.Parameters))
	p.Parameters[name] = value
	parameter := "{" + name + ":String}"
	var condition string
	if op == query.Regex || op == query.NotRegex {
		condition = "match(" + expression + ", " + parameter + ")"
	} else if label {
		condition = "(" + expression + " = " + parameter + ")"
	} else {
		condition = "(position(Body, " + parameter + ") > 0)"
	}
	if op == query.NotEqual || op == query.NotRegex {
		condition = "NOT (" + condition + ")"
	}
	return "(" + condition + ")", nil
}
