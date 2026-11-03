// SPDX-License-Identifier: AGPL-3.0-only

// Package sqlgen owns the shared, parameterized selection compiler. All future
// endpoint queries must use this compiler rather than extracting query text.
package sqlgen

import (
	"encoding/hex"
	"errors"
	"fmt"
	"grafana-clickhouse-drilldown/internal/record"
	"regexp"
	"strings"

	"github.com/prometheus/prometheus/model/labels"
	"grafana-clickhouse-drilldown/internal/query"
)

// ServiceExpression is the OTel default and a primary-key column in ClickStack.
const ServiceExpression = "ServiceName"

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
	// CountOnly omits message-byte totals from summary/metric aggregates.
	CountOnly bool
	// ParserGuard requests a conservative parser-safety count in metric SQL.
	ParserGuard bool
	SQL         string
	// Parameters are transport-encoded bindings for SQL, not raw query values.
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
		if m.Name == "service_name" && m.Op == query.Regex {
			// ClickHouse does not derive primary-key ranges from match(). Keep that
			// exact predicate and add a necessary, indexable finite set or prefix.
			matcher, err := labels.NewMatcher(labels.MatchRegexp, m.Name, m.Value)
			if err != nil {
				return Predicate{}, errors.New("invalid service regex")
			}
			if set := matcher.SetMatches(); len(set) > 0 && len(set) <= 64 {
				values := make([]string, 0, len(set))
				for _, value := range set {
					values = append(values, result.parameter(value))
				}
				conditions = append(conditions, "("+ServiceExpression+" IN ("+strings.Join(values, ", ")+"))")
			} else if re, err := regexp.Compile(`(?s:\A(?:` + m.Value + `)\z)`); err == nil {
				if prefix, _ := re.LiteralPrefix(); prefix != "" {
					conditions = append(conditions, "startsWith("+ServiceExpression+", "+result.parameter(prefix)+")")
				}
			}
		}
	}
	for _, f := range selection.Filters {
		condition, err := result.condition("Body", f.Op, f.Value, false)
		if err != nil {
			return Predicate{}, err
		}
		if condition != "" {
			conditions = append(conditions, condition)
		}
	}
	for _, filter := range selection.LabelPrefilters() {
		condition, err := result.labelPrefilter(filter)
		if err != nil {
			return Predicate{}, err
		}
		if condition != "" {
			conditions = append(conditions, condition)
		}
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
		if !label {
			if _, err := regexp.Compile(value); err != nil {
				return "", errors.New("invalid filter regex")
			}
			// Loki's line-regex simplifier implements contains/exists and its
			// own case folding. ClickHouse/RE2 is not even a safe superset for
			// both signs (notably .+ on newlines and (?i)k on Kelvin signs).
			// Keep every line regex residual-only until equivalence is proved.
			return "", nil
		}
		if label {
			// Prometheus label regexes are fully anchored and default to dot-all.
			// Strict text anchors avoid multiline flags weakening the outer bounds.
			value = `(?s:\A(?:` + value + `)\z)`
		}
		if _, err := regexp.Compile(value); err != nil {
			return "", errors.New("invalid filter regex")
		}
	}
	parameter := p.parameter(value)
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

// LabelExpression accepts only the two SQL-computable metric labels.
func LabelExpression(name string) (string, error) {
	switch name {
	case "service_name":
		return ServiceExpression, nil
	case "detected_level":
		return LevelExpression, nil
	default:
		return "", errors.New("unsupported SQL grouping label")
	}
}

func (p *Predicate) labelPrefilter(f *query.LabelPredicate) (string, error) {
	if f == nil {
		return "", nil
	}
	if f.Matcher == nil {
		left, err := p.labelPrefilter(f.Left)
		if err != nil {
			return "", err
		}
		right, err := p.labelPrefilter(f.Right)
		if err != nil {
			return "", err
		}
		if !f.And && (left == "" || right == "") {
			return "", nil
		}
		if left == "" {
			return right, nil
		}
		if right == "" {
			return left, nil
		}
		op := " OR "
		if f.And {
			op = " AND "
		}
		return "(" + left + op + right + ")", nil
	}
	m := f.Matcher
	expression := LevelExpression
	var scope, key, duplicateGuard string
	if m.Name != "detected_level" {
		var err error
		scope, key, err = record.Unalias(m.Name)
		if err != nil {
			return "", nil
		}
	}
	if m.Op == query.Regex || m.Op == query.NotRegex {
		// Pipeline labels use Loki's NewLabelFilter, not the Prometheus
		// stream matcher. Its concat/alternate simplifications can search
		// unanchored substrings, and its case folding differs from RE2.
		// No pipeline-label regex is currently eligible for SQL prefiltering.
		return "", nil
	}
	if scope != "" {
		parameter := p.parameter(key)
		column := "LogAttributes"
		if scope == "resource" {
			column = "ResourceAttributes"
		}
		// Map lookup returns empty for a missing key, exactly like omitted empty
		// metadata in Loki's labelValue. Keys are values, never SQL identifiers.
		expression = column + "[" + parameter + "]"
		// ClickHouse maps may contain duplicate keys; the Go decoder and map
		// subscript need not choose the same value. Such rows stay residual.
		duplicateGuard = "(countEqual(mapKeys(" + column + "), " + parameter + ") > 1)"
	}
	condition, err := p.condition(expression, m.Op, m.Value, true)
	return withDuplicateGuard(condition, duplicateGuard), err
}

func withDuplicateGuard(condition, guard string) string {
	if guard == "" {
		return condition
	}
	return "(" + guard + " OR " + condition + ")"
}

// Native String parameters use ClickHouse's escaped text format. Hex avoids
// driver/server escape handling changing backslashes or rejecting control
// bytes. Decode inside SQL; values still travel only as typed parameters.
func (p *Predicate) parameter(value string) string {
	name := fmt.Sprintf("filter_%d", len(p.Parameters))
	p.Parameters[name] = hex.EncodeToString([]byte(value))
	return "unhex({" + name + ":String})"
}
