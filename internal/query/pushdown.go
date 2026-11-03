// SPDX-License-Identifier: AGPL-3.0-only
package query

import (
	"github.com/grafana/loki/v3/pkg/logql/log"
	"github.com/grafana/loki/v3/pkg/logql/syntax"
	"github.com/prometheus/prometheus/model/labels"
)

// LabelPredicate preserves boolean structure. SQL may extract a necessary
// conjunct, but must never extract just one arm of a disjunction.
type LabelPredicate struct {
	Matcher     *Matcher
	Left, Right *LabelPredicate
	And         bool
}

// LabelPrefilters returns filters before any stage that can change labels.
// Even metadata names can be parsed when the stored value is missing/empty.
func (s Selection) LabelPrefilters() []*LabelPredicate {
	var result []*LabelPredicate
	for _, stage := range s.stages {
		switch e := stage.(type) {
		case *syntax.LineFilterExpr:
		case *syntax.LabelFilterExpr:
			result = append(result, labelPredicate(e.LabelFilterer))
		default:
			return result
		}
	}
	return result
}

func labelPredicate(filter log.LabelFilterer) *LabelPredicate {
	var m *labels.Matcher
	switch f := filter.(type) {
	case *log.BinaryLabelFilter:
		return &LabelPredicate{Left: labelPredicate(f.Left), Right: labelPredicate(f.Right), And: f.And}
	case *log.StringLabelFilter:
		m = f.Matcher
	case *log.LineFilterLabelFilter:
		m = f.Matcher
	default:
		return nil
	}
	if m == nil {
		return nil
	}
	var op Operator
	switch m.Type {
	case labels.MatchEqual:
		op = Equal
	case labels.MatchNotEqual:
		op = NotEqual
	case labels.MatchRegexp:
		op = Regex
	case labels.MatchNotRegexp:
		op = NotRegex
	default:
		return nil
	}
	return &LabelPredicate{Matcher: &Matcher{Name: m.Name, Op: op, Value: m.Value}}
}

func exactLineStage(e *syntax.LineFilterExpr) bool {
	for ; e != nil; e = e.Left {
		// Only byte-exact substring predicates have a SQL equivalence proof.
		// Regex stages must run Loki's simplifier and filter, even if the SQL
		// parser/drop guard would otherwise permit aggregation.
		if e.Or != nil || e.IsOrChild || e.Op != "" || (e.Ty != log.LineMatchEqual && e.Ty != log.LineMatchNotEqual) {
			return false
		}
	}
	return true
}

// SQLComplete reports that SQL implements every row predicate and no stage
// changes labels. Attribute prefilters remain residual and do not qualify.
func (s Selection) SQLComplete() bool {
	for _, stage := range s.stages {
		e, ok := stage.(*syntax.LineFilterExpr)
		if !ok || !exactLineStage(e) {
			return false
		}
	}
	return true
}

// CanAggregate reports whether counts/bytes and output labels are computable
// without running a residual pipeline. Unsummed/without metrics retain arbitrary
// metadata. Parser pipelines need GuardedAggregate and a zero SQL guard:
// drops reset internal errors rather than deleting identically named parsed
// labels. The fallback preserves Loki's grouping-dependent parser hints.
func (s Selection) CanAggregate(m Metric) bool {
	return m.RetainsOnly("service_name", "detected_level") && s.SQLComplete()
}

// GuardedAggregate permits only SQL-complete selection followed by parameterless
// json/logfmt parsers and one final, unconditional drop of both error labels.
// The store must prove the body guard is zero before using any aggregates.
func (s Selection) GuardedAggregate(m Metric) bool {
	if !m.RetainsOnly("service_name", "detected_level") {
		return false
	}
	parser := false
	dropped := false
	for _, stage := range s.stages {
		if dropped {
			return false
		}
		switch e := stage.(type) {
		case *syntax.LineFilterExpr:
			if parser || !exactLineStage(e) {
				return false
			}
		case *syntax.LineParserExpr:
			if e.Op != "json" {
				return false
			}
			parser = true
		case *syntax.LogfmtParserExpr:
			if e.Strict || e.KeepEmpty {
				return false
			}
			parser = true
		case *syntax.DropLabelsExpr:
			if !parser || e.HasNamedMatchers() || len(e.Names()) != 2 {
				return false
			}
			names := map[string]bool{}
			for _, label := range e.Names() {
				if label != "__error__" && label != "__error_details__" {
					return false
				}
				names[label] = true
			}
			if len(names) != 2 {
				return false
			}
			dropped = true
		default:
			return false
		}
	}
	return parser && dropped
}
