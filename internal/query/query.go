// SPDX-License-Identifier: AGPL-3.0-only

// Package query parses complete LogQL expressions into the supported subset.
// Accepting syntax in Loki is not sufficient: every AST node must be supported.
package query

import (
	"errors"
	"fmt"
	"regexp"
	"slices"
	"time"

	"github.com/grafana/loki/v3/pkg/logql/log"
	"github.com/grafana/loki/v3/pkg/logql/syntax"
	"github.com/prometheus/prometheus/model/labels"
)

var ErrUnsupported = errors.New("unsupported LogQL")

const (
	MaxQueryBytes = 16384
	MaxMatchers   = 32
	MaxFilters    = 64
)

type Operator uint8

const (
	Equal Operator = iota
	NotEqual
	Regex
	NotRegex
)

type Matcher struct {
	Name  string
	Op    Operator
	Value string
}

type LineFilter struct {
	Op    Operator
	Value string
}

type Selection struct {
	Matchers []Matcher
	Filters  []LineFilter
	stages   []syntax.StageExpr
}

// Parse accepts only a log selection. ParseQuery also accepts the bounded
// metric expressions used by Drilldown. All AST stages are validated before
// SQL or a residual pipeline can be constructed.
func Parse(input string) (Selection, error) {
	if len(input) == 0 || len(input) > MaxQueryBytes {
		return Selection{}, fmt.Errorf("query must contain 1..%d bytes", MaxQueryBytes)
	}
	expr, err := syntax.ParseExpr(input)
	if err != nil {
		// Do not include the user's query or filter values in public errors.
		return Selection{}, errors.New("invalid LogQL syntax")
	}
	return selectionFromExpr(expr)
}

func selectionFromExpr(expr syntax.Expr) (Selection, error) {
	var matchers *syntax.MatchersExpr
	var stages syntax.MultiStageExpr
	switch e := expr.(type) {
	case *syntax.MatchersExpr:
		matchers = e
	case *syntax.PipelineExpr:
		matchers, stages = e.Left, e.MultiStages
	default:
		return Selection{}, fmt.Errorf("%w: expression %T", ErrUnsupported, expr)
	}
	if matchers == nil || len(matchers.Mts) == 0 || len(matchers.Mts) > MaxMatchers {
		return Selection{}, errors.New("invalid matcher count")
	}
	selection := Selection{}
	for _, m := range matchers.Mts {
		if m.Name != "service_name" && m.Name != "detected_level" {
			return Selection{}, fmt.Errorf("%w: stream label", ErrUnsupported)
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
			return Selection{}, fmt.Errorf("%w: matcher operator", ErrUnsupported)
		}
		selection.Matchers = append(selection.Matchers, Matcher{m.Name, op, m.Value})
	}
	if len(stages) > MaxFilters {
		return Selection{}, errors.New("too many pipeline stages")
	}
	lineNodes := 0
	for _, stage := range stages {
		switch e := stage.(type) {
		case *syntax.LineFilterExpr:
			stack := []*syntax.LineFilterExpr{e}
			pushable := true
			for len(stack) > 0 {
				n := stack[len(stack)-1]
				stack = stack[:len(stack)-1]
				lineNodes++
				if lineNodes > MaxFilters {
					return Selection{}, errors.New("too many line filters")
				}
				if n.Op != "" {
					return Selection{}, fmt.Errorf("%w: function line filter", ErrUnsupported)
				}
				if n.Or != nil {
					pushable = false
					stack = append(stack, n.Or)
				}
				if n.Left != nil {
					stack = append(stack, n.Left)
				}
				if n.Ty == log.LineMatchPattern || n.Ty == log.LineMatchNotPattern {
					pushable = false
				}
			}
			if pushable {
				if err := appendFilters(&selection, e); err != nil {
					return Selection{}, err
				}
			}
		case *syntax.LineParserExpr:
			if e.Op != syntax.OpParserTypeJSON || e.Param != "" {
				return Selection{}, fmt.Errorf("%w: parser", ErrUnsupported)
			}
		case *syntax.LogfmtParserExpr, *syntax.JSONExpressionParserExpr,
			*syntax.LogfmtExpressionParserExpr, *syntax.LabelFilterExpr, *syntax.DropLabelsExpr:
			// Exact Loki stage implementations run after parameterized SQL
			// selection. These stages do not rewrite the message, so pushing
			// exact substring predicates down cannot reorder transformations.
		default:
			return Selection{}, fmt.Errorf("%w: pipeline stage %T", ErrUnsupported, stage)
		}
		if _, err := stage.Stage(); err != nil {
			return Selection{}, errors.New("invalid pipeline stage")
		}
		selection.stages = append(selection.stages, stage)
	}
	return selection, nil
}

type Metric struct {
	Function string
	Window   time.Duration
	Sum      bool
	Groups   []string
	Without  bool
	// unwrap is set when samples are label values rather than lines.
	unwrap     *syntax.RangeAggregationExpr
	expression syntax.SampleExpr
}

// Unwrap reports whether samples come from an unwrapped label value.
func (m Metric) Unwrap() bool { return m.unwrap != nil }

// AverageUsesSumAndCount follows Loki v3.7.9's shard mapper. A grouping
// other than without(), or a label-reducing pipeline, rewrites the average
// into a sum divided by a separate count. Ordinary averages use an
// incremental mean, which has different overflow and rounding behavior.
func (m Metric) AverageUsesSumAndCount() bool {
	return m.Function == syntax.OpRangeTypeAvg && m.unwrap != nil &&
		(syntax.ReducesLabels(m.unwrap) || m.unwrap.Grouping != nil && !m.unwrap.Grouping.Noop())
}

// NewAverageCountExtractor reproduces the denominator of Loki's grouped-avg
// rewrite, including its removal of unwrap and post-unwrap filters. It is
// deliberately separate from counting successful numeric conversions.
func (m Metric) NewAverageCountExtractor() (log.SampleExtractor, error) {
	if !m.AverageUsesSumAndCount() {
		return nil, errors.New("metric has no average count expression")
	}
	left, err := m.unwrap.Left.WithoutUnwrap()
	if err != nil {
		return nil, errors.New("invalid average count expression")
	}
	grouping := syntax.Grouping{Without: true}
	if m.unwrap.Grouping != nil {
		grouping.Without = m.unwrap.Grouping.Without
		grouping.Groups = append([]string(nil), m.unwrap.Grouping.Groups...)
	}
	if grouping.Without {
		grouping.Groups = append(grouping.Groups, m.unwrap.Left.Unwrap.Identifier)
	}
	count := &syntax.VectorAggregationExpr{
		Operation: syntax.OpTypeSum, Grouping: &grouping,
		Left: &syntax.RangeAggregationExpr{Operation: syntax.OpRangeTypeCount, Left: left},
	}
	extractors, err := count.Extractors()
	if err != nil || len(extractors) != 1 {
		return nil, errors.New("invalid average count expression")
	}
	return extractors[0], nil
}

// HasExtractor reports whether the metric came from a validated sample AST.
func (m Metric) HasExtractor() bool { return m.expression != nil || m.unwrap != nil }

// NewExtractor preserves Loki's parser hints, conversions, errors and grouping.
// Extractors are request-local.
func (m Metric) NewExtractor() (log.SampleExtractor, error) {
	expression := m.expression
	if expression == nil {
		if m.unwrap == nil {
			return nil, errors.New("metric has no parsed sample expression")
		}
		expression = m.unwrap
	}
	extractors, err := expression.Extractors()
	if err != nil || len(extractors) != 1 {
		return nil, errors.New("invalid sample expression")
	}
	return extractors[0], nil
}

type Plan struct {
	Selection Selection
	Metric    *Metric
	Health    bool
}

func ParseQuery(input string) (Plan, error) {
	if len(input) == 0 || len(input) > MaxQueryBytes {
		return Plan{}, errors.New("invalid query size")
	}
	expr, err := syntax.ParseExpr(input)
	if err != nil {
		return Plan{}, errors.New("invalid LogQL syntax")
	}
	if bin, ok := expr.(*syntax.BinOpExpr); ok && bin.Op == "+" {
		left, lok := bin.SampleExpr.(*syntax.VectorExpr)
		right, rok := bin.RHS.(*syntax.VectorExpr)
		plain := bin.Opts == nil || !bin.Opts.ReturnBool && (bin.Opts.VectorMatching == nil ||
			bin.Opts.VectorMatching.Card == syntax.CardOneToOne && !bin.Opts.VectorMatching.On && len(bin.Opts.VectorMatching.MatchingLabels) == 0 && len(bin.Opts.VectorMatching.Include) == 0)
		if lok && rok && left.Val == 1 && right.Val == 1 && plain {
			return Plan{Health: true}, nil
		}
	}
	original, _ := expr.(syntax.SampleExpr)
	var metric *Metric
	if sum, ok := expr.(*syntax.VectorAggregationExpr); ok {
		if sum.Operation != syntax.OpTypeSum || sum.Params != 0 || sum.Grouping == nil || len(sum.Grouping.Groups) > MaxMatchers {
			return Plan{}, fmt.Errorf("%w: vector aggregation", ErrUnsupported)
		}
		metric = &Metric{Sum: true, Groups: append([]string(nil), sum.Grouping.Groups...), Without: sum.Grouping.Without}
		expr = sum.Left
	}
	if r, ok := expr.(*syntax.RangeAggregationExpr); ok {
		if r.Left == nil || r.Params != nil || r.Left.Offset != 0 {
			return Plan{}, fmt.Errorf("%w: range aggregation", ErrUnsupported)
		}
		if r.Left.Unwrap != nil {
			// Drilldown charts numeric fields with avg_over_time(... | unwrap
			// [bytes|duration](field) | __error__="" [$__auto]) by ().
			if metric != nil || r.Operation != syntax.OpRangeTypeAvg && r.Operation != syntax.OpRangeTypeSum {
				return Plan{}, fmt.Errorf("%w: unwrapped range aggregation", ErrUnsupported)
			}
			switch r.Left.Unwrap.Operation {
			case "", syntax.OpConvBytes, syntax.OpConvDuration, syntax.OpConvDurationSeconds:
			default:
				return Plan{}, fmt.Errorf("%w: unwrap conversion", ErrUnsupported)
			}
			if r.Grouping != nil && len(r.Grouping.Groups) > MaxMatchers {
				return Plan{}, errors.New("too many grouping labels")
			}
			if _, err := r.Extractors(); err != nil {
				return Plan{}, errors.New("invalid unwrap expression")
			}
			metric = &Metric{unwrap: r}
		} else if r.Grouping != nil || (r.Operation != syntax.OpRangeTypeCount && r.Operation != syntax.OpRangeTypeRate && r.Operation != syntax.OpRangeTypeBytes && r.Operation != syntax.OpRangeTypeBytesRate) {
			return Plan{}, fmt.Errorf("%w: range aggregation", ErrUnsupported)
		}
		if r.Left.Interval <= 0 {
			return Plan{}, errors.New("metric window must be positive")
		}
		if metric == nil {
			metric = &Metric{}
		}
		metric.Function, metric.Window = r.Operation, r.Left.Interval
		expr = r.Left.Left
	} else if metric != nil {
		return Plan{}, fmt.Errorf("%w: sum requires a supported range aggregation", ErrUnsupported)
	}
	selection, err := selectionFromExpr(expr)
	if err != nil {
		return Plan{}, err
	}
	if metric != nil {
		metric.expression = original
	}
	return Plan{Selection: selection, Metric: metric}, nil
}

func (s Selection) IsSelector() bool { return len(s.stages) == 0 }

// FiltersOnLabels reports whether the residual pipeline can drop a record
// based on its labels. Such a pipeline needs every record's complete metadata;
// other stages read only the body and the SQL-computed service and level.
func (s Selection) FiltersOnLabels() bool {
	for _, stage := range s.stages {
		if _, ok := stage.(*syntax.LabelFilterExpr); ok {
			return true
		}
	}
	return false
}

// RetainsOnly reports whether every output series label must be one of names.
// Without a sum, or with sum without(...), any record label can survive.
func (m Metric) RetainsOnly(names ...string) bool {
	if !m.Sum || m.Without || m.unwrap != nil {
		return false
	}
	for _, group := range m.Groups {
		if !slices.Contains(names, group) {
			return false
		}
	}
	return true
}

// CanLimitLogs is deliberately conservative. Parser/extraction/drop stages
// preserve every input row; predicates must finish residual evaluation before
// a global log limit can be applied. Never push a limit through a filter.
func (s Selection) CanLimitLogs() bool {
	for _, stage := range s.stages {
		switch stage.(type) {
		case *syntax.LineParserExpr, *syntax.LogfmtParserExpr,
			*syntax.JSONExpressionParserExpr, *syntax.LogfmtExpressionParserExpr,
			*syntax.DropLabelsExpr:
		default:
			return false
		}
	}
	return true
}

func appendFilters(s *Selection, filter *syntax.LineFilterExpr) error {
	// Loki links chained line filters through Left. Walk iteratively and reverse
	// so SQL parameters retain source order without recursive stack growth.
	var chain []*syntax.LineFilterExpr
	for current := filter; current != nil; current = current.Left {
		if len(s.Filters)+len(chain) >= MaxFilters {
			return errors.New("too many line filters")
		}
		if current.Or != nil || current.IsOrChild || current.Op != "" {
			return fmt.Errorf("%w: compound or function line filter", ErrUnsupported)
		}
		chain = append(chain, current)
	}
	for i := len(chain) - 1; i >= 0; i-- {
		current := chain[i]
		var op Operator
		switch current.Ty {
		case log.LineMatchEqual:
			op = Equal
		case log.LineMatchNotEqual:
			op = NotEqual
		case log.LineMatchRegexp:
			op = Regex
		case log.LineMatchNotRegexp:
			op = NotRegex
		default:
			return fmt.Errorf("%w: line operator", ErrUnsupported)
		}
		if op == Regex || op == NotRegex {
			if _, err := regexp.Compile(current.Match); err != nil {
				return errors.New("invalid line filter regex")
			}
		}
		s.Filters = append(s.Filters, LineFilter{op, current.Match})
	}
	return nil
}
