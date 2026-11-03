// SPDX-License-Identifier: AGPL-3.0-only
package query

import (
	"context"
	"errors"

	"github.com/grafana/loki/v3/pkg/logql/log"
	"github.com/grafana/loki/v3/pkg/logql/syntax"
	"github.com/prometheus/prometheus/model/labels"
)

type Processed struct {
	Stream, Metadata, Parsed map[string]string
	Origins                  map[string]string
}

type evaluatorStage struct {
	stage  log.Stage
	parser string
}

// Evaluator is request-local, not concurrent. It executes the exact validated
// Loki stage implementations as residual predicates over a bounded SQL scan.
// Aggregation bypasses it only after proving that SQL implements every stage.
// Overflowing a scan is an error, never partial success. No line-changing
// stage is supported by this implementation.
type Evaluator struct{ stages []evaluatorStage }

func (s Selection) NewEvaluator() (*Evaluator, error) {
	e := &Evaluator{}
	for _, expr := range s.stages {
		stage, err := expr.Stage()
		if err != nil {
			return nil, errors.New("invalid pipeline stage")
		}
		parser := ""
		switch expr.(type) {
		case *syntax.LineParserExpr, *syntax.JSONExpressionParserExpr:
			parser = "json"
		case *syntax.LogfmtParserExpr, *syntax.LogfmtExpressionParserExpr:
			parser = "logfmt"
		}
		e.stages = append(e.stages, evaluatorStage{stage, parser})
	}
	return e, nil
}

func (e *Evaluator) Process(ctx context.Context, ts int64, body string, stream, metadata map[string]string) (Processed, bool, error) {
	base := labels.FromMap(stream)
	builder := log.NewBaseLabelsBuilder().ForLabels(base, base.Hash())
	builder.Add(log.StructuredMetadataLabel, labels.FromMap(metadata))
	origins := map[string]string{}
	line := []byte(body)
	for _, stage := range e.stages {
		if err := ctx.Err(); err != nil {
			return Processed{}, false, err
		}
		var before map[string]string
		if stage.parser != "" {
			before = builder.LabelsResult().Parsed().Map()
		}
		var ok bool
		line, ok = stage.stage.Process(ts, line, builder)
		if !ok {
			return Processed{}, false, nil
		}
		if stage.parser != "" {
			builder.LabelsResult().Parsed().Range(func(label labels.Label) {
				if old, found := before[label.Name]; !found || old != label.Value {
					origins[label.Name] = stage.parser
				}
			})
		}
	}
	result := builder.LabelsResult()
	if result.Labels().Len() > 256 {
		return Processed{}, false, errors.New("too many fields in a log record")
	}
	return Processed{result.Stream().Map(), result.StructuredMetadata().Map(), result.Parsed().Map(), origins}, true, nil
}
