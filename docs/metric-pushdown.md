# Guarded parser metrics

The shared SQL compiler can aggregate `count_over_time`, `rate`,
`bytes_over_time` and `bytes_rate` beneath `sum` or `sum by` containing only
`service_name` and `detected_level`. Windows must be multiples of the step,
with buckets anchored to the request's evaluation phase. Absolute Unix-step
alignment is unnecessary. A trailing incomplete evaluation step is excluded.
Buckets retain exact nanosecond boundaries and feed the same sliding-window evaluator as streamed samples.
Splits occur at whole steps and preserve that phase. Rows after the final
evaluation cannot affect any returned sample.

Besides SQL-complete selectors and line filters, one guarded form is supported:
parameterless `json`/`logfmt` stages followed by one unconditional
`drop __error__, __error_details__`. Both names must appear exactly once.
Parser options, extraction expressions, other drops and later stages use the
streaming path.

## Guard

The same SQL aggregate counts rows satisfying any of:

- `Body` contains the case-sensitive ASCII substring `error`.
- `Body` contains a backslash after replacing exact `\/` pairs with `/`.
- The effective service identity is empty.

Any nonzero guard in the evaluated span discards the aggregates and runs the
exact streaming path. SQL errors and cost bounds never trigger this fallback.
Guards are returned in every aggregate bucket, including across adaptive
result-size splits, and are checked before adding any samples. No separate
whole-range guard query is needed. False positives, including ordinary error
messages and other escaped values, deliberately cost an additional aggregate scan.

## Proof against pinned Loki 3.7.9

The source reviewed is `github.com/grafana/loki/v3@v3.7.9`, particularly
`pkg/logql/log/{parser.go,util.go,drop_labels.go,labels.go,parser_hints.go}`,
`pkg/logql/syntax/{ast.go,extractor.go}`, `log/logfmt/jsonstring.go`, and the
metric pipeline error check.

1. JSON and default logfmt preserve the body and every row when there are no
   label filters. JSON parse failures create internal error labels; default
   logfmt is non-strict. The final named drops reset both internal error slots.
2. Drops reset those slots rather than deleting identically named parsed
   labels. Both sanitizers preserve ASCII letters and replace other runes
   with underscores. Nested JSON inserts underscores between sanitized key
   components. Producing `__error__` or `__error_details__` therefore requires
   the contiguous letters `error` somewhere in the original body, unless a
   decoder produces them from escapes. Guarding remaining backslashes covers Unicode
   escapes and all other escape encodings. The only allowed escape, `\/`,
   produces a slash, which sanitizes to an underscore. It cannot synthesize
   an ASCII letter or join letters across a key separator. Punctuation and nested keys cannot
   synthesize those letters. The broader substring guard catches them too.
3. Nonempty `service_name` is a base stream label; `detected_level` is always
   present as metadata. Loki suffixes parser collisions with `_extracted`,
   including names that become collisions only after sanitization. Parsers
   cannot replace either grouping label. Empty services are guarded because
   their missing label can be created by a parser. Other metadata/parsed
   labels are discarded by the outer sum and cannot affect rows or bytes.
4. With a zero guard, no surviving error label or grouping-label change is
   possible. Each selected row contributes exactly one count and its original
   byte length. SQL buckets and the streamed metric therefore agree.

The streaming fallback uses the original metric AST's Loki sample extractor.
This matters: `VectorAggregationExpr.Extractors` injects outer-sum grouping into
parser hints. A plain sum can skip parsing labels entirely, and grouping can
suppress extraction of a body label named `__error__`. Such a summed metric
can succeed while an unsummed metric over the same body fails the pipeline
error check. Evaluating a log pipeline and then summing is not equivalent.

Unit tests cover eligibility and fallback; ClickHouse tests prove aggregate
execution without transferring bodies, compare exact boundaries and profiles,
and exercise collisions and empty services. Differential tests run safe,
reserved, sanitized, nested and escaped bodies against the pinned Loki server,
including successful summed metrics and failing unsummed metrics.
