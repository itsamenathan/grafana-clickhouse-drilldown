# Compatibility target

Development target, verified 2026-10-26: Grafana 13.2.4, its bundled Loki
data source 13.2.2, Logs Drilldown 3.0.0 and production ClickHouse
26.9.20.23. The application serves the bounded contract below. This is not a
claim of full Loki or arbitrary LogQL compatibility.

Acceptance results for each deployed build, with dates and measurements, are
in the [operating instructions](operations.md#current-build-and-acceptance).

## Request observations

A read-only Chromium probe loaded the old trial's overview and opened a
service. It observed browser requests to `POST /api/ds/query` containing:

```logql
{service_name=`fixture-api` }
sum by (detected_level) (count_over_time({ service_name="fixture-api" } [$__auto]))
```

Service identities above are synthetic replacements. `$__auto` is a Grafana
macro, not valid backend LogQL. Contract fixtures must distinguish browser
queries from the expanded requests Grafana sends to the adapter. The service
page also generated these forms:

```logql
{service_name="fixture-api"} | json | logfmt | drop __error__, __error_details__
sum(count_over_time({service_name="fixture-api"} | json | logfmt | drop __error__, __error_details__ [$__auto]))
sum(count_over_time({service_name="fixture-api"} [$__auto])) by (detected_level)
```

The instant count uses query type `instant`; the other count forms use `range`.
Resource calls used GET at Grafana's
`/api/datasources/uid/clickhouse-loki-test/resources/` prefix:

| Resource | Observed parameters, in addition to start/end |
| --- | --- |
| `drilldown-limits` | None |
| `index/volume` | `limit=5000`, query with backtick `service_name=~\`.+\`` |
| `detected_labels` | Empty overview query, selected-service query on service page |
| `patterns` | Selected-service stream selector, `step` |
| `detected_fields` | Selected-service query with JSON/logfmt/drop stages |

These are browser-to-Grafana resource paths. The adapter implements their
`/loki/api/v1/` upstream forms, verified through real HTTP contract tests.
Additional read-only captures exercised level and structured-field predicates,
numeric parsed-field predicates, case-insensitive line filtering and parsers
disabled. A hand-crafted JSON-valued field URL was malformed by Grafana's URL
decoder and was not adopted as a supported executable query. Browser controls
passed using real field selections. Capturing a request proves its
shape, not old-trial correctness.

## Implementation status

Every endpoint compiles its query with the shared parameterized compiler and
applies the exact residual pipeline. Record scans use bounded pages in
deterministic complete-row order. Backward limited log scans begin near the
newest edge and grow their search window; a backward scan reverses the forward
order. Supported count/byte metrics aggregate in ClickHouse when their window is a multiple of the step; other supported forms use streamed compact samples. Common
parser/drop counts and byte metrics use a conservative guard in the same SQL
aggregate, with exact streaming fallback. See the [guard proof](metric-pushdown.md).
Exceeding a scan, memory, deadline, series or response bound returns an explicit
Loki-shaped error, never a partial result. Limits and environment defaults are
in [resource bounds](operations.md#readiness-and-resource-bounds).

Scans omit attribute maps when they cannot change the result: discovery,
volumes, patterns, and metrics whose output keeps only `service_name` or
`detected_level`. Any label-filter stage forces complete metadata.

Selector-only byte volumes, index statistics and service/label discovery are
aggregated in ClickHouse using the same parameterized selection predicate.
Label discovery and plain count/rate metrics omit message-byte totals, so
ClickHouse can skip `Body` unless a predicate needs it. Guarded parser metrics
still inspect the body. Volumes and byte metrics retain exact byte totals.
Residual pipeline stages still run exactly; summaries never bypass a filter.

| Operation | Current parser/compiler | Verification or boundary |
| --- | --- | --- |
| Stream selector, double quotes/backticks, four matcher operators | Shared parameterized compiler, unit and ClickHouse tests | Patched Loki comparisons pass |
| Positive/negative substring and regex line filters | Shared compiler, real ClickHouse and Loki comparison cases | Fixture/live UI and SQL checks pass |
| Unknown stream labels | Explicit unsupported error | Deliberate initial allowlist |
| JSON/logfmt, explicit extraction, numeric/string/regex field predicates | Exact validated Loki residual stages after shared SQL selection | Loki/unit tests and actual field controls pass |
| Error-label filters and drop stages | Exact Loki stages, including metric pipeline-error behavior | Fixture/live UI pass |
| count_over_time, rate, bytes_over_time, bytes_rate; one outer sum by/without | Real sliding windows, bounded points/groups/samples | Loki/unit tests, UI and live window checks pass |
| Compound line OR and positive/negative pattern line filters | Exact Loki residual stages, no message rewrite | Differential cases pass |
| avg_over_time and sum_over_time with unwrap (plain, bytes(), duration()), post-unwrap filters and range grouping | Loki's exact sample extractor after the shared SQL selection; windows recomputed per step | Loki differential cases pass; Drilldown numeric field charts |
| Other metric functions, aggregating unwrapped samples, line/label formatting, keep, unpack, parser regex/pattern | Explicit unsupported errors | Outside the contract |
| Discovery, log rows, byte volumes, fields and values | Same complete compiled selection, real HTTP/DB tests | Fixture/live UI and SQL checks pass |
| Readiness, Grafana arithmetic probe, errors and tenant rejection | Real table readiness, errors and cancellation tested | Separate live account verified |
| Pattern discovery | Loki's Drain per stream and level over a lean streamed scan, Loki's querier pruning | Unit, fixture and live checks; Drilldown Patterns tab and include filter pass |
| Live tail (websocket) | Shared compiler and residual pipeline, polled with late-row rereads | Unit, fixture browser and live Explore checks |
| Ingestion, push, rules, arbitrary metric functions | Explicit unsupported errors | Outside the contract |

The endpoint contract covers GET and form-encoded POST for `query`,
`query_range`, `labels`, label values, `series`, `index/volume`,
`index/volume_range`, `index/stats`, `detected_labels`, `detected_fields`,
detected-field values, `patterns`, `drilldown-limits` and the `tail`
websocket. Discovery accepts repeated
`match[]` selectors as a single SQL OR, preserving real stored duplicates
without duplicating overlap between selectors. Numeric second and duration
steps are validated. Unknown parameters, repeated singleton parameters,
unsupported tenant headers, syntax and stages produce explicit errors.

`query` accepts supported metrics and the Grafana `vector(1)+vector(1)` probe,
which requires actual database readiness. Instant log queries are unsupported.
`query_range` accepts logs or supported metrics. Log bounds use [start,end),
direction is honored and the limit applies globally before stream grouping.
For a selector with only row-preserving JSON/logfmt/extraction/drop stages,
the requested limit is also applied in SQL. A filtering pipeline streams pages
from the requested edge and stops once it holds the limit; the limit is never
pushed through a filter. A 6 MB synthetic fixture proves filtered pages,
counts and volumes are exact across pages beyond one 4 MiB reader result.
The installed Loki data-source frontend is version 13.2.2. Its context
provider discovers labels with start=end at the selected row's millisecond,
then requests two hours before/after the row with a nanosecond-refined near
edge. For label/series discovery only, an equal millisecond-aligned bound
selects that one-millisecond bucket.

Time ranges and metric windows have no default time cap. An optional
`ADAPTER_MAX_QUERY_RANGE` applies to log/discovery ranges and metric windows;
metric request ranges may exceed a configured cap by one step because Grafana
aligns both bounds outward to whole steps. Ordering, non-negative bounds,
metric millisecond precision and evaluation-point limits still apply. Empty `{}`
selectors remain invalid, never a fallback for broken context label discovery.
The contract comes from the installed frontend's reported version and
[`LogContextProvider.ts` at v13.2.2](https://github.com/grafana/grafana-loki-datasource/blob/v13.2.2/src/LogContextProvider.ts),
corroborated by fixture browser requests. It is not an assumption that all
Grafana versions build context requests this way.
Drilldown's Fields tab charts `duration`, `bytes` and `float` fields with
`avg_over_time(… | unwrap [bytes|duration](field) | __error__="" [$__auto]) by ()`
and other fields with `sum by (field) (count_over_time(…))`. A field with a
distinct value per record, such as journald's `__CURSOR` or `__SEQNUM`, yields
one series per record; beyond 1,000 series the chart returns an explicit
error, as Loki does past its `max_query_series` limit.

Structured metadata/parsed categories honor the categorize-labels encoding
header. Metric windows are (evaluation-window,evaluation], evaluated at each
real step, not non-overlapping buckets. Metric timestamps and steps must be
millisecond-aligned; log records retain exact nanoseconds. Unsupported finer
metric precision returns an error instead of silently rounding.

`drilldown-limits` identifies the adapter honestly and reports
`pattern_ingester_enabled=true`. `max_query_length` and `max_query_range`
report `0s` when the optional time cap is disabled, following Loki's limits
semantics, or the configured duration. `query_timeout` reports the adapter
request deadline. The installed Drilldown frontend's handling of disabled limits is not
browser-verified. Ingestion requests return
unsupported errors.
Build information and `/source` provide the corresponding-source offer.

## Patterns

Drilldown requests `patterns` with the service's stream selector. As in
Loki, only a stream selector is accepted. Pattern inclusion and exclusion use
`|>` and `!>` line filters, which run as exact Loki residual stages.

The adapter trains Loki v3.7.9's `pkg/pattern/drain` on demand: one Drain per
stream and detected level, default configuration, log format detected from
each stream's earliest collected line, and Loki's default tokenizable JSON
fields. A backward scan finds the timestamp cutoff of the newest
`ADAPTER_PATTERN_LINES` lines, 50,000 by default, without retaining them; a
forward scan from that cutoff then trains Drain in ascending time order as
lines arrive. This default gives Drain enough examples for its 30-sample
pruning while bounding work on busy services. Drain's models and output share
the retained memory budget; exhausting it returns an error rather than a
shorter sample.

Memory accounting follows the pinned Drain structures. Each detector reserves
4 KiB for fixed objects and map headers. Live clusters charge their object,
LRU/accounting entries, cloned token slice and strings, cloned integer
`TokenState`, outer `Chunks` capacity, and every sample array's capacity.
An hourly chunk preallocates 361 16-byte samples even when it holds one sample.
Small-allocation allowances add 25% and round upward; large allocations round
to 8 KiB heap pages. Evicted cluster charges and
references are released. Prefix-tree nodes survive eviction, so each newly
created path node keeps a conservative 368-byte charge plus its key and actual
cluster-ID slice capacity. Shared paths are charged once. Tree and tokenizer
scratch inspection uses read-only reflection against Loki v3.7.9's private
layout; a dependency upgrade must rerun the calibration tests.

Scratch accounting includes token/state capacities, logfmt decoder storage,
and all input bodies still referenced by used or unused tokenizer slots.
Cluster token clones do not retain their source body. The engine checks the
budget after each bounded training mutation, then charges output against the
same budget. This is a conservative retained-heap estimate, not a process RSS
limit; transient scan, GC and response allocations also exist.

`TestPatternMemoryCalibration` trains Drain directly in isolated test processes,
without a store or retained input rows. It measures live heap after GC while
keeping detectors and their accounting alive, and requires estimates between
1 and 2.05 times measured heap. Workloads cover high volume, many templates and
IDs, multiple levels, long JSON/logfmt, hourly chunks over days, and eviction.
A separate regression covers about 16,000 varied lines over 36 hours within the
default 32 MiB budget, and a model with genuinely excessive hourly chunk storage
still returns an explicit memory error.

If the oldest timestamp cohort crosses the line limit, the whole cohort is
excluded so the covered period contains no partially sampled timestamp. Samples
use Drain's ten-second resolution, re-bucketed to the requested step (at
least ten seconds), with Unix-second timestamps. As in Loki's pattern querier,
patterns with fewer than 30 samples in range are dropped and the 300 largest
are returned. Samples of one pattern and level are summed exactly across
streams.

A Loki pattern ingester trains continuously and retains up to three hours of
history. This adapter trains only on the most recent bounded portion of the
selected range. Clusters can differ from a Loki deployment that saw a different
history, and a quiet service may yield no patterns. No JSON fields are added.
`X-Patterns-Covered-Start` gives the effective start as Unix nanoseconds,
`X-Patterns-Sampled` says whether older records were excluded, and
`X-Patterns-Lines` gives the collected training count. A complete scan reports
the requested start. Sample timestamps label buckets; the first bucket can
start before the effective start and contains only collected lines. Counts make
no claim about the unscanned past. The scan transfers timestamps, bodies,
service and level, never attribute maps.

## Live tail

Grafana Explore's Live mode opens a browser websocket through Grafana's
data-source proxy to `/loki/api/v1/tail?query=…`. Grafana's experimental
backend streaming toggle (`lokiExperimentalStreaming`, path
`/loki/api/v2alpha/tail`) is off by default and is not implemented.

The tail accepts any supported log query, `limit` (default 100, at most
2,000), `start` with a default and maximum lookback of `ADAPTER_TAIL_LOOKBACK`, one
hour by default and `delay_for` (0–5
seconds). It first sends the latest `limit` records in [start, now−delay),
then polls every two seconds. One backward startup scan selects that backlog
and counts fingerprints of the historical rows in the reread window. Only the
backlog bodies and labels are retained. Rows excluded by `limit` are the oldest
ones; their fingerprints prevent them from being sent as new. The baseline
has its own fingerprint and memory bounds, without the 2,000-new-record poll
limit. Rows arriving after the startup scan remain eligible for later delivery.

Production rows reach ClickHouse up to about 20 seconds after their timestamp.
Each poll rereads from 60 seconds before the last successfully completed
coverage boundary and sends only records it has not sent. Reader overload
leaves that boundary unchanged, so the next successful poll covers the whole
gap even after more than 60 seconds of failed admission. Catch-up obeys the
same bounds and fails explicitly if it cannot fit. Delivery is tracked as a
multiset of complete-record fingerprints, so stored duplicates are each
delivered once and late rows within the reread window are delivered when they
arrive. Rows older than that window when they arrive are not guaranteed to be
tailed.

Messages use Loki's v1 format, `{"streams":[…],"dropped_entries":null}`, with
all labels on each stream and records in ascending time order. At most four
tails run at once; a fifth is refused before upgrading. A tail lasts at most
one hour, then closes normally. More than 2,000 new records in one poll, or
20,000 distinct fingerprints tracked in a scan, closes the stream with an
explicit websocket 1011 error rather than dropping records. The configured
`ADAPTER_MAX_RETAINED_BYTES` budget also applies to startup and polls. It
charges fingerprint maps, retained bodies and labels, websocket buffers and
conservative allowances for response objects and JSON serialization buffers
before retaining events. Exceeding it closes with 1011, without sending a
partial poll. Each query has the configured request deadline. Client disconnect
and server-side closure both stop and join the websocket reader. Browser
origins must match the request host or `ADAPTER_ALLOWED_ORIGINS`; Grafana's
backend sends no origin.

Volume values are exact UTF-8 message bytes of the selected rows, not row
counts and not Loki's estimated indexed chunk sizes. Index stats report exact
selected entries/message bytes/virtual streams and zero Loki chunks, because
there is no Loki chunk store. Volume grouping currently supports service_name
series only. Other modes return explicit errors. This corrects the old trial's
row-count volume behavior; tests must compare against SQL sum(length(Body)).

Exact identical stored events are preserved. The reference Loki ingester
deduplicates them, so equality comparisons use the shared unique fixture view;
separate base-table tests assert duplicate preservation. Do not interpret this
intentional difference as permission to collapse ClickHouse events. Equal-time
ordering uses a deterministic complete-row tie break. There is no opaque event
cursor; timestamp-only pagination cannot uniquely resume inside an equal-time
cohort larger than the requested limit. Do not claim such pagination is lossless.
Timestamp-boundary pagination and global forward/backward limits are covered
by HTTP fixture tests, including preservation of duplicate rows across a page
boundary outside an equal-time cohort.

## Canonical mappings

`service_name` defaults to the OTel `ServiceName` column. Equality selectors
and finite-set/prefix regex prefilters can use ClickStack's primary key without
reading attribute maps. Regex predicates still run in full after the prefilter.

`ADAPTER_SERVICE_NAME_ATTRIBUTES` optionally lists up to 16 ordered attribute
keys to try before `ServiceName`, skipping empty values. Bare keys and `log:`
keys use `LogAttributes`; `resource:` keys use `ResourceAttributes`. Keys may
contain ASCII letters, digits, `_ . : / -`, with at most 256 bytes per key.
The compiler validates keys before using them as SQL literals. Query values
remain typed parameters.

The deployment and local fixture Compose files set
`journald.CONTAINER_NAME,journald._SYSTEMD_UNIT`. This preserves their existing
identity. Arbitrary fallback attributes can override any raw `ServiceName`, so
no additional raw-service prefilter is sound. This mode reads the configured
maps and loses service-key pruning. Materialize the desired identity into
`ServiceName` at ingestion for the cheapest queries. No stored data is changed.

`detected_level` uses case-normalized recognized severity text, selected aliases,
then standard OTel severity-number bands. Unknown values become `unknown`.
`critical` remains distinct from `fatal`. The exact expression is centralized
in `internal/sqlgen/selection.go` for all future endpoints. It does not infer
levels from message bodies. Selected canonical mapping cases pass against real
ClickHouse and normalized synthetic Loki streams.

Only service_name is emitted as an indexed stream label. detected_level is
structured metadata, with a deliberate virtual selector extension for that
computed value. Attribute aliases use log_ and resource_ prefixes, escape
underscore as _u, dot as _d and other bytes as _xHH. They are scoped, reversible
and collision-safe, including dotted/underscored keys. trace_id/span_id remain
separate metadata keys. Attributes are never silently exposed as indexed
labels; unknown selector labels return errors. Parsed JSON/logfmt names follow
Loki's own normalization, independently of reversible stored-attribute aliases.
Empty stored attribute values are omitted from emitted metadata, matching the
pinned Loki 3.7 ingestion normalization. Missing and empty label values still
have the same matcher behavior; raw stored attributes and historical service
identity are not rewritten.

Label regexes are fully anchored and default to dot-all, matching Prometheus
label semantics. Line regexes search within the body and default to no dot-all.
The compiler sets these modes explicitly because ClickHouse defaults differ.
Substring matching is byte-based and case-sensitive. Values use typed query
parameters, not SQL interpolation. Selected cases pass against real
ClickHouse and pinned Loki; this is not an exhaustive differential proof.
