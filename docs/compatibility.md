# Compatibility target

Development target, verified 2026-10-22: Grafana 13.2.4, Logs Drilldown 3.0.0,
and production ClickHouse 26.9.20.23. The application now serves the bounded
contract below. Browser and live deployment acceptance passed for that contract;
this is not a claim of full Loki or arbitrary LogQL compatibility.

Local core acceptance passed with Grafana 13.2.4, its Loki frontend 13.2.2,
Drilldown 3.0.0 and Loki reference 3.7.9. The full browser run completed 57
queries and verified 102 returned log rows, including context, level/text/field
controls, the actual parser switch and a time-range change. No core requests
failed and no unhandled page errors occurred. Four superseded requests were
canceled during navigation, recorded separately rather than counted as query
successes.

Live acceptance passed on 2026-10-23 UTC. All 12 API/SQL checks pass, including
positive/absent structured-field controls and every sliding-window sample.
Production UI controls/context completed 73 queries and verified 3,016 returned
log rows with no failures or page errors. A separate 20-service overview
completed in 4.204 seconds, verified 1,091 log rows across 45 queries, and passed
context with no failures. Six one-second cgroup samples measured maxima of
40,124,416 bytes for the adapter and 1,006,194,688 bytes for shared ClickHouse.
Container-lifetime peaks were 41,586,688 and 1,872,441,344 bytes respectively;
the latter includes earlier unrelated database activity and is not a load-only
measurement. The adapter remains capped at 256 MiB and two active queries.

The old trial was retired after saved-consumer checks. After retirement all
12 API checks still pass, SQL and PromQL return data, `clickstack` remains the
global default, and ingestion was observed within ten seconds of current time.

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
| `patterns` | Selected-service query, `step=2s` |
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

Current incident status, 2026-10-23 UTC: selector-only overview summaries match
one-hour live SQL across 40 services and 10,883 records, and all 12 standard API
checks pass with smaller complete filter controls. Full browser revalidation
still fails on bounded scans under current production volume. Earlier browser
successes above describe the original release, not a current end-to-end pass.

Selector-only byte volumes, index statistics and service/label discovery are
aggregated in ClickHouse using the same parameterized selection predicate.
They do not download full log bodies and attribute maps merely to compute
counts or byte totals. The 4 MiB native result limit still applies to the small
summary, and server memory/read/deadline limits remain unchanged. Queries with
pipeline stages retain the full bounded residual path; summaries never bypass
a filter. The large synthetic fixture verifies exact 6,000,000-byte/1,000-row
totals and discovery under that 4 MiB limit.

| Operation | Current parser/compiler | Verification or boundary |
| --- | --- | --- |
| Stream selector, double quotes/backticks, four matcher operators | Shared parameterized compiler, unit and ClickHouse tests | Patched Loki comparisons pass |
| Positive/negative substring and regex line filters | Shared compiler, real ClickHouse and Loki comparison cases | Fixture/live UI and SQL checks pass |
| Unknown stream labels | Explicit unsupported error | Deliberate initial allowlist |
| JSON/logfmt, explicit extraction, numeric/string/regex field predicates | Exact validated Loki residual stages after shared SQL selection | Loki/unit tests and actual field controls pass |
| Error-label filters and drop stages | Exact Loki stages, including metric pipeline-error behavior | Fixture/live UI pass |
| count_over_time, rate, bytes_over_time, bytes_rate; one outer sum by/without | Real sliding windows, bounded points/groups/samples | Loki/unit tests, UI and live window checks pass |
| Compound line OR and positive/negative pattern line filters | Exact Loki residual stages, no message rewrite | Differential cases pass |
| Other metric functions, line/label formatting, keep, unpack, parser regex/pattern | Explicit unsupported errors | Outside the initial contract |
| Discovery, log rows, byte volumes, fields and values | Same complete compiled selection, real HTTP/DB tests | Fixture/live UI and SQL checks pass |
| Readiness, Grafana arithmetic probe, errors and tenant rejection | Real table readiness, errors and cancellation tested | Separate live account verified |
| Pattern discovery, tailing, ingestion | Explicit unsupported errors | Patterns advertised disabled per data source |

The local fixture startup blocker is resolved. Host CPU passthrough exposes
AVX/AVX2, and the pinned ClickHouse image runs. Race-enabled database tests pass
for the implemented selection subset, reader restrictions and cancellation.
This does not mark a full plan step or any unimplemented endpoint supported.

The endpoint contract covers GET and form-encoded POST for `query`,
`query_range`, `labels`, label values, `series`, `index/volume`,
`index/volume_range`, `index/stats`, `detected_labels`, `detected_fields`,
detected-field values and `drilldown-limits`. Discovery accepts repeated
`match[]` selectors as a single SQL OR, preserving real stored duplicates
without duplicating overlap between selectors. Numeric second and duration
steps are validated. Unknown parameters, repeated singleton parameters,
unsupported tenant headers, syntax and stages produce explicit errors.

`query` accepts supported metrics and the Grafana `vector(1)+vector(1)` probe,
which requires actual database readiness. Instant log queries are unsupported.
`query_range` accepts logs or supported metrics. Log bounds use [start,end),
direction is honored and the limit applies globally before stream grouping.
For a selector with only row-preserving JSON/logfmt/extraction/drop stages,
the shared compiler's predicate and complete-row ordering select the requested
page in SQL before transmitting metadata. This keeps a 100-row context request
from reading every record in its two-hour range. Pipelines with residual
predicates still require a full bounded scan before applying the log limit;
the limit is never pushed through a filter. A separate 6 MB synthetic fixture
proves that bounded pages succeed under the 4 MiB result limit while an
over-budget filtered scan fails explicitly.
The installed Loki data-source frontend is version 13.2.2. Its context
provider discovers labels with start=end at the selected row's millisecond,
then requests two hours before/after the row with a nanosecond-refined near
edge. For label/series discovery only, an equal millisecond-aligned bound
selects that one-millisecond bucket. Log ranges allow two hours plus one
millisecond of edge-refinement tolerance. The original one-hour limit remains
for metric/discovery ranges and metric windows. SQL scan, memory, row and
deadline budgets are unchanged; a large context request can still return an
explicit budget error. Empty `{}` selectors remain invalid, never a fallback
for broken context label discovery.
The contract comes from the installed frontend's reported version and
[`LogContextProvider.ts` at v13.2.2](https://github.com/grafana/grafana-loki-datasource/blob/v13.2.2/src/LogContextProvider.ts),
corroborated by fixture browser requests. It is not an assumption that all
Grafana versions build context requests this way.
Structured metadata/parsed categories honor the categorize-labels encoding
header. Metric windows are (evaluation-window,evaluation], evaluated at each
real step, not non-overlapping buckets. Metric timestamps and steps must be
millisecond-aligned; log records retain exact nanoseconds. Unsupported finer
metric precision returns an error instead of silently rounding.

`drilldown-limits` identifies the adapter honestly and reports
`pattern_ingester_enabled=false`, so Drilldown disables only this source's
optional pattern UI without changing global plugin settings. Direct pattern,
tail and ingestion requests still return unsupported errors. Build information
and `/source` provide the corresponding-source offer.

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

`service_name` uses original `LogAttributes['journald.CONTAINER_NAME']`, then
`LogAttributes['journald._SYSTEMD_UNIT']`, then `ServiceName`, ignoring empty
journal values. This expression was checked against the live SQL Logs Explorer
dashboard. It intentionally differs from the old trial's raw `ServiceName`.
No historical data is changed.

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
parameters, not SQL interpolation. Selected cases now pass against real
ClickHouse; comprehensive Loki differential comparisons remain outstanding.
