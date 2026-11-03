# Build, test and operate the adapter

The runtime is a single-backend, read-only query adapter, not a Loki server or
tenant-isolated service. Source is AGPL-3.0-only. No hosted remote is required.

## Build and corresponding source

```sh
/usr/libexec/docker/cli-plugins/docker-compose -f deploy/compose.build.yaml build
/usr/libexec/docker/cli-plugins/docker-compose -f testdata/fixtures/compose.yaml up -d --wait --wait-timeout 90
GOTOOLCHAIN=go1.26.10 go test -p=1 -tags=integration -race -count=1 ./...
GOTOOLCHAIN=go1.26.10 go vet -tags=integration ./...
```

On hosts with the normal Docker CLI, use `docker compose` instead of the
standalone Compose executable. Only the local fixture project publishes
loopback test ports. Never run fixtures on observability or copy production
logs/credentials into them. Fixture passwords in source are public test values.

The pinned multi-stage build produces a static binary in a scratch image,
running as UID/GID 65532. It packages application source, build/install files,
the vendored imported dependency source, actual license/NOTICE files, Loki's
licensing declaration and the Go license in `/source.tar.gz`. It contains no
runtime environment or credentials. `/source` streams that archive. Build
information advertises its SHA-256 and configured source-download URL.

An extracted archive supports an offline Go rebuild using the selected Go
1.26.10 toolchain:

```sh
CGO_ENABLED=0 GOTOOLCHAIN=go1.26.10 go build -mod=vendor -trimpath -buildvcs=false \
  -ldflags='-s -w -X main.version=dev' -o adapter ./cmd/adapter
```

Use the deployed build's version in place of `dev` when reproducing it.
Changing dependencies requires a fresh license/notice and vulnerability review.

## Private deployment

Target host: `root@observability.n.vpn`. Deployed runtime:
`/root/docker/clickhouse-loki-adapter`, ordinary Docker Compose, external
`clickstack_internal` network. `deploy/compose.yaml` publishes no ports, uses a
read-only filesystem, drops capabilities, prevents privilege escalation, caps
the adapter at 256 MiB and 0.5 CPU, and rotates logs.

The new reader is `clickhouse_drilldown`, separate from every old reader. The
one-time `deploy/provision-reader.py` creates its profile and SELECT-only grant
on `default.otel_logs` in the live mounted user XML, preserving its inode and
all existing entries. It creates a private current-XML backup and reloads
ClickHouse without a restart. It refuses existing credentials or accounts.
Do not rerun it to rotate credentials or restore a stale XML backup wholesale.

The host's owner-only `.env` contains `CLICKHOUSE_PASSWORD`, exact loaded
`ADAPTER_IMAGE`, and `SOURCE_OFFER_URL`. Never source or print it. The source URL
is Grafana's authenticated generic proxy endpoint:

```text
https://grafana.h.frcv.net/api/datasources/proxy/uid/clickhouse-loki-go/source
```

Grafana connects to `http://clickhouse-loki-go:3100` through the private Docker
alias. The data source is a separate non-default Loki source, UID
`clickhouse-loki-go`, display name `ClickHouse Logs Drilldown`. Keep
`clickstack` as the global default. Current Grafana credentials come only from
OpenBao `prod/observability`, in memory. Never use the retired `prod/grafana`
token. Explore's Live mode reaches the adapter's tail websocket through
Grafana's data-source proxy; Compose sets `ADAPTER_ALLOWED_ORIGINS` to
Grafana's public origin for that browser handshake.

## Update procedure

Reinspect the live Compose file, `.env` keys (not values) and running image
before each update. Deploy only an exact tested local image ID:

1. Build with `ADAPTER_VERSION` set and record the image ID.
2. `docker save` the image, copy the archive to the runtime directory, `docker
   load` it, and confirm the loaded ID matches.
3. Extract `/source.tar.gz` from the image into the runtime directory and
   record image, archive and source SHA-256 values in `manifest.json`.
4. Back up the previous `.env`, `manifest.json` and source archive with a
   `.before-<short image id>` suffix. Change only the `ADAPTER_IMAGE` line.
5. Update `compose.yaml` from `deploy/compose.yaml` if it changed, then
   `docker compose up -d` in the runtime directory. This recreates only the
   adapter.
6. Run the acceptance commands below and record results here and in the
   runtime README.

To roll back, restore the previous `.env` (its `ADAPTER_IMAGE` line) and
`compose.yaml`, then `docker compose up -d`. Retained image archives can be
reloaded with `docker load`. Each `.before-<id>` file holds the state before
build `<id>` was deployed. The 2026-10-26 build `ac65bf38c4fe` (one-hour range
cap, no ClickHouse aggregation) is restored from the `.before-6878a99aa0f2`
files; the 2026-10-23 build `b6e0da06a5af` (no patterns or tail) from the
`.before-d2cddc718b2e` files.

## Current build and acceptance

Deployed 2026-10-28: version `2026.10.28.1`, image
`sha256:813e57d668fba2094fc68c9417e991f6cc9648738c99c01dd8474e2b08758e83`,
image archive SHA-256
`52a14545fea89a4d563e268c4aba449b8017339ed9c949c88ba611a7dcb66ea6`,
corresponding source SHA-256
`7299117defc3ca91c99f8a3fe34ef406cad092ea273adb15b4348509cc7881b1`. It
removes the fixed 4 MiB HTTP response cap and writes log pages value by
value. With roughly 2.4 KB of structured metadata per line, a 2,000-line page
is about 5 MB and was rejected by `2026.10.27.3`. Live checks on 2026-10-28:
all 14 API/SQL checks pass, including a 2,000-line page of the most
attribute-heavy service over a day (5.0 MB, matching the newest 2,000 SQL
rows); that page returns in about one second over 24 hours and three days in
both response encodings; the Drilldown UI exercise over 24 hours passes (53
queries, 11,600 rows); adapter peak memory 64 MiB, no restarts.

The previous build, `2026.10.27.3` (`6ee35a8adf05`, restored from the
`.before-813e57d668fb` files), still has the response cap. It introduced the
rest of the current behavior: it removes the fixed one-hour range cap in favor
of configurable cost limits, aggregates supported metrics in ClickHouse, reads log pages newest
first in growing windows, maps service identity from `ServiceName` with this
deployment's journald fallback, and takes all ClickHouse settings from the
reader profile. It also contains fixes from an independent review: regex
filters are evaluated only by Loki's residual stages, split pages share one
memory budget, unwrapped averages follow Loki's arithmetic, tail startup and
overload cannot lose records, and pattern memory accounting is calibrated.
Earlier builds of 2026-10-27, `6878a99aa0f2` and `b11bcf3baa88`, contain
regex pushdown errors and must not be restored.

Live acceptance of `2026.10.27.3` on 2026-10-27, against production data with
three days of retention:

- All 13 API/SQL checks pass.
- Over 24 hours and three days, volumes, level and parsed counts,
  case-insensitive text search, log pages, detected fields and patterns each
  return in under two seconds.
- The Drilldown UI exercise passes over 24 hours (41 queries, 11,505 verified
  rows) and three days (40 queries, 11,606 rows) with no failed requests.
- A 20-service overview over three days loads in 5.9 seconds; over 15 minutes
  in 4.3 seconds.
- Patterns render and filter; a varied service's patterns succeed over 36
  hours and three days on 20 of 20 attempts. Explore's Live mode delivers a
  backlog and new records and stays open for 75 seconds.
- The adapter's container-lifetime memory peak was 70 MiB of its 256 MiB
  limit, with no restarts or panics.

Synthetic production-scale measurements (100 million rows, seven days, see
`testdata/bench`) are recorded in `testdata/bench/results/README.md`.

Known limitations: unwrapped averages over very busy services and long ranges
fail explicitly at the scan or deadline bounds. A Fields-tab chart of a field
with a distinct value per record, such as journald `__CURSOR`, fails
explicitly beyond 1,000 series, as Loki does beyond `max_query_series`.

## Acceptance commands

Compare the registered source with settled direct-SQL results, then exercise
the real UI:

```sh
python3 tools/verify-live.py
PLAYWRIGHT_MODULE=/tmp/opencode/drilldown-browser/node_modules/playwright \
  python3 tools/measure-live.py
```

The verifier prints pass/fail counts, not log bodies. It includes a filtered
count of the busiest service's last hour against SQL, which needs paged scans,
and a patterns check: Loki pruning, Unix-second steps, sample totals no
larger than the record count and a nonempty `|>` filter for the top pattern.
The measurement command loads a 20-service overview in a fresh browser context
and checks every returned log's selected-service identity using the real
LogQL parser. It samples the adapter and ClickHouse cgroups once per second.
ClickHouse is shared, so these figures do not attribute all database memory
to the adapter.

`tools/browser-patterns-live.cjs` reads `{url, token, datasource, service,
holdSeconds}` as JSON on stdin. It opens Drilldown's Patterns tab, includes the
top pattern and checks the filtered logs, then starts Explore's Live mode and
requires a backlog frame and new records, optionally holding the websocket for
`holdSeconds`. Websocket handshakes cannot be routed, so the credential is a
browser-context header and every request to another origin is aborted. It
prints counts and pattern counts only.

## Readiness and resource bounds

These controls describe current source, validated locally. The deployed build
recorded above has not received the service-mapping, reader-profile or guarded
metric changes.

`/healthz` means the process is alive. `/ready` verifies `readonly=1`, throwing result/read/group/sort/timeout overflow
modes, and a real SELECT against the configured table. Startup requires the same
checks before opening the listener. The Grafana `vector(1)+vector(1)` probe also
requires that real readiness query before returning its arithmetic result.
`/adapter healthcheck` calls `/ready`; it does not trust a canned success.

HTTP admission permits two active queries and 32 waiting requests. The native
client has two connections and its own bounded gate, taken per database query.
Requests have configurable deadlines, 15 seconds by default. Cancellation
retains the original context's Done signal. The driver-specific context adapter prevents
the pinned driver's automatic attempt to change server timeout settings under
`readonly=1`. Only server busy code 202 receives a bounded retry while the
same permit and deadline remain held: at most eight attempts and 635 ms total
backoff. Persistent contention returns overload; memory/read/result limits
and other errors are not retried.

The restricted server profile enforces one thread/query, 128 MiB/query,
15 seconds, 10,000 result rows, 4 MiB result bytes, two million rows read and
two concurrent queries. Overflow throws, never returns partial success.

Record scans are paged. A grouped count and `byteSize` estimate per time
bucket plans pages of at most 4,000 rows and 2 MiB estimated. Startup reads
`max_result_rows` and `max_result_bytes` from the server and lowers these page
budgets to half of any smaller limits. Zero server limits retain the defaults. Each page is buffered whole (at most 16 MiB decoded)
before it is visited; a page the server still rejects for size is split in
time and refetched, so no record is visited twice. Buckets larger than one
page are replanned at finer resolution. Logs, fields, field values and tail
backlogs plan disjoint windows from the requested edge and stop when enough
matching lines arrive. Windows grow fourfold, or sixteenfold when empty, so
sparse selections do not require a query for every second of a long range. The adapter defaults to at most
200,000 transferred records per request; one record is at most 64 KiB with
128 attributes. Endpoints keep at most 32 MiB of retained results by default. HTTP responses
have no separate size cap: every payload is bounded before encoding, and log
pages, the largest, are written value by value rather than encoded whole. A
2,000-line page with heavy structured metadata can exceed 4 MB. Exceeding any bound is an
explicit error asking for a narrower selection.

Selector-only volumes/statistics and service discovery aggregate inside
ClickHouse instead of transferring records. Summary output is limited to
5,000 services and the same 4 MiB result budget. Label discovery and plain
count/rate aggregates skip message-byte totals and unused body reads.

Time ranges and metric lookback windows have no default cap. Optional
`ADAPTER_MAX_QUERY_RANGE` applies to both, with one step of Grafana's outward
alignment allowed for metric request bounds. Zero-width millisecond
label/series discovery still uses the containing millisecond bucket, as required
by Grafana log context. Output bounds remain 2,000 evaluation points, 1,000
metric groups, 10,000 metric samples, 5,000 services and 2,000 log entries.

Configuration is validated before opening the database. Durations use Go syntax,
for example `15s`, `2h` or `721h`, not `1d`. Integer budgets use decimal counts
or bytes without suffixes. Empty variables use defaults. Negative values,
invalid syntax and overflowing values fail startup. Only the optional time cap
accepts zero.

| Variable | Default | Meaning |
| --- | --- | --- |
| `ADAPTER_SERVICE_NAME_ATTRIBUTES` | empty | Ordered attribute fallbacks before `ServiceName`; bare keys use LogAttributes, `resource:` selects ResourceAttributes |
| `ADAPTER_MAX_QUERY_RANGE` | `0s` | Optional request range and metric window cap; zero disables it |
| `ADAPTER_REQUEST_TIMEOUT` | `15s` | Whole HTTP request deadline and deadline for each tail query |
| `ADAPTER_TAIL_LOOKBACK` | `1h` | Default and maximum initial tail lookback |
| `ADAPTER_PATTERN_LINES` | `50000` | Maximum newest lines collected for pattern training, positive integer below the platform maximum |
| `ADAPTER_MAX_SCAN_ROWS` | `200000` | Maximum records transferred across scan pages |
| `ADAPTER_MAX_SCAN_BYTES` | `268435456` | Maximum estimated decoded bytes transferred across scan pages |
| `ADAPTER_MAX_RETAINED_BYTES` | `33554432` | Retained results, pattern input/model/output, field values, discovery, volumes and metric samples; also caps decoded storage pages |

The adapter sends no ClickHouse query settings. The server reader profile owns
threads, physical rows read, memory, result size, execution time and overflow
behavior. The former `ADAPTER_MAX_READ_ROWS` and `ADAPTER_QUERY_MEMORY_BYTES`
variables have been removed. Setting them has no effect. There is no client
settings opt-in. Client deadlines and scan/retention guards remain active.

Scan row/byte budgets count transferred records and their decoded estimates,
not all rows or compressed bytes ClickHouse examines. They span scan pages.
The ClickHouse read-row, memory and execution limits apply separately to each
SQL query, including planning and aggregation. They do not become a cumulative
physical-read budget across a request's multiple SQL queries. The request
deadline bounds that work; aggregation still fails if any reader limit is hit.

Keep Grafana's Loki data source `jsonData.timeout` greater than or equal to
`ADAPTER_REQUEST_TIMEOUT`; the deployed source uses 20 seconds. HTTP write
allowance is the adapter deadline plus five seconds. Changing the adapter's
limits does not raise the restricted reader's server profile. Adjust that
profile separately under the infrastructure access/deployment procedure if
needed, preserving throwing overflow modes, bounded rows/bytes read, memory,
execution time and concurrency. Read-only credentials cannot raise server
limits. The client deadline may be shorter than the server deadline. A server
profile can be resized without rebuilding or matching client query settings.

Unwrapped `avg_over_time`/`sum_over_time` stop after 20 million sample visits.
Patterns collect at most the newest 50,000 lines by default, train ascending,
cover at most 500 streams, and return at most 300 patterns after Loki's minimum
30-sample pruning. Coverage headers and partial first buckets are described in
[patterns](compatibility.md#patterns). Memory exhaustion is an error, not an
implicit reduction of the line limit. Tails permit four concurrent streams,
one hour each, with two-second polls over a 60-second reread window, at most
2,000 new records per poll and 20,000 tracked. Each tail query has the configured
request deadline and takes the storage gate like any other query.

## Rollback and retirement

All core fixture and live API/browser/load gates passed before retirement on
2026-10-23 UTC. The old source UID `clickhouse-loki-test`, its adapter/private
HAProxy containers, and only its `loki_adapter_test` user/profile were removed.
Ten dashboards, one alert rule, library elements and plugin configuration had
no old-source consumers; external bookmarks cannot be inspected. No volumes,
shared networks, collectors, SQL/PromQL sources or shared query queue changed.

The old trial's source/build/configuration/credentials are archived privately
at `/root/docker/retired/clickhouse-lgtm-adapter-20261023T050115Z`.
Its `retired-image-references.json` records exact retained Docker image IDs;
`rollback-loki_adapter_test.xml` contains only its former user/profile. The
new runtime's `old-trial-data-source.json` preserves nonsecret source metadata.
The current-XML safety backup is
`/root/docker/clickstack/config/poc-users.xml.before-retire-loki-trial-20261023T050115Z`.
Do not restore that shared XML wholesale. The old trial had incorrect filters
and must not be restored as a trusted investigative source.

If the replacement fails, stop only its Compose service/project and retain its
exact image, source archive, nonsecret Grafana data-source metadata and private
runtime `.env`. Existing SQL/PromQL data sources and ingestion must not change.
Never use `down -v`, broad prune commands or broad recursive removal.

After retirement, all 12 API/SQL checks still passed, both existing SQL/PromQL
paths returned data, and recent logs were within ten seconds of current time.
The infrastructure handbook records current placement and recovery boundaries.
Any rollback must preserve subsequent configuration changes and the source
offer; never restore an old whole-file configuration over unrelated accounts.
