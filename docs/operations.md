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
alias. Register a separate non-default Loki data source with UID
`clickhouse-loki-go`. Keep `clickstack` as the global default. Current Grafana
credentials come only from OpenBao `prod/observability`, in memory. Never use
the retired `prod/grafana` token.

Deploy only an exact tested local image ID/build reference, retain the image
archive and corresponding source privately for rollback, and record the
actual image/source hashes with acceptance results. This repository is not an
automatic deployment. Reinspect live definitions before each update.

The accepted build is image
`sha256:b6e0da06a5af6aa4da6a32d0e5dee03b36fa1c40572b616f16c4ca600d0501b6`.
Its corresponding-source SHA-256 is
`4dfe387e2479ce75759c4e180bd109c60c75a2e482af08fb84f05bfdecfbbd3b`.
Both the loaded image and the authenticated Grafana source download were
verified against those identities. The immutable image archive, source archive
and manifest are retained in the runtime directory. The data-source display
name is `ClickHouse Logs Drilldown`, still non-default.

The 2026-10-23 code-396 fix replaces full-record downloads with exact SQL
summaries for selector-only overview volumes, statistics and discovery. A
one-hour live SQL comparison passed for all 40 services and 10,883 records,
including 1,233,406 message bytes. All 12 standard live checks passed using a
complete 120-record service control; the cross-service negative-selector
control requests 100 rows. Large metadata-heavy log pages and residual scans
can still exceed the unchanged result budget and must fail explicitly.
Authenticated source, runtime manifest/archive hashes, SQL, PromQL and fresh
ingestion were reverified. The prior runtime's private rollback files are in
`/root/docker/clickhouse-loki-adapter/rollback-code396-20261023T142402Z`.

Current browser revalidation does not pass. The latest 20-service run returned
a bounded-scan error on a query even though all overview summary comparisons
passed. An earlier run also failed service log/count and detected-field
requests. Do not treat the original accepted browser/load measurements below
as acceptance of this incident fix under today's data volume. The adapter
remains healthy; the incident is only partially resolved.

## Acceptance commands

After creating the separate runtime and reader, register only the new source
and compare it with settled direct-SQL results:

```sh
python3 tools/verify-live.py --register
PLAYWRIGHT_MODULE=/tmp/opencode/drilldown-browser/node_modules/playwright \
  python3 tools/measure-live.py
```

The verifier prints pass/fail counts, not log bodies. The measurement command
loads a 20-service overview in a fresh browser context and checks every
returned log's selected-service identity using the real LogQL parser. It
samples the adapter and ClickHouse cgroups once per second and reports both
sampled maximum memory and container-lifetime peaks. ClickHouse is shared, so
these figures do not attribute all database memory to the adapter. Missing
samples or browser failures fail the command. These scripts are release-gate
tools, not evidence of a completed live deployment by themselves.

## Readiness and resource bounds

`/healthz` means the process is alive. `/ready` runs a real bounded SELECT
against the configured table. The Grafana `vector(1)+vector(1)` probe also
requires that real readiness query before returning its arithmetic result.
`/adapter healthcheck` calls `/ready`; it does not trust a canned success.

HTTP admission permits two active queries and 32 waiting requests. The native
client has two connections and its own bounded gate. Requests have 15-second
deadlines, and native query cancellation retains the original context's Done
signal. The driver-specific context adapter prevents the pinned driver's
automatic attempt to change server timeout settings under `readonly=1`.
Native end-of-stream/cancellation can arrive before ClickHouse releases its
process-list slot. Only server busy code 202 receives a bounded retry while
the same admission permit and original deadline remain held: at most eight
attempts and 635 ms total backoff. Persistent contention returns overload;
memory/read/result limits and other errors are not retried. This does not
increase either the client or server's two-query concurrency limit.

The restricted server profile enforces one thread/query, 128 MiB/query,
15 seconds, 10,000 result rows, 4 MiB result bytes, two million rows read and
two concurrent queries. Overflow throws, never returns partial success.
Selected records also have a 64 KiB per-record and 8 MiB decoded scan/pipeline
budget. HTTP query responses are at most 4 MiB; source downloads stream
separately and are not query responses.

Selector-only volumes/statistics and service discovery aggregate inside
ClickHouse instead of transferring every record's body and metadata. Summary
output is limited to 5,000 services and the same 4 MiB result budget. Pipeline
queries retain residual evaluation and scan limits. A remaining code 396 error
therefore reports the bounded scan budget and asks for a narrower selection;
it must not be retried or fixed by silently returning partial counts.

Metric/discovery ranges and metric lookback windows are at most one hour.
Log ranges allow Grafana's two-hour context window plus one millisecond for
its nanosecond-refined near edge. That does not increase any database scan,
memory, row or deadline budget. Zero-width millisecond label/series discovery
uses the containing millisecond bucket, as required by Grafana log context.
At most 2,000 evaluation points, 1,000 metric groups and 10,000 output samples
are allowed. Log output limits are at most 2,000. These are bounds, not a
promise that all one-hour queries fit the byte/read budgets. Narrow the time
range or selector when a budget error occurs.

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
