# Disposable synthetic fixtures

Local development only. Never copy production logs or service credentials into
these services. Do not run this Compose project on the observability host.
Passwords in this folder are deliberately public fixture values.

The ClickHouse image matches the checked production version. Loki matches the
parser release. Image manifest digests were resolved from Docker Hub on
2026-10-22. Both databases use tmpfs, not persistent production volumes.

```sh
docker compose -f testdata/fixtures/compose.yaml up -d --wait --wait-timeout 90
GOTOOLCHAIN=go1.26.10 go test -p=1 -tags=integration -race -count=1 ./...
docker compose -f testdata/fixtures/compose.yaml stop
docker compose -f testdata/fixtures/compose.yaml rm -f
```

Run those commands from the repository root, on a machine with Docker Compose.
Tests refuse non-loopback targets and use only the fixed `fixture` database.
The ClickHouse health check waits for fixture-table reads with a separate
restricted probe user, so tests do not race the temporary initialization server
and the container probe does not consume the adapter's two reader slots.
Run integration packages serially with `-p=1`: independent packages share the
same server-enforced two-query reader. This does not disable the race detector
or the explicit admission/overload tests. Do not run browser acceptance and
database integration tests concurrently against the same fixture reader.
Loki differential tests ingest only synthetic fixture rows. They compare the
shared unique view because Loki deduplicates identical timestamp/body events;
separate base-table tests verify that this adapter preserves stored duplicates.

On linux-code the Docker Engine and standalone Compose executable are installed,
although `docker` itself is not on PATH. Substitute
`/usr/libexec/docker/cli-plugins/docker-compose` for `docker compose` in the
commands above.

## Tables

- `otel_logs`: mixed services, nested JSON, logfmt, empty and dotted keys,
  levels, quotes, Unicode, boundaries and duplicates.
- `otel_logs_unique`: the same rows without exact duplicates, for Loki
  differential tests.
- `otel_logs_large`: 1,000 rows of 6,000 bytes, beyond one 4 MiB reader
  result, for paged scans.
- `otel_logs_patterns`: two repeating templates of 40 lines each and rare
  templates below Loki's 30-sample pattern threshold.
- `otel_logs_tie`: 4,001 rows at one nanosecond, more than one page can hold.

Browser checks of Explore's live tail need rows with current timestamps.
Insert synthetic rows into the local fixture as `fixture_admin` while the
check runs; never point this at production.

## Verification on 2026-10-22

The user enabled host CPU passthrough after the initial AVX failure. A live
check now identifies an Intel i7-12700K with AVX and AVX2 exposed. Both pinned
containers start locally, and the ClickHouse integration suite passes with
`-race -count=1` after a clean fixture-container recreation.

Verified against real ClickHouse:

- Canonical service fallback and severity mapping cases.
- Parameterized selector and line filters, case sensitivity, Unicode, empty
  filters, regex anchoring/newline behavior, and injection-shaped values.
- Preservation of legitimate identical duplicate records.
- Readiness against the actual table and failure for a missing table.
- Restricted-reader write rejection.
- Native driver deadline cancellation without weakening `readonly=1` or the
  fixed 15-second server execution limit; readiness still works afterward.

The driver initially failed because v2.40.3 derives a query setting from the
context deadline. `internal/chcontext` prevents that setting rewrite only at
the native driver boundary, while preserving the original Done, Err and Value
behavior. The admission queue and HTTP handlers still see real deadlines.

The patched Go 1.26.10/Loki 3.7.9 race-enabled suite passes, including real-Loki
filter, metadata, parser and sliding-window comparisons. Empty metadata values
are omitted to match that Loki release's ingestion normalization.

The optional Grafana fixture pins Grafana 13.2.4 and Drilldown 3.0.0. Build the
adapter first with `deploy/compose.build.yaml`, then start the fixture project.
Grafana uses the production-equivalent 1 GiB cgroup limit. Its tmpfs plugin
and database files count against that limit; the earlier 512 MiB fixture was
OOM-killed during extended browser checks and is not a supported fixture size.
Grafana listens at `http://127.0.0.1:13001`, with public test credentials
`fixture_admin:fixture-grafana-not-production`. The initial browser check
verified 22 log rows across nine queries with no core request or page errors.
Extended control/context acceptance passed with 57 queries and 102 verified
log rows, no core failures and no page errors. Four superseded navigation
requests were canceled, not counted as successes. Passing the
fixture tests does not establish full LogQL or live deployment compatibility.
No production service was changed. The earlier AVX failure was resolved without
changing the pinned database image or using a remote fixture host.

The adapter fixture explicitly sets
`ADAPTER_SERVICE_NAME_ATTRIBUTES=journald.CONTAINER_NAME,journald._SYSTEMD_UNIT`.
An unconfigured adapter uses `ServiceName`. Parser guard fixtures cover reserved,
sanitized, nested and escaped label names. The small reader uses a different
read-only profile with 100 result rows and 1 MiB, proving client settings do not
need to match the server. The unsafe reader deliberately uses result overflow
`break` and must fail adapter startup.
