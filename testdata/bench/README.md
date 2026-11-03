# Local production-volume benchmark

Use this project only on a development machine. Never run it on observability,
point it at production, or load production logs. All rows and passwords are
public synthetic test values. It does not contact Grafana or OpenBao.

This is a separate Compose project, `drilldown-bench`. It publishes ClickHouse
only at `127.0.0.1:29000` and `127.0.0.1:28123`. The benchmark launches an
adapter at `127.0.0.1:29100`. It does not change or stop `drilldown-fixtures`.

## Start and generate

From the repository root:

```sh
export PATH=$HOME/.local/share/mise/installs/go/1.25.12/bin:$PATH
export GOTOOLCHAIN=go1.26.10
export BENCH_DATA_DIR="$(pwd)/testdata/bench/.data"
mkdir -p "$BENCH_DATA_DIR" testdata/bench/results
df -h "$BENCH_DATA_DIR" /var/lib/docker
free -h
/usr/libexec/docker/cli-plugins/docker-compose -f testdata/bench/compose.yaml up -d --wait --wait-timeout 120
python3 testdata/bench/generate.py --rows 100000000 --scale 1 --days 7 \
  --data-dir "$BENCH_DATA_DIR" > testdata/bench/results/generation.log 2>&1
```

`docker compose` can replace the standalone Compose executable. Python uses
only its standard library; the Go tool adds no module dependencies.

`python3 testdata/bench/verify.py` checks the complete load, reader settings,
date coverage, saved service totals and last-15-minute rates/body formats. It saves aggregate evidence in
`results/validation.json`. Run it after generation, before a timing run.

The named volume `drilldown-bench_data` uses Docker's local bind volume driver
and the absolute `BENCH_DATA_DIR` path. It is disk-backed. Put that directory
on a filesystem with room for data and temporary merge copies. An existing
Docker volume retains its original path even if the environment variable
changes. The default path in Compose is this development checkout on
`/mnt/code`, because Docker's root filesystem had only 6.5 GiB free when the
benchmark was built.

The generator checks `df` and `free`, requires 4 GiB available host RAM and
2 GiB free disk, and measures a one-million-row pilot. It scales down the
requested rows if its compressed-size estimate plus 25% margin exceeds
18 GiB, or if free space cannot also accommodate merges and a 2 GiB reserve.
It checks the active data size and remaining disk during loading. This is a
measured estimate, not a hard filesystem quota. Use `--scale 0.1` for ten
million rows, `--batch 100000` to set insert size, or `--max-gib` to lower the
data budget. The environment caps ClickHouse at 3 GiB and two CPUs. A
500,000-row insert initially exceeded the generator's 1.2 GB query memory
limit; the default is 100,000 rows with smaller insert blocks.

`results/dataset.json` records the chosen target, time bounds, generation
seconds, active on-disk and uncompressed bytes, free resources before loading,
and per-service counts. Loading refuses a nonempty table. A fixed `--end`,
such as `--end 2026-10-27T15:22:00Z`, makes historical data repeatable.

## What is generated

The MergeTree table uses the ClickStack `otel_logs` shape, LowCardinality
service/severity and map keys, nanosecond timestamps, trace/span identifiers,
resource/scope columns and secondary indexes. It partitions by
`toDate(Timestamp)` and orders by
`(toStartOfFiveMinutes(Timestamp), ServiceName, Timestamp)`. The small fixture
table has a different shape and is deliberately not reused. Benchmark-only
cleanup settings retain old merged parts for 30 seconds and check every five
seconds, rather than keeping many minutes of disposable copies. This bounds
temporary disk use without changing the partition or sorting keys.

At the default scale, 88 million historical rows span the preceding seven
days excluding the final hour. Another 12 million rows fill that final hour,
with about 2,000 hot-service rows/s and 500 medium-service rows/s. This is a
high-rate incident hour on top of a lower-rate week. It does not claim to
store a full week at 2,000 hot-service rows/s, which would require over a
billion hot-service rows alone. Scaling changes rates while retaining the
seven-day coverage and service proportions.

Twenty services have a skewed distribution: `edge-gateway` has 60%,
`checkout` 15%, `payments` 9%, followed by a long tail ending in
`archive-sweeper` at 0.005%. The tail service averages below one row/s even
during the incident hour. Each record has 18 log attributes, 12 resource
attributes and one scope attribute. They include host/region/namespace,
endpoint/status, per-record request IDs, journald cursors and sequence
numbers, timestamps, duration strings, byte strings and floats. Severity is
25% debug, 70% info, 4% warn and 1% error, chosen independently of service.
About a quarter of records have trace/span IDs. JSON, logfmt and plain-text
bodies have changing IDs and numeric values for pattern detection.

One third of rows resolve `service_name` through `journald.CONTAINER_NAME`,
one third through `journald._SYSTEMD_UNIT`, and one third through `ServiceName`.
The selected name is always one of the twenty logical services. The raw
ServiceName intentionally differs in the first two cases to exercise the
adapter's canonical fallback expression.

Generation also appends 64 current rows for tail experiments. Those rows
are outside the anchored historical benchmark range. Refresh them while a
local websocket tail is open:

```sh
python3 testdata/bench/live.py --rows 64
```

Run this after bulk generation, not concurrently with it. This appends new
synthetic sequence numbers and IDs. It does not test the
websocket protocol itself or update the saved historical dataset manifest.

## Readers and query matrix

`bench_reader` has the production-equivalent settings:

```text
readonly=1
max_threads=1
max_concurrent_queries_for_user=2
max_memory_usage=134217728
max_execution_time=15
max_result_rows=10000
max_result_bytes=4194304
result_overflow_mode=throw
max_rows_to_read=2000000
read_overflow_mode=throw
```

`bench_reader_wide` differs only in `max_rows_to_read=200000000`, which raises that
one limit by 100 times. This finite value also works with the new adapter
configuration, where zero selects the default. Both have SELECT only on `bench.otel_logs`; both use the public
password `bench-reader-not-production`. The local admin account is
`bench_admin`, password `bench-admin-not-production`.

The matrix has 185 requests per reader. It covers 15m, 1h, 6h, 24h and 7d for
the hot, medium and tail services and the all-service overview. Ranges are
anchored to the saved dataset end, so a later run uses the same historical
selection. Requests include detected labels, backward logs with the exact
`json | logfmt | drop __error__, __error_details__` pipeline, level counts,
parsed counts, metadata and parsed-field filters, detected fields, patterns,
and duration unwrap averages. The overview also requests index volume with
``{service_name=~`.+`}``. Step rounds range/500 up to a practical interval;
metric windows equal that step. The metadata filter uses the real
`record.Alias("log", "http.route")`, resulting in `log_http_droute`.

## Run a baseline or comparison

Freeze the binary before other agents change adapter code:

```sh
git diff --stat > testdata/bench/results/baseline-source.diffstat
git rev-parse HEAD > testdata/bench/results/baseline-source.head
go build -o testdata/bench/results/adapter-baseline ./cmd/adapter
go build -o testdata/bench/results/bench ./tools/bench

testdata/bench/results/bench \
  -adapter-binary testdata/bench/results/adapter-baseline \
  -source-diffstat testdata/bench/results/baseline-source.diffstat \
  -reader bench_reader -output testdata/bench/results/baseline \
  > testdata/bench/results/baseline.log 2>&1

testdata/bench/results/bench \
  -adapter-binary testdata/bench/results/adapter-baseline \
  -source-diffstat testdata/bench/results/baseline-source.diffstat \
  -reader bench_reader_wide -output testdata/bench/results/baseline-wide \
  > testdata/bench/results/baseline-wide.log 2>&1
```

Omit `-adapter-binary` to build the current `./cmd/adapter` before running.
Use a different output prefix for each comparison. The tool supplies
`CLICKHOUSE_ADDRESS`, `CLICKHOUSE_DATABASE=bench`, `CLICKHOUSE_TABLE=otel_logs`,
`CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD`, `ADAPTER_LISTEN`, and a matching
`ADAPTER_MAX_READ_ROWS`, and waits for
`GET /ready`. It removes inherited adapter/database credentials and settings
before launching. It stops only its own adapter process at completion. A readiness failure is
saved as a report with zero executed requests and a startup diagnostic.

The frozen old adapter hardcodes a two-million-row connection setting. It
cannot connect to the wider reader under `readonly=1`, because ClickHouse
rejects the setting change before logging a query. The saved `baseline-wide`
report records that startup failure. To test the wider reader, use a build
that accepts `ADAPTER_MAX_READ_ROWS`; the tool supplies `200000000` for that
reader. Current-code comparisons were frozen separately:

```sh
git diff --stat > testdata/bench/results/current-source.diffstat
git status --short > testdata/bench/results/current-source.status
go build -o testdata/bench/results/adapter-current ./cmd/adapter
testdata/bench/results/bench -adapter-binary testdata/bench/results/adapter-current \
  -source-diffstat testdata/bench/results/current-source.diffstat \
  -reader bench_reader -output testdata/bench/results/current-strict
testdata/bench/results/bench -adapter-binary testdata/bench/results/adapter-current \
  -source-diffstat testdata/bench/results/current-source.diffstat \
  -reader bench_reader_wide -output testdata/bench/results/current-wide
```

Those comparisons measure the source snapshot identified in their reports;
they are not a controlled single-setting comparison with the old binary.

For an adapter already running against this bench database:

```sh
go run ./tools/bench -target http://127.0.0.1:29100 -pid 12345 \
  -reader bench_reader -output testdata/bench/results/existing
```

Replace `12345` with the actual adapter PID. A container can use
`-cgroup-memory-peak /path/to/its/cgroup/memory.peak` instead. All network
addresses must be numeric loopback addresses; the tool accepts only the two
synthetic reader names. With no PID or cgroup file, an external target's peak
memory is unavailable and recorded as zero. The benchmark must be the only
client using that reader. `-ranges 15m,7d` and
`-shapes logs,volume,level_count` select a smaller matrix.

## Read results

Each run writes a per-request JSON report and a compact Markdown table.
The report checkpoints atomically after each request, so interruption leaves
completed measurements available. It records:

- The full request shape, selection range and step, HTTP status, raw error and
  normalized error class. Rejection by the old time-range limit is valid
  baseline evidence and does not stop the matrix.
- HTTP latency, response bytes, log rows, metric series/samples, and discovery
  or pattern item counts. It preserves empty successes; this is a performance
  measurement, not an independent correctness oracle.
- Adapter process-lifetime peak RSS from `/proc/PID/status` `VmHWM`, or the
  supplied container `memory.peak`. It is cumulative across requests and is
  not a per-request memory allocation figure. A host-launched adapter has no
  256 MiB container cap, so RSS can expose a bound that an actual deployment
  would terminate first. The host process also has no production CPU cap.
- ClickHouse query count, read rows/bytes, maximum query memory, total query
  milliseconds, exception codes and bounded exception messages from
  `system.query_log`. The underlying exception refines the error class when
  the adapter reports a generic scan-budget error. Admin queries
  collect terminal query events only, bounded by server-clock timestamps and
  the selected reader. `QueryStart` is excluded to avoid double counting.
  Log flushing/collection is outside HTTP latency. Reader readiness happens
  before measurement. Collection failures are recorded, not converted to
  zero-cost successes. Collection waits up to two seconds for reader cancellation
  after a client timeout. It marks costs as potentially incomplete if a
  reader query remains active.
- Binary SHA-256, Git HEAD, working-tree `git diff --stat` at run start/end,
  and the optional diffstat captured before the frozen binary was built.

The Markdown groups the nine shapes by range/service, with a tenth overview
volume request. It sums ClickHouse read rows across shapes. Failed queries
can have substantial cost. It reports the maximum database query memory and
adapter lifetime RSS per group. The JSON retains every individual request.

No cache is flushed. Matrix order, background merges and filesystem cache
state affect latency. Compare the same data, binary/configuration and order,
and repeat runs if measuring timing differences. Large ranges rejected by
HTTP validation should have no reader queries. Comparing the two profiles
shows whether the two-million-row bound hid memory, scan or timeout limits.
The table/index layout and maps also make it possible for a rare service to
hit read limits even when very few rows match it.

Run focused tool checks with:

```sh
go test ./tools/bench
go vet ./tools/bench
python3 -m py_compile testdata/bench/generate.py testdata/bench/live.py testdata/bench/verify.py
```

## Measured run on 2026-10-27

[Combined results table and observations](results/README.md).

The saved dataset has 100,000,000 historical rows across seven days, plus
64 live-tail rows. Active data was 13.0 GiB, or 77.6 GiB uncompressed.
Bulk inserts took 969.216 seconds; including the full service census took
1,156.601 seconds. Validation found 20 services, exactly 18 log and 12
resource attributes in the three-million-row incident sample, and an even
split of JSON/logfmt/plain bodies. Last-15-minute rates were 1,999.696/s
for `edge-gateway`, 500.041/s for `checkout`, and 0.187/s for
`archive-sweeper`. The dataset's fixed historical end is
`2026-10-27T15:20:00Z`.

The frozen baseline binary SHA-256 starts `e7ff24081b1e8e02`; its build
snapshot had 34 tracked files changed, 1,331 insertions and 503 deletions.
The separate current-workspace binary starts `8298bdc515bf9ef7`; its build
snapshot had 41 tracked files changed, 2,122 insertions and 554 deletions.
The source diffstats are saved alongside the reports, and the current build
also has a saved short status including untracked source files. Neither
snapshot was committed. Work by the other agents continued afterward.

- [Dataset manifest](results/dataset.json) and [validation](results/validation.json).
- [Frozen baseline](results/baseline.md) and [per-request JSON](results/baseline.json).
- [Frozen wider-reader startup failure](results/baseline-wide.md).
- [Current snapshot, production reader](results/current-strict.md).
- [Current snapshot, wider reader](results/current-wide.md).

The frozen production-profile baseline rejected all 185 requests: 74
requests at 15m/1h hit the two-million-row limit during index analysis, and
111 requests over 6h/24h/7d hit the old time-range validation. No actual rows
were read for those failures. The wider frozen baseline could not pass
readiness because it tried to change the wider account's row setting under
`readonly=1`. That connection failure is separate from the query matrix.

The current-workspace measurements exercise work in progress, not an accepted
release. They preserve its observed successes and failures without claiming
result correctness. See the saved reports for all shapes and limits. In one
15-minute hot-service field-discovery request, the wider reader allowed
80,193,931 rows of database reads before the 15-second request deadline.
This is total reads across SQL queries, including repeated planning scans,
not 80 million distinct matching records.

## Stop, reset and remove

To retain the large dataset and stop only this project:

```sh
/usr/libexec/docker/cli-plugins/docker-compose -f testdata/bench/compose.yaml stop
```

To regenerate, first stop any generator. Cancel outstanding inserts only
against this synthetic table, then truncate only this table. A killed Python
HTTP client can leave its current insert running, so truncating before
cancellation finishes can leave late rows in a supposedly empty table:

```sh
python3 - <<'PY'
import sys
sys.path.insert(0, 'testdata/bench')
from generate import sql
sql("KILL QUERY WHERE user='bench_admin' AND position(query, 'INSERT INTO bench.otel_logs') > 0 AND query_id != currentQueryID() SYNC")
sql('TRUNCATE TABLE bench.otel_logs')
PY
```

To reclaim the large table's disk, run the reset commands above while the
bench service is running. Then remove only this project's containers, network
and named volume object:

```sh
/usr/libexec/docker/cli-plugins/docker-compose -f testdata/bench/compose.yaml down -v
```

That command is authorized only for this bench project. Never substitute the
fixture or deployment Compose file. The bind-backed directory remains on
disk when Docker removes the named volume object. Truncating the table first
reclaims its large data files; a small amount of database metadata and system
logs can remain. Keep the reports and source provenance as needed. The data
directory, binaries and logs are gitignored; no commit is required for this
task.
