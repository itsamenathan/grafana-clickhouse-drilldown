# grafana-clickhouse-drilldown

A Go service providing a bounded Loki-compatible query API over existing
ClickHouse OpenTelemetry logs, for Grafana Logs Drilldown.

The service uses a shared, tested LogQL-to-SQL compiler, read-only database
access, and bounded query concurrency. It rejects unsupported queries
instead of silently ignoring filters. It does not ingest logs, duplicate
storage, or provide a separate frontend.

Deployed on observability with a separate restricted reader and non-default
Grafana source, UID `clickhouse-loki-go`. The user approved Loki's pinned
parser and AGPL-3.0-only. See the
[parser decision](docs/parser-decision.md) and
[compatibility status](docs/compatibility.md) for verified behavior and gaps.
[Open Logs Drilldown](https://grafana.h.frcv.net/a/grafana-lokiexplore-app/explore?var-ds=clickhouse-loki-go&from=now-15m&to=now).
The old experimental adapter was retired after acceptance passed.

## Development

Use the pinned patched Go 1.26.10 toolchain:

```sh
GOTOOLCHAIN=go1.26.10 go test -race ./...
GOTOOLCHAIN=go1.26.10 go vet ./...
GOTOOLCHAIN=go1.26.10 go test ./internal/sqlgen -run '^$' -fuzz '^FuzzCompile$' -fuzztime=15s -parallel=2
GOTOOLCHAIN=go1.26.10 go build -o /tmp/opencode/grafana-clickhouse-drilldown ./cmd/adapter
```

The HTTP service implements log queries, discovery, field values, byte volumes,
bounded sliding-window metrics and per-source Drilldown capabilities. JSON and
logfmt stages use Loki's exact implementations after the shared parameterized
SQL selection. Unsupported operations return explicit errors. See the
compatibility matrix and [operating instructions](docs/operations.md). This is
not full Loki or arbitrary LogQL compatibility. Patterns and live tailing are
explicitly unsupported.

Runtime configuration uses `CLICKHOUSE_ADDRESS`, `CLICKHOUSE_DATABASE`,
`CLICKHOUSE_TABLE`, `CLICKHOUSE_USER`, and either `CLICKHOUSE_PASSWORD_FILE` or
`CLICKHOUSE_PASSWORD`. Do not pass passwords as command arguments. The default
listener is `127.0.0.1:3100`; `ADAPTER_LISTEN` overrides it for the private
container network. There is no independent authentication or tenant routing.
Never expose this listener directly on a LAN, VPN, or public network.

The client uses two connections, a two-active/32-waiting admission queue, and
query deadlines. Client limits are additional protection, not a substitute for
the separate server-enforced restricted reader profile. Readiness performs an
actual SELECT against the configured table. The restricted fixture user and
native driver cancellation are tested. Live acceptance passed all 12 API/SQL
checks, actual UI controls/context, and a 20-service overview in 4.2 seconds.
The adapter used about 38 MiB sampled memory under that load, below its 256 MiB
limit. SQL, PromQL, the default data source and ingestion remain unchanged.

See [synthetic fixtures](testdata/fixtures/README.md) for the pinned disposable
ClickHouse/Loki environment and integration tests. Local Docker/Compose and
the pinned ClickHouse image now work after the user enabled host CPU
passthrough. The ClickHouse subset integration tests pass with the race
detector, including restricted-reader behavior and native query cancellation.
Pinned real-Loki comparisons and browser acceptance pass for the documented
contract. Run database integration packages serially against their shared
two-query fixture reader: `go test -p=1 -tags=integration -race -count=1 ./...`.

See `AGENTS.md` for the infrastructure handbook and operating boundaries. No
production adapter ports are published. The matching application/dependency
source and notices are available through the authenticated source offer.
