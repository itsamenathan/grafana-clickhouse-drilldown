# grafana-clickhouse-drilldown

A Go service providing a bounded Loki-compatible query API over existing
ClickHouse OpenTelemetry logs, for Grafana Logs Drilldown and Explore.

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

## Features

- Log queries, discovery, field values, byte volumes, bounded sliding-window
  metrics and per-source Drilldown capabilities.
- JSON, logfmt, label-filter and pattern line-filter stages use Loki's exact
  implementations after the shared parameterized SQL selection.
- Patterns: Drilldown's Patterns tab, computed on demand with Loki's own Drain
  implementation over at most the newest 50,000 selected lines. Nothing is persisted.
- Live tail: Loki's websocket tail endpoint, used by Grafana Explore's Live
  mode. Drilldown 3.0 itself has no live-tail control.

Unsupported operations return explicit errors. This is not full Loki or
arbitrary LogQL compatibility. See the compatibility matrix and
[operating instructions](docs/operations.md).

## Development

Use the pinned patched Go 1.26.10 toolchain:

```sh
GOTOOLCHAIN=go1.26.10 go test -race ./...
GOTOOLCHAIN=go1.26.10 go vet ./...
GOTOOLCHAIN=go1.26.10 go test ./internal/sqlgen -run '^$' -fuzz '^FuzzCompile$' -fuzztime=15s -parallel=2
GOTOOLCHAIN=go1.26.10 go build -o /tmp/grafana-clickhouse-drilldown ./cmd/adapter
```

Runtime configuration uses `CLICKHOUSE_ADDRESS`, `CLICKHOUSE_DATABASE`,
`CLICKHOUSE_TABLE`, `CLICKHOUSE_USER`, and either `CLICKHOUSE_PASSWORD_FILE` or
`CLICKHOUSE_PASSWORD`. Do not pass passwords as command arguments. The default
listener is `127.0.0.1:3100`; `ADAPTER_LISTEN` overrides it for the private
container network. `ADAPTER_ALLOWED_ORIGINS` lists browser origins, such as
Grafana's public URL, allowed to open tail websockets through Grafana's
data-source proxy. There is no independent authentication or tenant routing.
Never expose this listener directly on a LAN, VPN, or public network.

Query time ranges have no default cap. `ADAPTER_MAX_QUERY_RANGE` sets an
optional Go duration, with `0s` disabling it. `ADAPTER_REQUEST_TIMEOUT` defaults
to `15s`; keep Grafana's data source `jsonData.timeout` at least this long,
currently `20s` on the deployed source. Work still fails explicitly at its cost
or output bounds. See [resource limits and configuration](docs/operations.md#readiness-and-resource-bounds).

Queries read ClickHouse in bounded pages. Supported count/byte metrics and
selector-only summaries aggregate in ClickHouse; residual pipelines stream
selected records. Client limits are additional protection, not a
substitute for the separate server-enforced restricted reader profile.
Startup and readiness verify `readonly=1`, throwing overflow modes, and SELECT
access to the configured table. ClickHouse settings come only from the server
reader profile. Service identity defaults to OTel `ServiceName`;
`ADAPTER_SERVICE_NAME_ATTRIBUTES` enables ordered attribute fallbacks. The
deployment and fixtures explicitly enable their journald fallback. See
[canonical mappings](docs/compatibility.md#canonical-mappings).

See [synthetic fixtures](testdata/fixtures/README.md) for the pinned disposable
ClickHouse/Loki/Grafana environment and integration tests. Run database
integration packages serially against their shared two-query fixture reader:
`go test -p=1 -tags=integration -race -count=1 ./...`.

See `AGENTS.md` for the infrastructure handbook and operating boundaries. No
production adapter ports are published. The matching application/dependency
source and notices are available through the authenticated source offer.
