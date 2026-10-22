# grafana-clickhouse-drilldown

A planned Go service providing Loki-compatible query APIs over existing
ClickHouse OpenTelemetry logs, for Grafana Logs Drilldown.

The service will use a shared, tested LogQL-to-SQL compiler, read-only database
access, and bounded query concurrency. It will reject unsupported queries
instead of silently ignoring filters. It will not ingest logs, duplicate
storage, or provide a separate frontend.

Implementation has not started. Review the implementation plan in this
project's OpenChamber Plans panel before building. Parser dependency and
licensing decisions are the first checkpoint.

Deployment is intended for the existing observability host. See `AGENTS.md`
for the infrastructure handbook and safe operating constraints. The old
experimental adapter must remain available until the replacement passes
its acceptance checks.
