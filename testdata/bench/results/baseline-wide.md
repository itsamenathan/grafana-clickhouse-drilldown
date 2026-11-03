# Local synthetic benchmark

Reader `bench_reader_wide`, 0 requests, binary SHA-256 `e7ff24081b1e8e023a8ca9b52d9e2f36d2244e6b2ee8ce28fd93a6b44c915ef7`.

No matrix ran: adapter /ready did not become healthy in 30s. See JSON notes for startup diagnostics.

RSS is the adapter process-lifetime high-water mark. ClickHouse memory is the largest query peak in each group. Latency excludes query-log collection.

| Range | Scope | OK / total | Error classes | Median / max ms | CH read rows | CH peak MiB | Adapter peak MiB |
|---|---|---:|---|---:|---:|---:|---:|

Full per-request queries, status, errors, sizes, timings, read bytes, query counts and provenance are in the matching JSON report.
