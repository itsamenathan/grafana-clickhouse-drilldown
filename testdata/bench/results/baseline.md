# Local synthetic benchmark

Reader `bench_reader`, 185 requests, binary SHA-256 `e7ff24081b1e8e023a8ca9b52d9e2f36d2244e6b2ee8ce28fd93a6b44c915ef7`.

RSS is the adapter process-lifetime high-water mark. ClickHouse memory is the largest query peak in each group. Latency excludes query-log collection.

| Range | Scope | OK / total | Error classes | Median / max ms | CH read rows | CH peak MiB | Adapter peak MiB |
|---|---|---:|---|---:|---:|---:|---:|
| 15m | hot | 0 / 9 | read_rows:9 | 8.0 / 59.0 | 0 | 0.0 | 38.4 |
| 15m | medium | 0 / 9 | read_rows:9 | 7.5 / 47.9 | 0 | 0.0 | 38.6 |
| 15m | tail | 0 / 9 | read_rows:9 | 7.2 / 41.8 | 0 | 0.0 | 38.8 |
| 15m | overview | 0 / 10 | read_rows:10 | 6.6 / 47.6 | 0 | 0.0 | 39.3 |
| 1h | hot | 0 / 9 | read_rows:9 | 8.2 / 49.5 | 0 | 0.0 | 39.0 |
| 1h | medium | 0 / 9 | read_rows:9 | 7.2 / 11.6 | 0 | 0.0 | 40.0 |
| 1h | tail | 0 / 9 | read_rows:9 | 8.1 / 46.6 | 0 | 0.0 | 40.1 |
| 1h | overview | 0 / 10 | read_rows:10 | 7.8 / 15.4 | 0 | 0.0 | 40.3 |
| 6h | hot | 0 / 9 | time_range:9 | 0.3 / 0.9 | 0 | 0.0 | 40.8 |
| 6h | medium | 0 / 9 | time_range:9 | 0.4 / 0.8 | 0 | 0.0 | 41.1 |
| 6h | tail | 0 / 9 | time_range:9 | 0.3 / 1.3 | 0 | 0.0 | 41.7 |
| 6h | overview | 0 / 10 | time_range:10 | 0.3 / 1.7 | 0 | 0.0 | 41.8 |
| 24h | hot | 0 / 9 | time_range:9 | 0.3 / 0.7 | 0 | 0.0 | 41.3 |
| 24h | medium | 0 / 9 | time_range:9 | 0.3 / 1.1 | 0 | 0.0 | 41.4 |
| 24h | tail | 0 / 9 | time_range:9 | 0.3 / 1.1 | 0 | 0.0 | 41.3 |
| 24h | overview | 0 / 10 | time_range:10 | 0.5 / 1.0 | 0 | 0.0 | 41.3 |
| 7d | hot | 0 / 9 | time_range:9 | 0.3 / 1.0 | 0 | 0.0 | 41.4 |
| 7d | medium | 0 / 9 | time_range:9 | 0.3 / 1.0 | 0 | 0.0 | 41.3 |
| 7d | tail | 0 / 9 | time_range:9 | 0.5 / 1.2 | 0 | 0.0 | 41.3 |
| 7d | overview | 0 / 10 | time_range:10 | 0.6 / 0.9 | 0 | 0.0 | 41.3 |

Full per-request queries, status, errors, sizes, timings, read bytes, query counts and provenance are in the matching JSON report.
