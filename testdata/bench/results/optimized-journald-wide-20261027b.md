# Local synthetic benchmark

Reader `bench_reader_wide`, 26 requests, binary SHA-256 `15ed67e76745d4c2dd245658aeafc3ac290d8d2bd04be675d8ea3029dfc82d27`.

RSS is the adapter process-lifetime high-water mark. ClickHouse memory is the largest query peak in each group. Latency excludes query-log collection.

| Range | Scope | OK / total | Error classes | Median / max ms | CH read rows | CH peak MiB | Adapter peak MiB |
|---|---|---:|---|---:|---:|---:|---:|
| 15m | hot | 8 / 9 | timeout:1 | 1511.3 / 15001.3 | 53398721 | 22.9 | 52.7 |
| 15m | medium | 8 / 9 | scan_rows:1 | 390.2 / 6703.5 | 8374243 | 24.1 | 52.7 |
| 15m | tail | 7 / 8 | transport:1 | 2152.0 / 6498.4 | 18235483 | 29.7 | 52.7 |

Full per-request queries, status, errors, sizes, timings, read bytes, query counts and provenance are in the matching JSON report.

Incomplete intermediate run. Superseded by optimized-final reports.
