# Local synthetic benchmark

Reader `bench_reader_wide`, 75 requests, binary SHA-256 `bc5435497c69140ff8d0abc59974f6045c8e3688903cd2f0cc56bc3f6b06325b`.

RSS is the adapter process-lifetime high-water mark. ClickHouse memory is the largest query peak in each group. Latency excludes query-log collection.

| Range | Scope | OK / total | Error classes | Median / max ms | CH read rows | CH peak MiB | Adapter peak MiB |
|---|---|---:|---|---:|---:|---:|---:|
| 15m | hot | 7 / 9 | scan_rows:2 | 280.2 / 5211.8 | 12346926 | 18.8 | 63.5 |
| 15m | medium | 8 / 9 | scan_rows:1 | 191.7 / 4140.7 | 8333409 | 18.4 | 63.5 |
| 15m | tail | 9 / 9 |  | 133.2 / 343.6 | 988149 | 22.5 | 63.5 |
| 15m | overview | 8 / 10 | timeout:2 | 657.8 / 15001.3 | 227133585 | 20.1 | 63.5 |
| 1h | hot | 7 / 9 | scan_rows:1, timeout:1 | 513.5 / 15001.0 | 48156370 | 22.2 | 65.6 |
| 1h | medium | 7 / 9 | scan_rows:2 | 209.5 / 5270.7 | 8911892 | 23.4 | 66.8 |
| 1h | tail | 9 / 9 |  | 242.7 / 536.6 | 1920099 | 22.4 | 66.8 |
| 1h | overview | 8 / 10 | timeout:2 | 2111.5 / 15001.4 | 190995089 | 19.6 | 66.8 |
| 6h | hot | 0 / 1 | transport:1 | 660.3 / 660.3 | 0 | 0.0 | 66.8 |

Full per-request queries, status, errors, sizes, timings, read bytes, query counts and provenance are in the matching JSON report.

Incomplete exploratory run. Superseded by the 20261027b reports. The initial
all-backslash guard forced streaming for escaped JSON path separators.
