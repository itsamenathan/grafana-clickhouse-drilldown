# Local synthetic benchmark

Reader `bench_reader_wide`, 110 requests, binary SHA-256 `e8269829c50a76cc7b83996af4ad3b16ff85f3c97ecb9050853af03a3ade159d`.

RSS is the adapter process-lifetime high-water mark. ClickHouse memory is the largest query peak in each group. Latency excludes query-log collection.

| Range | Scope | OK / total | Error classes | Median / max ms | CH read rows | CH peak MiB | Adapter peak MiB |
|---|---|---:|---|---:|---:|---:|---:|
| 15m | hot | 8 / 9 | timeout:1 | 1300.3 / 15001.4 | 38954243 | 57.0 | 53.2 |
| 15m | medium | 8 / 9 | scan_rows:1 | 426.3 / 6827.6 | 4469475 | 56.4 | 54.2 |
| 15m | tail | 9 / 9 |  | 376.5 / 3070.6 | 5101282 | 21.2 | 54.2 |
| 15m | overview | 9 / 10 | timeout:1 | 2134.6 / 15001.2 | 24271114 | 57.1 | 54.8 |
| 1h | hot | 8 / 9 | timeout:1 | 1649.8 / 15000.8 | 30954613 | 55.9 | 54.8 |
| 1h | medium | 8 / 9 | timeout:1 | 1299.7 / 15000.5 | 45146824 | 55.7 | 54.8 |
| 1h | tail | 8 / 9 | timeout:1 | 878.9 / 15000.8 | 77497098 | 30.2 | 54.8 |
| 1h | overview | 9 / 10 | timeout:1 | 7498.1 / 15001.2 | 58852852 | 56.2 | 54.9 |
| 6h | hot | 8 / 9 | timeout:1 | 1363.8 / 15001.4 | 38030321 | 56.9 | 54.9 |
| 6h | medium | 8 / 9 | timeout:1 | 1624.5 / 15001.1 | 43051917 | 56.6 | 54.9 |
| 6h | tail | 8 / 9 | timeout:1 | 2087.1 / 15001.2 | 78818510 | 25.9 | 54.9 |
| 6h | overview | 8 / 9 | transport:1 | 1207.4 / 11975.5 | 46757590 | 57.2 | 55.6 |

Full per-request queries, status, errors, sizes, timings, read bytes, query counts and provenance are in the matching JSON report.

Intermediate evidence, superseded by validated-grid reports.
