# Local synthetic benchmark

Reader `bench_reader_wide`, 185 requests, binary SHA-256 `e8269829c50a76cc7b83996af4ad3b16ff85f3c97ecb9050853af03a3ade159d`.

RSS is the adapter process-lifetime high-water mark. ClickHouse memory is the largest query peak in each group. Latency excludes query-log collection.

| Range | Scope | OK / total | Error classes | Median / max ms | CH read rows | CH peak MiB | Adapter peak MiB |
|---|---|---:|---|---:|---:|---:|---:|
| 15m | hot | 8 / 9 | scan_rows:1 | 163.3 / 4337.9 | 3628589 | 18.7 | 52.3 |
| 15m | medium | 8 / 9 | scan_rows:1 | 85.4 / 3585.4 | 1955669 | 18.4 | 54.2 |
| 15m | tail | 9 / 9 |  | 112.0 / 196.3 | 699400 | 15.4 | 54.4 |
| 15m | overview | 9 / 10 | timeout:1 | 291.6 / 15000.5 | 28218269 | 20.2 | 54.2 |
| 1h | hot | 8 / 9 | timeout:1 | 150.5 / 15000.6 | 16140567 | 22.1 | 54.2 |
| 1h | medium | 8 / 9 | scan_rows:1 | 107.4 / 4745.6 | 4025102 | 22.1 | 54.2 |
| 1h | tail | 9 / 9 |  | 212.2 / 385.0 | 1633579 | 22.5 | 54.2 |
| 1h | overview | 9 / 10 | timeout:1 | 978.5 / 15009.9 | 62683246 | 19.5 | 54.2 |
| 6h | hot | 8 / 9 | scan_rows:1 | 162.7 / 12817.9 | 17772953 | 26.6 | 54.2 |
| 6h | medium | 8 / 9 | scan_rows:1 | 127.3 / 7423.7 | 7184041 | 23.4 | 54.2 |
| 6h | tail | 9 / 9 |  | 363.1 / 757.1 | 4093468 | 22.5 | 54.2 |
| 6h | overview | 9 / 10 | timeout:1 | 1138.1 / 15000.5 | 73283896 | 19.5 | 54.2 |
| 24h | hot | 8 / 9 | timeout:1 | 255.7 / 15001.4 | 129089120 | 27.2 | 54.2 |
| 24h | medium | 8 / 9 | scan_rows:1 | 176.2 / 9332.4 | 13561663 | 23.7 | 54.2 |
| 24h | tail | 9 / 9 |  | 655.4 / 2287.4 | 13291506 | 22.5 | 54.2 |
| 24h | overview | 9 / 10 | timeout:1 | 1232.8 / 15001.4 | 111193688 | 19.4 | 54.2 |
| 7d | hot | 8 / 9 | timeout:1 | 1005.3 / 15001.5 | 117070191 | 29.8 | 54.2 |
| 7d | medium | 8 / 9 | timeout:1 | 641.3 / 15001.1 | 59131491 | 28.4 | 54.2 |
| 7d | tail | 8 / 9 | timeout:1 | 2868.4 / 15000.9 | 70773765 | 34.2 | 54.2 |
| 7d | overview | 7 / 10 | timeout:3 | 2544.4 / 15001.6 | 339849346 | 21.3 | 54.2 |

Full per-request queries, status, errors, sizes, timings, read bytes, query counts and provenance are in the matching JSON report.

Intermediate evidence, superseded by validated-grid reports.
