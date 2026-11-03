# Local synthetic benchmark

Reader `bench_reader_wide`, 185 requests, binary SHA-256 `15ed67e76745d4c2dd245658aeafc3ac290d8d2bd04be675d8ea3029dfc82d27`.

RSS is the adapter process-lifetime high-water mark. ClickHouse memory is the largest query peak in each group. Latency excludes query-log collection.

| Range | Scope | OK / total | Error classes | Median / max ms | CH read rows | CH peak MiB | Adapter peak MiB |
|---|---|---:|---|---:|---:|---:|---:|
| 15m | hot | 8 / 9 | scan_rows:1 | 232.7 / 4415.3 | 3735137 | 18.7 | 55.3 |
| 15m | medium | 8 / 9 | scan_rows:1 | 112.1 / 3806.3 | 2032130 | 18.4 | 55.0 |
| 15m | tail | 9 / 9 |  | 113.4 / 219.1 | 699400 | 15.4 | 55.2 |
| 15m | overview | 9 / 10 | timeout:1 | 621.3 / 15001.6 | 27366913 | 20.1 | 56.3 |
| 1h | hot | 8 / 9 | timeout:1 | 455.5 / 15000.8 | 16166037 | 22.1 | 58.4 |
| 1h | medium | 8 / 9 | scan_rows:1 | 208.4 / 4994.9 | 4025102 | 23.4 | 58.4 |
| 1h | tail | 9 / 9 |  | 261.0 / 450.2 | 1633579 | 22.5 | 58.4 |
| 1h | overview | 9 / 10 | timeout:1 | 2054.0 / 15000.9 | 62134506 | 19.5 | 58.4 |
| 6h | hot | 8 / 9 | scan_rows:1 | 754.7 / 14211.3 | 54398795 | 23.9 | 58.4 |
| 6h | medium | 8 / 9 | scan_rows:1 | 283.9 / 8795.6 | 38251191 | 23.4 | 58.4 |
| 6h | tail | 9 / 9 |  | 513.1 / 800.0 | 7304943 | 22.5 | 58.4 |
| 6h | overview | 9 / 10 | timeout:1 | 2538.4 / 15001.0 | 72609599 | 19.5 | 58.4 |
| 24h | hot | 8 / 9 | timeout:1 | 932.3 / 15001.6 | 182049381 | 24.8 | 58.4 |
| 24h | medium | 8 / 9 | scan_rows:1 | 654.6 / 14009.2 | 181718658 | 23.7 | 66.8 |
| 24h | tail | 9 / 9 |  | 1037.4 / 2400.2 | 24679732 | 22.2 | 66.8 |
| 24h | overview | 9 / 10 | timeout:1 | 4099.0 / 15000.8 | 126252075 | 18.4 | 66.8 |
| 7d | hot | 6 / 9 | timeout:3 | 929.7 / 15001.7 | 303074183 | 29.9 | 66.8 |
| 7d | medium | 6 / 9 | timeout:3 | 897.6 / 15001.3 | 378945102 | 28.3 | 66.8 |
| 7d | tail | 8 / 9 | timeout:1 | 5983.1 / 15002.3 | 108340664 | 34.6 | 66.8 |
| 7d | overview | 5 / 10 | timeout:5 | 15000.6 / 15001.4 | 332887989 | 21.3 | 66.8 |

Full per-request queries, status, errors, sizes, timings, read bytes, query counts and provenance are in the matching JSON report.
