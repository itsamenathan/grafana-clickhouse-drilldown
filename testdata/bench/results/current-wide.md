# Local synthetic benchmark

Reader `bench_reader_wide`, 185 requests, binary SHA-256 `8298bdc515bf9ef730956496e11fa482b72969717d448e52b914e1a5b300788e`.

RSS is the adapter process-lifetime high-water mark. ClickHouse memory is the largest query peak in each group. Latency excludes query-log collection.

| Range | Scope | OK / total | Error classes | Median / max ms | CH read rows | CH peak MiB | Adapter peak MiB |
|---|---|---:|---|---:|---:|---:|---:|
| 15m | hot | 6 / 9 | scan_rows:1, timeout:2 | 2011.7 / 15001.0 | 200570110 | 23.0 | 70.9 |
| 15m | medium | 7 / 9 | scan_rows:2 | 1221.3 / 14448.9 | 128981276 | 24.6 | 70.9 |
| 15m | tail | 9 / 9 |  | 663.3 / 3583.7 | 19214174 | 19.9 | 70.9 |
| 15m | overview | 6 / 10 | retained_bytes:1, timeout:3 | 2485.8 / 15001.5 | 146709236 | 23.2 | 73.1 |
| 1h | hot | 6 / 9 | timeout:3 | 9020.2 / 15001.3 | 90099827 | 25.6 | 73.1 |
| 1h | medium | 6 / 9 | scan_rows:1, timeout:2 | 6786.9 / 15001.3 | 161436689 | 26.1 | 73.1 |
| 1h | tail | 9 / 9 |  | 6451.6 / 12113.0 | 105867606 | 22.3 | 73.1 |
| 1h | overview | 6 / 10 | retained_bytes:1, timeout:3 | 8690.2 / 15001.3 | 92188723 | 23.2 | 73.1 |
| 6h | hot | 6 / 9 | timeout:3 | 10344.0 / 15000.8 | 76699965 | 30.0 | 73.1 |
| 6h | medium | 6 / 9 | timeout:3 | 8568.7 / 15000.8 | 136030496 | 28.1 | 73.1 |
| 6h | tail | 9 / 9 |  | 8250.6 / 11323.1 | 110183354 | 24.0 | 73.1 |
| 6h | overview | 6 / 10 | retained_bytes:1, timeout:3 | 10607.3 / 15001.5 | 154144262 | 23.2 | 73.1 |
| 24h | hot | 4 / 9 | timeout:5 | 15000.7 / 15001.1 | 93162214 | 28.5 | 73.1 |
| 24h | medium | 5 / 9 | timeout:4 | 14997.0 / 15001.4 | 259934661 | 29.0 | 73.1 |
| 24h | tail | 6 / 9 | timeout:3 | 9117.1 / 15001.5 | 170921930 | 26.7 | 73.1 |
| 24h | overview | 3 / 10 | retained_bytes:1, timeout:6 | 15001.3 / 15001.5 | 109692204 | 23.2 | 73.1 |
| 7d | hot | 4 / 9 | timeout:5 | 15000.7 / 15001.0 | 71296588 | 30.0 | 73.1 |
| 7d | medium | 4 / 9 | timeout:5 | 15000.5 / 15005.0 | 78214089 | 30.1 | 73.1 |
| 7d | tail | 2 / 9 | timeout:7 | 15000.8 / 15030.5 | 131872270 | 29.0 | 73.1 |
| 7d | overview | 3 / 10 | retained_bytes:1, timeout:6 | 15000.9 / 15001.2 | 88650132 | 23.2 | 73.1 |

Full per-request queries, status, errors, sizes, timings, read bytes, query counts and provenance are in the matching JSON report.
