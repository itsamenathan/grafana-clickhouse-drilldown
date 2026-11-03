# Local synthetic benchmark

Reader `bench_reader`, 185 requests, binary SHA-256 `8298bdc515bf9ef730956496e11fa482b72969717d448e52b914e1a5b300788e`.

RSS is the adapter process-lifetime high-water mark. ClickHouse memory is the largest query peak in each group. Latency excludes query-log collection.

| Range | Scope | OK / total | Error classes | Median / max ms | CH read rows | CH peak MiB | Adapter peak MiB |
|---|---|---:|---|---:|---:|---:|---:|
| 15m | hot | 4 / 9 | read_rows:5 | 8.4 / 1327.4 | 1783240 | 15.2 | 70.8 |
| 15m | medium | 4 / 9 | read_rows:5 | 8.9 / 1939.9 | 2514716 | 15.4 | 70.8 |
| 15m | tail | 4 / 9 | read_rows:5 | 31.3 / 7343.8 | 21978854 | 28.6 | 70.8 |
| 15m | overview | 3 / 10 | read_rows:6, retained_bytes:1 | 9.2 / 1304.7 | 1746687 | 23.2 | 70.8 |
| 1h | hot | 4 / 9 | read_rows:5 | 9.3 / 756.8 | 430342 | 13.6 | 73.2 |
| 1h | medium | 4 / 9 | read_rows:5 | 8.4 / 827.0 | 554368 | 13.4 | 73.2 |
| 1h | tail | 0 / 9 | read_rows:9 | 11.8 / 490.4 | 601994 | 18.8 | 72.6 |
| 1h | overview | 3 / 10 | read_rows:6, retained_bytes:1 | 9.5 / 1425.5 | 1670986 | 23.2 | 72.6 |
| 6h | hot | 4 / 9 | read_rows:5 | 51.8 / 673.4 | 424985 | 13.6 | 72.6 |
| 6h | medium | 4 / 9 | read_rows:5 | 12.9 / 812.4 | 554368 | 13.4 | 72.6 |
| 6h | tail | 0 / 9 | read_rows:9 | 12.3 / 375.7 | 580282 | 19.9 | 72.6 |
| 6h | overview | 3 / 10 | read_rows:6, retained_bytes:1 | 13.6 / 1439.0 | 1670986 | 23.2 | 72.6 |
| 24h | hot | 4 / 9 | read_rows:5 | 18.7 / 767.4 | 424985 | 13.6 | 72.7 |
| 24h | medium | 4 / 9 | read_rows:5 | 11.5 / 828.9 | 554368 | 13.4 | 72.7 |
| 24h | tail | 0 / 9 | read_rows:9 | 16.4 / 419.3 | 580282 | 19.9 | 72.7 |
| 24h | overview | 3 / 10 | read_rows:6, retained_bytes:1 | 14.9 / 1240.0 | 1670986 | 23.2 | 72.7 |
| 7d | hot | 4 / 9 | read_rows:5 | 38.1 / 750.7 | 424985 | 13.6 | 73.1 |
| 7d | medium | 4 / 9 | read_rows:5 | 17.1 / 753.0 | 554368 | 11.8 | 73.1 |
| 7d | tail | 0 / 9 | read_rows:9 | 20.5 / 462.7 | 580282 | 18.9 | 73.0 |
| 7d | overview | 3 / 10 | read_rows:6, retained_bytes:1 | 17.1 / 1500.0 | 1670986 | 23.2 | 73.0 |

Full per-request queries, status, errors, sizes, timings, read bytes, query counts and provenance are in the matching JSON report.
