# Local synthetic benchmark

Reader `bench_reader_wide`, 185 requests, binary SHA-256 `50d80365d8130d536f01f658fb1d93355de62449da880dd279f2e37b4cb0d170`.

RSS is the adapter process-lifetime high-water mark. ClickHouse memory is the largest query peak in each group. Latency excludes query-log collection.

| Range | Scope | OK / total | Error classes | Median / max ms | CH read rows | CH peak MiB | Adapter peak MiB |
|---|---|---:|---|---:|---:|---:|---:|
| 15m | hot | 8 / 9 | scan_rows:1 | 160.9 / 4200.6 | 3628589 | 18.7 | 54.1 |
| 15m | medium | 8 / 9 | scan_rows:1 | 73.3 / 4516.8 | 1955669 | 18.0 | 58.7 |
| 15m | tail | 9 / 9 |  | 117.2 / 235.4 | 699400 | 15.4 | 58.7 |
| 15m | overview | 9 / 10 | timeout:1 | 363.5 / 15000.8 | 27528987 | 20.1 | 58.7 |
| 1h | hot | 8 / 9 | timeout:1 | 145.4 / 15001.3 | 16210010 | 24.1 | 58.7 |
| 1h | medium | 8 / 9 | scan_rows:1 | 142.5 / 4464.6 | 4025102 | 23.4 | 58.7 |
| 1h | tail | 9 / 9 |  | 218.4 / 429.2 | 1633579 | 22.5 | 58.7 |
| 1h | overview | 9 / 10 | timeout:1 | 936.3 / 15001.5 | 62979390 | 20.2 | 58.7 |
| 6h | hot | 8 / 9 | scan_rows:1 | 147.7 / 12413.3 | 17772953 | 26.5 | 58.7 |
| 6h | medium | 8 / 9 | scan_rows:1 | 135.4 / 7313.9 | 7184041 | 23.4 | 58.7 |
| 6h | tail | 9 / 9 |  | 362.6 / 746.7 | 4093468 | 22.5 | 58.7 |
| 6h | overview | 9 / 10 | timeout:1 | 1247.8 / 15001.5 | 73633635 | 19.5 | 58.7 |
| 24h | hot | 8 / 9 | timeout:1 | 325.0 / 15000.6 | 89044233 | 27.3 | 58.7 |
| 24h | medium | 8 / 9 | scan_rows:1 | 234.2 / 9214.3 | 13561663 | 24.8 | 58.7 |
| 24h | tail | 9 / 9 |  | 690.8 / 2603.2 | 13291506 | 23.9 | 58.7 |
| 24h | overview | 9 / 10 | timeout:1 | 1146.8 / 15000.8 | 110019385 | 18.4 | 58.7 |
| 7d | hot | 8 / 9 | timeout:1 | 909.0 / 15001.5 | 107959343 | 29.8 | 58.7 |
| 7d | medium | 8 / 9 | timeout:1 | 929.6 / 15000.9 | 56015596 | 28.2 | 58.7 |
| 7d | tail | 8 / 9 | timeout:1 | 2875.0 / 15000.9 | 64552620 | 31.8 | 58.7 |
| 7d | overview | 7 / 10 | timeout:3 | 2583.2 / 15001.2 | 349406425 | 21.3 | 58.7 |

Full per-request queries, status, errors, sizes, timings, read bytes, query counts and provenance are in the matching JSON report.
