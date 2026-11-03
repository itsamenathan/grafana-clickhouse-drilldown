# Local synthetic benchmark

Reader `bench_reader_wide`, 172 requests, binary SHA-256 `e54d7531d334938ac8210fa2b86c2ac518d984ff02a4692b6e252f636e095b60`.

RSS is the adapter process-lifetime high-water mark. ClickHouse memory is the largest query peak in each group. Latency excludes query-log collection.

| Range | Scope | OK / total | Error classes | Median / max ms | CH read rows | CH peak MiB | Adapter peak MiB |
|---|---|---:|---|---:|---:|---:|---:|
| 15m | hot | 8 / 9 | scan_rows:1 | 170.7 / 4572.6 | 3628589 | 20.0 | 52.9 |
| 15m | medium | 8 / 9 | scan_rows:1 | 86.0 / 3849.5 | 1955669 | 18.4 | 54.6 |
| 15m | tail | 9 / 9 |  | 124.9 / 209.5 | 699400 | 15.4 | 54.6 |
| 15m | overview | 9 / 10 | timeout:1 | 308.2 / 15000.7 | 28034805 | 20.1 | 57.2 |
| 1h | hot | 8 / 9 | timeout:1 | 148.7 / 15008.3 | 16155719 | 22.2 | 57.2 |
| 1h | medium | 8 / 9 | scan_rows:1 | 126.5 / 5461.7 | 4025102 | 23.4 | 57.2 |
| 1h | tail | 9 / 9 |  | 231.6 / 421.3 | 1633579 | 22.5 | 57.2 |
| 1h | overview | 9 / 10 | timeout:1 | 1001.7 / 15000.9 | 62210184 | 19.6 | 57.2 |
| 6h | hot | 8 / 9 | scan_rows:1 | 148.8 / 12675.4 | 17997209 | 26.6 | 57.2 |
| 6h | medium | 8 / 9 | scan_rows:1 | 114.3 / 7728.2 | 7295435 | 23.4 | 57.2 |
| 6h | tail | 9 / 9 |  | 398.0 / 750.4 | 4104274 | 22.5 | 57.2 |
| 6h | overview | 9 / 10 | timeout:1 | 1117.4 / 15000.5 | 73590871 | 19.5 | 57.2 |
| 24h | hot | 8 / 9 | timeout:1 | 282.2 / 15000.8 | 153493099 | 27.6 | 57.2 |
| 24h | medium | 8 / 9 | scan_rows:1 | 164.4 / 8740.7 | 13600915 | 23.8 | 57.2 |
| 24h | tail | 9 / 9 |  | 749.2 / 2347.8 | 13302422 | 22.5 | 57.2 |
| 24h | overview | 9 / 10 | timeout:1 | 1221.3 / 15001.5 | 111172087 | 18.4 | 57.2 |
| 7d | hot | 6 / 9 | timeout:3 | 1086.6 / 15001.4 | 311152191 | 29.7 | 57.2 |
| 7d | medium | 6 / 9 | timeout:3 | 581.9 / 15001.6 | 314503756 | 27.9 | 57.2 |
| 7d | tail | 5 / 6 | transport:1 | 2737.2 / 3289.5 | 30444863 | 22.5 | 57.2 |

Full per-request queries, status, errors, sizes, timings, read bytes, query counts and provenance are in the matching JSON report.

Incomplete intermediate run. Superseded by optimized-grid reports.
