# Local synthetic benchmark

Reader `bench_reader_wide`, 185 requests, binary SHA-256 `50d80365d8130d536f01f658fb1d93355de62449da880dd279f2e37b4cb0d170`.

RSS is the adapter process-lifetime high-water mark. ClickHouse memory is the largest query peak in each group. Latency excludes query-log collection.

| Range | Scope | OK / total | Error classes | Median / max ms | CH read rows | CH peak MiB | Adapter peak MiB |
|---|---|---:|---|---:|---:|---:|---:|
| 15m | hot | 8 / 9 | timeout:1 | 1223.4 / 15000.9 | 37416648 | 57.0 | 51.6 |
| 15m | medium | 8 / 9 | scan_rows:1 | 397.3 / 6530.1 | 4469475 | 56.4 | 51.6 |
| 15m | tail | 9 / 9 |  | 335.7 / 568.9 | 1528830 | 22.4 | 51.6 |
| 15m | overview | 9 / 10 | timeout:1 | 2135.1 / 15000.7 | 24033307 | 57.1 | 54.3 |
| 1h | hot | 8 / 9 | timeout:1 | 1378.5 / 15001.0 | 31251773 | 55.9 | 54.6 |
| 1h | medium | 8 / 9 | timeout:1 | 1303.2 / 15001.2 | 40025338 | 55.7 | 54.6 |
| 1h | tail | 9 / 9 |  | 620.6 / 6205.9 | 10998179 | 25.6 | 54.6 |
| 1h | overview | 9 / 10 | timeout:1 | 7914.1 / 15000.9 | 58869057 | 56.2 | 55.1 |
| 6h | hot | 8 / 9 | timeout:1 | 1468.7 / 15000.9 | 37927687 | 56.9 | 56.7 |
| 6h | medium | 8 / 9 | timeout:1 | 1503.0 / 15001.0 | 66876019 | 56.6 | 56.7 |
| 6h | tail | 9 / 9 |  | 675.7 / 10864.1 | 22730382 | 26.0 | 56.7 |
| 6h | overview | 9 / 10 | timeout:1 | 9411.1 / 15001.1 | 69387034 | 57.2 | 56.7 |
| 24h | hot | 8 / 9 | timeout:1 | 1420.6 / 15001.1 | 64880957 | 57.4 | 56.7 |
| 24h | medium | 8 / 9 | timeout:1 | 1526.9 / 15001.2 | 77331896 | 57.4 | 56.7 |
| 24h | tail | 6 / 9 | timeout:3 | 8812.9 / 15001.4 | 176315028 | 57.2 | 56.7 |
| 24h | overview | 5 / 10 | timeout:5 | 15000.6 / 15001.5 | 93562264 | 57.6 | 56.9 |
| 7d | hot | 5 / 9 | timeout:4 | 1365.5 / 15001.7 | 71795349 | 61.1 | 56.7 |
| 7d | medium | 5 / 9 | timeout:4 | 1467.7 / 15001.5 | 73219115 | 57.5 | 56.7 |
| 7d | tail | 4 / 9 | timeout:5 | 15001.1 / 15001.8 | 135136049 | 57.3 | 56.7 |
| 7d | overview | 5 / 10 | timeout:5 | 15000.7 / 15001.4 | 89435770 | 57.9 | 56.7 |

Full per-request queries, status, errors, sizes, timings, read bytes, query counts and provenance are in the matching JSON report.
