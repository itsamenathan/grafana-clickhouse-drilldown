# Benchmark results, 2026-10-27

100,000,000 historical rows over seven days, plus 64 current tail rows. Active table data is about 13.0 GiB. Inserts took 969.216 seconds; the service census brought total generation to 1,156.601 seconds.

Each full run contains 185 sequential requests. Counts below combine hot, medium, tail and overview selections. A success is an HTTP/API success, without an independent result-correctness claim.

| Range | Frozen baseline OK / 37 | Current, 2M reader OK / 37 | Current, 200M reader OK / 37 | Current 200M failures |
|---|---:|---:|---:|---|
| 15m | 0 | 15 | 28 | retained_bytes: 1, scan_rows: 3, timeout: 5 |
| 1h | 0 | 11 | 27 | retained_bytes: 1, scan_rows: 1, timeout: 8 |
| 6h | 0 | 11 | 27 | retained_bytes: 1, timeout: 9 |
| 24h | 0 | 11 | 18 | retained_bytes: 1, timeout: 18 |
| 7d | 0 | 11 | 13 | retained_bytes: 1, timeout: 23 |

The frozen baseline rejected all 74 requests at 15m/1h during ClickHouse index analysis with `max_rows_to_read=2000000`. Its 111 longer-range requests failed the old time-range validation. Actual database read rows were zero for these rejections.

The frozen wider-reader run executed no matrix. Its hardcoded client row setting cannot be changed under the wider account's `readonly=1`; readiness failed. That startup failure is retained separately, rather than presented as a query result.

The current snapshot uses work in progress from the concurrent agents. It was frozen into a separate binary, and its native row setting matches each reader. All other server reader limits are identical.

- [baseline details](baseline.md), [JSON](baseline.json): 0/185 successes; adapter peak 41.8 MiB, largest ClickHouse query peak 0.0 MiB; total SQL read rows 0.
- [current-strict details](current-strict.md), [JSON](current-strict.json): 59/185 successes; adapter peak 73.2 MiB, largest ClickHouse query peak 28.6 MiB; total SQL read rows 40,973,050.
- [current-wide details](current-wide.md), [JSON](current-wide.json): 113/185 successes; adapter peak 73.1 MiB, largest ClickHouse query peak 30.1 MiB; total SQL read rows 2,425,869,806.

The wider reader exposes 15-second execution deadlines, bounded scans and retained-result limits. No query hit the 128 MiB database memory cap. One 15-minute hot-service detected-fields request read 80,193,931 rows across SQL queries before timing out; repeated planning reads are included in that count. All 555 executed requests have query-log collection without errors.

Peak RSS is a process-lifetime measurement. The host-launched adapter has no production CPU/memory cgroup cap. Latency includes normal cache and background-merge effects.

## Source snapshots

- `e7ff24081b1e8e023a8ca9b52d9e2f36d2244e6b2ee8ce28fd93a6b44c915ef7`: [baseline-source.diffstat](baseline-source.diffstat).
- `8298bdc515bf9ef730956496e11fa482b72969717d448e52b914e1a5b300788e`: [current-source.diffstat](current-source.diffstat).

The original snapshot had 34 tracked files changed, 1,331 insertions and 503 deletions. The current snapshot had 41 tracked files changed, 2,122 insertions and 554 deletions; [its short status](current-source.status) also lists untracked source files. No commit was created. Each JSON report includes run-time diffstats and the build diffstat.

[Dataset manifest](dataset.json), [validation evidence](validation.json), [wider frozen startup failure](baseline-wide.json), and [reproduction instructions](../README.md).
