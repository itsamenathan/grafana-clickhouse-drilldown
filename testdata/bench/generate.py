#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-only
"""Load only deterministic synthetic rows into the fixed, loopback bench database."""
import argparse
import datetime as dt
import hashlib
import json
import math
import pathlib
import shutil
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request

HERE = pathlib.Path(__file__).resolve().parent


def sql(query, **params):
    values = {'user': 'bench_admin', 'password': 'bench-admin-not-production'}
    values.update({'param_' + k: str(v) for k, v in params.items()})
    req = urllib.request.Request('http://127.0.0.1:28123/?' + urllib.parse.urlencode(values),
                                 data=query.encode())
    try:
        with urllib.request.urlopen(req, timeout=300) as response:
            return response.read().decode().strip()
    except urllib.error.HTTPError as error:
        raise RuntimeError(error.read().decode()) from error


def stats():
    return json.loads(sql("SELECT sum(rows) AS rows, sum(bytes_on_disk) AS bytes_on_disk, "
                          "sum(data_uncompressed_bytes) AS uncompressed_bytes, count() AS parts "
                          "FROM system.parts WHERE active AND database='bench' AND table='otel_logs' "
                          "FORMAT JSONEachRow"))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--rows', type=int, default=100_000_000, help='target before scale')
    parser.add_argument('--scale', type=float, default=1.0)
    parser.add_argument('--days', type=int, default=7)
    parser.add_argument('--end', help='fixed UTC end, e.g. 2026-10-27T18:00:00Z')
    parser.add_argument('--batch', type=int, default=100_000)
    parser.add_argument('--max-gib', type=float, default=18.0, help='active data disk budget')
    parser.add_argument('--data-dir', type=pathlib.Path, default=HERE / '.data')
    parser.add_argument('--report', type=pathlib.Path, default=HERE / 'results/dataset.json')
    args = parser.parse_args()
    if args.rows <= 0 or args.scale <= 0 or args.days < 1 or args.batch < 1 or args.max_gib <= 0:
        parser.error('rows, scale, days, batch and max-gib must be positive')
    # Print host checks before generating anything. Never assume Docker's root disk has space.
    for command in [['df', '-h', str(args.data_dir)], ['free', '-h']]:
        subprocess.run(command, check=True)
    free = shutil.disk_usage(args.data_dir).free
    mem = pathlib.Path('/proc/meminfo').read_text()
    available = int(next(x.split()[1] for x in mem.splitlines() if x.startswith('MemAvailable:'))) * 1024
    if free < 2 * 1024**3 or available < 4 * 1024**3:
        raise SystemExit('Need at least 2 GiB disk free and 4 GiB available RAM before generation')
    if int(stats()['rows']):
        raise SystemExit('bench.otel_logs is nonempty; preserve it or explicitly TRUNCATE only this bench table first')
    requested = int(args.rows * args.scale)
    end = dt.datetime.fromisoformat(args.end.replace('Z', '+00:00')) if args.end else dt.datetime.now(dt.timezone.utc).replace(second=0, microsecond=0)
    if end.tzinfo is None:
        raise SystemExit('--end needs a timezone')
    if end > dt.datetime.now(dt.timezone.utc):
        raise SystemExit('--end must not be in the future; live rows must fall outside historical bounds')
    if requested < 1:
        raise SystemExit('Scaled row target must be at least 1')
    query = (HERE / 'generate.sql').read_text()
    started = time.monotonic()
    pilot = min(requested, 1_000_000)
    common = dict(total=requested, scale=args.scale, days=args.days, end_ns=int(end.timestamp()) * 10**9)
    for offset in range(0, pilot, args.batch):
        sql(query, **common, offset=offset, count=min(args.batch, pilot-offset))
    measured = stats()
    # Keep 25% estimation margin, and enough free space for merges plus 2 GiB.
    bytes_per_row = max(int(measured['bytes_on_disk']) / pilot, 1)
    budget = min(args.max_gib * 1024**3, (free - 2 * 1024**3) / 2)
    target = min(requested, math.floor(budget / (bytes_per_row * 1.25)))
    target = max(pilot, target)
    if target != requested:
        # The timestamp mapping depends on total; the small pilot must be regenerated.
        sql('TRUNCATE TABLE bench.otel_logs')
        common['total'] = target
        first = 0
    else:
        first = pilot
    print(json.dumps({'pilot': measured, 'projected_gib': requested * bytes_per_row / 1024**3,
                      'requested_rows': requested, 'selected_rows': target}), flush=True)
    for offset in range(first, target, args.batch):
        sql(query, **common, offset=offset, count=min(args.batch, target-offset))
        if offset % (args.batch * 10) == 0:
            current = stats()
            print(json.dumps({'loaded': offset + min(args.batch, target-offset),
                              'elapsed_s': round(time.monotonic()-started, 1), 'parts': current}), flush=True)
            if shutil.disk_usage(args.data_dir).free < 2 * 1024**3:
                raise SystemExit('Free disk below 2 GiB; stopped with partial data for inspection')
            if int(current['bytes_on_disk']) > args.max_gib * 1024**3:
                raise SystemExit('Disk budget exceeded; stopped with partial data for inspection')
    actual_rows = int(sql('SELECT count() FROM bench.otel_logs'))
    if actual_rows != target:
        raise SystemExit(f'Historical row count mismatch: {actual_rows} != {target}; inspect for a previous interrupted insert')
    from live import insert_live
    live_rows = insert_live()
    # No FINAL/OPTIMIZE: normal background merges should settle by themselves.
    result = dict(stats(), requested_rows=requested, selected_rows=target, live_rows=live_rows, scale=args.scale,
                  sql_sha256=hashlib.sha256(query.encode()).hexdigest(),
                  days=args.days, end=end.isoformat(), start=(end-dt.timedelta(days=args.days)).isoformat(),
                  generation_seconds=round(time.monotonic()-started, 3),
                  free_disk_before=free, available_ram_before=available,
                  estimated_bytes_per_row=bytes_per_row, max_gib=args.max_gib)
    result['services'] = [json.loads(line) for line in sql("SELECT coalesce(nullIf(LogAttributes['journald.CONTAINER_NAME'],''), "
        "nullIf(LogAttributes['journald._SYSTEMD_UNIT'],''), ServiceName) AS service, count() AS rows "
        "FROM bench.otel_logs WHERE Timestamp >= fromUnixTimestamp64Nano({start_ns:Int64}) "
        "AND Timestamp < fromUnixTimestamp64Nano({end_ns:Int64}) GROUP BY service ORDER BY rows DESC FORMAT JSONEachRow",
        start_ns=int((end-dt.timedelta(days=args.days)).timestamp())*10**9,
        end_ns=int(end.timestamp())*10**9).splitlines()]
    result['total_generation_seconds'] = round(time.monotonic()-started, 3)
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(result), flush=True)


if __name__ == '__main__':
    main()
