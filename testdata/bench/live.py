#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-only
"""Append a small batch of current synthetic logs for local websocket tail checks."""
import argparse
import json
import pathlib
import time

from generate import sql


def insert_live(rows=64):
    # No production inputs. Reuse exactly the same maps/body/IDs as the main load.
    query = (pathlib.Path(__file__).parent / 'generate.sql').read_text()
    before, found, after = query.partition('    if(n < burst,')
    _, found_end, rest = after.partition(' AS ts,')
    if not found or not found_end:
        raise RuntimeError('generate.sql timestamp expression changed; update live.py')
    query = before + '    {end_ns:Int64} - toInt64(({offset:UInt64} + {count:UInt64} - n) * 1000000) AS ts,' + rest
    offset = int(sql('SELECT count() FROM bench.otel_logs'))
    sql(query, offset=offset, count=rows, total=offset+rows, scale=1, days=7, end_ns=time.time_ns())
    return rows


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--rows', type=int, default=64)
    args = parser.parse_args()
    if not 1 <= args.rows <= 2000:
        parser.error('rows must be 1..2000')
    print(json.dumps({'live_rows_inserted': insert_live(args.rows)}))
