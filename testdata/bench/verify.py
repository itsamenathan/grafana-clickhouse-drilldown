#!/usr/bin/env python3
# SPDX-License-Identifier: AGPL-3.0-only
"""Validate the local synthetic load and exact reader settings; save aggregates only."""
import json
import pathlib
import urllib.request

from generate import sql, stats

HERE = pathlib.Path(__file__).resolve().parent
EXPECTED = {
    'readonly': '1', 'max_threads': '1', 'max_concurrent_queries_for_user': '2',
    'max_memory_usage': '134217728', 'max_execution_time': '15',
    'max_result_rows': '10000', 'max_result_bytes': '4194304',
    'result_overflow_mode': 'throw', 'max_rows_to_read': '2000000',
    'read_overflow_mode': 'throw',
}
SERVICE = "coalesce(nullIf(LogAttributes['journald.CONTAINER_NAME'],''), nullIf(LogAttributes['journald._SYSTEMD_UNIT'],''), ServiceName)"


def reader_settings(user):
    req = urllib.request.Request('http://127.0.0.1:28123/', data=(
        'SELECT name,value FROM system.settings WHERE name IN (' +
        ','.join("'" + key + "'" for key in EXPECTED) + ') FORMAT JSONEachRow').encode(),
        headers={'X-ClickHouse-User': user, 'X-ClickHouse-Key': 'bench-reader-not-production'})
    with urllib.request.urlopen(req, timeout=30) as response:
        return {row['name']: row['value'] for row in map(json.loads, response.read().decode().splitlines())}


def main():
    manifest = json.loads((HERE / 'results/dataset.json').read_text())
    strict = reader_settings('bench_reader')
    wide = reader_settings('bench_reader_wide')
    assert strict == EXPECTED, strict
    assert wide == dict(EXPECTED, max_rows_to_read='200000000'), wide
    selected = int(manifest['selected_rows'])
    end = manifest['end']
    start = manifest['start']
    # Synthetic, generated timestamps only; no external SQL inputs or production data.
    where = f"Timestamp >= parseDateTime64BestEffort('{start}',9) AND Timestamp < parseDateTime64BestEffort('{end}',9)"
    bounds = json.loads(sql('SELECT count() AS rows, toString(min(Timestamp)) AS min_timestamp, '
        'toString(max(Timestamp)) AS max_timestamp '
        'FROM bench.otel_logs WHERE ' + where + ' FORMAT JSONEachRow'))
    assert int(bounds['rows']) == selected, bounds
    days = [json.loads(row) for row in sql('SELECT toString(toDate(Timestamp)) AS day, count() AS rows '
        'FROM bench.otel_logs WHERE ' + where + ' GROUP BY day ORDER BY day FORMAT JSONEachRow').splitlines()]
    assert len(days) >= int(manifest['days']), days
    services = manifest['services']
    assert len(services) == 20 and sum(int(s['rows']) for s in services) == selected, services
    sample_where = where + " AND Timestamp >= parseDateTime64BestEffort('" + end + "',9) - INTERVAL 15 MINUTE"
    rates = [json.loads(row) for row in sql('SELECT ' + SERVICE + ' AS service, count() AS rows, '
        'count() / 900. AS last_15m_lines_per_second FROM bench.otel_logs WHERE ' + sample_where +
        ' GROUP BY service ORDER BY rows DESC FORMAT JSONEachRow').splitlines()]
    sample_rows = sum(int(row['rows']) for row in rates)
    shape = json.loads(sql("SELECT min(length(LogAttributes)) AS min_log_attributes, max(length(LogAttributes)) AS max_log_attributes, "
        "min(length(ResourceAttributes)) AS min_resource_attributes, max(length(ResourceAttributes)) AS max_resource_attributes, "
        "countIf(startsWith(Body,'{')) AS json_rows, "
        "countIf(startsWith(Body,'msg=')) AS logfmt_rows, countIf(startsWith(Body,'request ')) AS plain_rows, "
        "countIf(TraceId != '') AS traced_rows, countIf(LogAttributes['journald.CONTAINER_NAME'] != '') AS container_fallback_rows, "
        "countIf(LogAttributes['journald._SYSTEMD_UNIT'] != '') AS systemd_fallback_rows "
        "FROM bench.otel_logs WHERE " + sample_where + ' FORMAT JSONEachRow'))
    assert int(shape['min_log_attributes']) == int(shape['max_log_attributes']) == 18, shape
    assert int(shape['min_resource_attributes']) == int(shape['max_resource_attributes']) == 12, shape
    assert sum(int(shape[k]) for k in ['json_rows', 'logfmt_rows', 'plain_rows']) == sample_rows, shape
    for key in ['json_rows', 'logfmt_rows', 'plain_rows']:
        assert 0.30 < int(shape[key]) / sample_rows < 0.37, shape
    levels = [json.loads(row) for row in sql('SELECT SeverityText AS level, count() AS rows FROM bench.otel_logs WHERE ' + where +
        ' GROUP BY level ORDER BY rows DESC FORMAT JSONEachRow').splitlines()]
    schema = json.loads(sql("SELECT sorting_key, partition_key, engine FROM system.tables WHERE database='bench' AND name='otel_logs' FORMAT JSONEachRow"))
    assert schema['sorting_key'] == 'toStartOfFiveMinutes(Timestamp), ServiceName, Timestamp', schema
    assert schema['partition_key'] == 'toDate(Timestamp)', schema
    result = dict(reader_settings={'bench_reader': strict, 'bench_reader_wide': wide},
                  bounds=bounds, days=days, services=services, last_15m_rates=rates,
                  shape_sample_rows=sample_rows, shape_sample_minutes=15, shape=shape, levels=levels,
                  table=schema, size_after_merges=stats())
    (HERE / 'results/validation.json').write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(result, indent=2))


if __name__ == '__main__':
    main()
