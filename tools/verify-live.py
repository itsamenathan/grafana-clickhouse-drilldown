"""Read-only acceptance through Grafana; never prints log bodies or credentials.

--register creates only the separate non-default experimental data source.
Credentials stay in memory and are sent only to the current Grafana instance.
"""

import argparse
from decimal import Decimal
import json
import re
import sys
import time
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

sys.path.insert(0, '/mnt/code/dev/frcv/infa')
from openbao.access import OpenBaoClient

UID = 'clickhouse-loki-go'
SERVICE = "coalesce(nullIf(LogAttributes['journald.CONTAINER_NAME'], ''), nullIf(LogAttributes['journald._SYSTEMD_UNIT'], ''), ServiceName)"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--register', action='store_true')
    args = parser.parse_args()
    secret = OpenBaoClient().read_folder('observability')
    base = secret['GRAFANA_URL'].rstrip('/')
    if base != 'https://grafana.h.frcv.net':
        raise SystemExit('Review the Grafana address before sending credentials')
    headers = {'Authorization': 'Bearer ' + secret['GRAFANA_TOKEN'], 'Content-Type': 'application/json'}

    def api(path, payload=None):
        request = Request(base + path, headers=headers, data=None if payload is None else json.dumps(payload).encode())
        with urlopen(request, timeout=30) as response:
            return json.load(response, parse_float=Decimal)

    if args.register:
        wanted = {
            'uid': UID, 'name': 'ClickHouse Logs Drilldown (experimental)',
            'type': 'loki', 'access': 'proxy', 'url': 'http://clickhouse-loki-go:3100',
            'isDefault': False, 'jsonData': {'maxLines': 2000, 'timeout': 20},
        }
        try:
            existing = api('/api/datasources/uid/' + UID)
        except HTTPError as error:
            if error.code != 404:
                raise
            existing = None
        if existing is None:
            api('/api/datasources', wanted)
            print('Created only the separate non-default experimental data source')
        elif any(existing.get(k) != wanted[k] for k in ['type', 'url', 'isDefault']):
            raise SystemExit('Existing data source differs; refusing overwrite')

    end = (int(time.time()) - 120) * 10**9
    start = end - 300 * 10**9

    def sql(statement):
        result = api('/api/ds/query', {'queries': [{
            'refId': 'A', 'datasource': {'uid': 'clickstack', 'type': 'grafana-clickhouse-datasource'},
            'rawSql': statement, 'format': 1, 'queryType': 'table', 'editorType': 'sql',
        }]})['results']['A']
        if result.get('error'):
            raise RuntimeError('Direct SQL baseline failed')
        rows = []
        for frame in result.get('frames', []):
            keys = [field['name'] for field in frame['schema']['fields']]
            rows.extend(dict(zip(keys, row)) for row in zip(*frame['data']['values']))
        return rows

    def loki(path, **params):
        return api('/api/datasources/proxy/uid/' + UID + '/loki/api/v1/' + path + '?' + urlencode(params))

    time_filter = f'Timestamp >= fromUnixTimestamp64Nano({start}) AND Timestamp < fromUnixTimestamp64Nano({end})'
    baseline = sql(f'SELECT {SERVICE} AS service, count() AS n, sum(length(Body)) AS bytes FROM default.otel_logs WHERE {time_filter} GROUP BY service ORDER BY n DESC')
    baseline = [row for row in baseline if row['service']]
    if len(baseline) < 2:
        raise SystemExit('Need at least two active services in the settled fixture interval')
    # Filter/field checks need a complete positive control, not a 2,000-row
    # page of the busiest metadata-heavy stream. Overview totals above still
    # cover every service. Keep database result limits unchanged.
    candidates = [row for row in baseline if 5 <= int(row['n']) <= 200]
    if not candidates:
        raise SystemExit('Need a service with 5–200 records for complete filter controls')
    chosen = candidates[0]
    baseline.remove(chosen)
    baseline.insert(0, chosen)
    service = baseline[0]['service']
    selector = '{service_name=' + json.dumps(service) + '}'
    # Direct baseline data values use hex literals rather than SQL interpolation.
    service_value = "unhex('" + service.encode().hex() + "')"
    selected_filter = time_filter + f' AND {SERVICE} = {service_value}'
    tests = []

    def check(name, action):
        before = time.monotonic()
        try:
            passed, detail = action()
        except Exception as error:
            passed, detail = False, {'error_class': type(error).__name__}
        result = {'test': name, 'pass': bool(passed), 'detail': detail, 'seconds': round(time.monotonic() - before, 3)}
        tests.append(result)
        print(json.dumps(result))

    def logs(query, limit=2000):
        result = loki('query_range', query=query, start=str(start), end=str(end), limit=limit, direction='backward')
        streams = result.get('data', {}).get('result', [])
        return result, [(stream.get('stream', {}), value) for stream in streams for value in stream.get('values', [])]

    def volume():
        result = loki('index/volume', query='{service_name=~`.+`}', start=str(start), end=str(end), limit=5000)
        actual = {s['metric']['service_name']: int(s['value'][1]) for s in result['data']['result']}
        expected = {row['service']: int(row['bytes']) for row in baseline}
        return actual == expected, {'services': len(actual), 'exact_message_byte_volumes': actual == expected}

    def service_logs():
        result, rows = logs(selector)
        match = all(labels.get('service_name') == service and start <= int(value[0]) < end for labels, value in rows)
        return result.get('status') == 'success' and bool(rows) and match, {'rows': len(rows), 'correct_service_and_bounds': match}

    def backtick():
        _, impossible = logs('{service_name=`__clickhouse_drilldown_impossible_service__`}')
        # Real positive retrieval prevents an empty backend from passing this gate.
        _, positive = logs(selector)
        return not impossible and bool(positive), {'impossible_rows': len(impossible), 'positive_rows': len(positive)}

    def positive_text():
        _, rows = logs(selector + ' |= "__clickhouse_drilldown_absent_04a31c927e2d__"')
        return not rows, {'unexpected_rows': len(rows)}

    def known_term():
        _, rows = logs(selector)
        return next((word for _, value in rows for word in re.findall(r'[A-Za-z0-9_]{5,}', value[1])), None)

    term = known_term()
    if term is None:
        raise SystemExit('No usable synthetic-free text term for negative-filter verification')

    def negative_text():
        _, rows = logs(selector + ' != ' + json.dumps(term))
        expected = sql(f"SELECT count() AS n FROM default.otel_logs WHERE {selected_filter} AND position(Body, unhex('{term.encode().hex()}')) = 0")[0]['n']
        leaks = sum(term in value[1] for _, value in rows)
        return leaks == 0 and len(rows) == min(int(expected), 2000), {'expected_rows': int(expected), 'returned_rows': len(rows), 'leaks': leaks}

    def positive_regex():
        _, rows = logs(selector + ' |~ ' + json.dumps('(?i)' + re.escape(term)))
        expected = sql(f"SELECT count() AS n FROM default.otel_logs WHERE {selected_filter} AND match(Body, unhex('{('(?i)' + re.escape(term)).encode().hex()}'))")[0]['n']
        leaks = sum(re.search(re.escape(term), value[1], re.IGNORECASE) is None for _, value in rows)
        return expected > 0 and len(rows) == min(int(expected), 2000) and leaks == 0, {'expected_rows': int(expected), 'returned_rows': len(rows), 'leaks': leaks}

    def negative_selector_regex():
        query = '{service_name=~".+",service_name!~' + json.dumps(re.escape(service)) + '}'
        _, rows = logs(query, limit=100)
        return bool(rows) and all(labels.get('service_name') != service for labels, _ in rows), {'other_service_rows': len(rows), 'excluded_service_absent': all(labels.get('service_name') != service for labels, _ in rows)}

    def cardinality():
        result = loki('detected_labels', query='', start=str(start), end=str(end))
        actual = next((int(x['cardinality']) for x in result['detectedLabels'] if x['label'] == 'service_name'), 0)
        return actual == len(baseline), {'expected_services': len(baseline), 'actual_services': actual}

    def fields():
        result = loki('detected_fields', query=selector, start=str(start), end=str(end))
        visible = [field for field in result['fields'] if field.get('parsers') is None]
        return bool(visible) and any(f['label'] == 'detected_level' for f in visible), {'visible_without_parsers': len(visible)}

    def field_filter():
        _, unfiltered = logs(selector)
        if len(unfiltered) != int(baseline[0]['n']):
            return False, {'error': 'Need a complete positive control within the output budget'}
        key, value = next((
            (key, value) for labels, _ in unfiltered for key, value in labels.items()
            if key.startswith(('log_', 'resource_')) and isinstance(value, str) and 0 < len(value) <= 128
        ), (None, None))
        if key is None:
            return False, {'error': 'No structured field available for a positive control'}
        expected = sum(labels.get(key) == value for labels, _ in unfiltered)
        _, actual = logs(selector + ' | ' + key + '=' + json.dumps(value))
        _, absent = logs(selector + ' | ' + key + '="__drilldown_absent_field_1da36be93__"')
        matches = len(actual) == expected and all(labels.get(key) == value for labels, _ in actual)
        return matches and not absent, {'expected_rows': expected, 'actual_rows': len(actual), 'absent_value_rows': len(absent), 'exact_field_matches': matches}

    def windows():
        points = list(range(start, end + 1, 60 * 10**9))
        values = ','.join(str(point) for point in points)
        expected = sql(f'SELECT eval_ns, count() AS n FROM default.otel_logs ARRAY JOIN [{values}] AS eval_ns WHERE Timestamp > fromUnixTimestamp64Nano(toInt64(eval_ns)-60000000000) AND Timestamp <= fromUnixTimestamp64Nano(toInt64(eval_ns)) AND {SERVICE} = {service_value} GROUP BY eval_ns ORDER BY eval_ns')
        expected = {int(row['eval_ns']): int(row['n']) for row in expected}
        result = loki('query_range', query='sum(count_over_time(' + selector + '[1m]))', start=str(start), end=str(end), step='60')
        actual = {int(Decimal(p[0]) * 10**9): int(p[1]) for series in result['data']['result'] for p in series['values']}
        return actual == expected, {'evaluation_points': len(actual), 'exact_sliding_windows': actual == expected}

    def patterns():
        config = loki('drilldown-limits')
        try:
            loki('patterns', query=selector, start=str(start), end=str(end))
        except HTTPError as error:
            return config.get('pattern_ingester_enabled') is False and error.code == 501, {'advertised_disabled': config.get('pattern_ingester_enabled') is False, 'explicit_unsupported_status': error.code}
        return False, {'error': 'unsupported pattern discovery unexpectedly succeeded'}

    check('canonical service list and exact byte volumes', volume)
    check('log retrieval, service identity and nanosecond bounds', service_logs)
    check('impossible backtick selector plus nonempty positive control', backtick)
    check('positive absent-text filter', positive_text)
    check('negative known-text filter versus SQL', negative_text)
    check('real regex filtering versus SQL', positive_regex)
    check('negative service regex retains other services', negative_selector_regex)
    check('effective service cardinality', cardinality)
    check('structured fields visible with parsers disabled', fields)
    check('structured field filtering with positive and absent controls', field_filter)
    check('every metric evaluation window versus direct SQL', windows)
    check('optional patterns coherently disabled, never fake success', patterns)
    defaults = [d['uid'] for d in api('/api/datasources') if d.get('isDefault')]
    print(json.dumps({'passed': sum(t['pass'] for t in tests), 'total': len(tests), 'default_data_sources': defaults}))
    return 0 if all(t['pass'] for t in tests) and defaults == ['clickstack'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
