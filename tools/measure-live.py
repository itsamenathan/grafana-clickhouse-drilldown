"""Run browser acceptance while sampling only adapter/ClickHouse cgroup memory.

Requires the separately deployed source. No log bodies or credentials are
printed. Memory maxima are sampled, not attributed exclusively to this load.
"""

import json
import os
import subprocess
import sys
import threading
import time

sys.path.insert(0, '/mnt/code/dev/frcv/infa')
from openbao.access import OpenBaoClient

SAMPLE = r'''
import json, subprocess
from pathlib import Path
out = {}
for name in ['clickhouse-loki-adapter-adapter-1', 'clickstack-ch-server-1']:
    pid = subprocess.check_output(['docker', 'inspect', '--format', '{{.State.Pid}}', name], text=True).strip()
    relative = next(line.split(':', 2)[2] for line in Path('/proc/' + pid + '/cgroup').read_text().splitlines() if line.startswith('0::'))
    group = Path('/sys/fs/cgroup') / relative.lstrip('/')
    out[name] = {key: int((group / key).read_text()) for key in ['memory.current', 'memory.peak']}
print(json.dumps(out))
'''


def main():
    secret = OpenBaoClient().read_folder('observability')
    base = secret['GRAFANA_URL'].rstrip('/')
    if base != 'https://grafana.h.frcv.net':
        raise SystemExit('Unexpected Grafana origin')
    stopped = threading.Event()
    samples, errors = [], []

    def sample():
        while not stopped.is_set():
            try:
                result = subprocess.run(['ssh', '-o', 'BatchMode=yes', 'root@observability.n.vpn', 'python3', '-'], input=SAMPLE, capture_output=True, text=True, timeout=10, check=True)
                samples.append(json.loads(result.stdout))
            except Exception as error:
                errors.append(type(error).__name__)
            stopped.wait(1)

    thread = threading.Thread(target=sample, daemon=True)
    thread.start()
    start = time.monotonic()
    try:
        result = subprocess.run(['node', 'tools/browser-check.cjs'], input=json.dumps({
            'url': base + '/a/grafana-lokiexplore-app/explore?var-ds=clickhouse-loki-go&from=now-15m&to=now',
            'token': secret['GRAFANA_TOKEN'], 'overviewServices': 20, 'probeContext': True,
        }), capture_output=True, text=True, timeout=240, env=os.environ)
    finally:
        stopped.set()
        thread.join(timeout=12)
    maxima = {}
    for sample in samples:
        for name, values in sample.items():
            maxima.setdefault(name, {'sampled_max_bytes': 0, 'container_lifetime_peak_bytes': 0})
            maxima[name]['sampled_max_bytes'] = max(maxima[name]['sampled_max_bytes'], values['memory.current'])
            maxima[name]['container_lifetime_peak_bytes'] = max(maxima[name]['container_lifetime_peak_bytes'], values['memory.peak'])
    print(json.dumps({'elapsed_seconds': round(time.monotonic() - start, 3), 'memory_samples': len(samples), 'sampling_errors': errors, 'memory': maxima}))
    # browser-check's normal output contains counts and status only. Do not
    # enable its fixture-only diagnostics for production telemetry.
    print(result.stdout)
    if result.returncode:
        print('Browser load acceptance failed', file=sys.stderr)
        print(json.dumps({'browser_error': result.stderr.splitlines()[0] if result.stderr else 'no diagnostic'}))
    return result.returncode or (1 if errors or not samples else 0)


if __name__ == '__main__':
    raise SystemExit(main())
