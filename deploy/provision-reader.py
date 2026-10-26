"""Run on observability via SSH stdin. Creates only this adapter's reader.

This is a live configuration change, not a read-only check. It refuses existing
runtime credentials/accounts, backs up the current XML privately, preserves
its bind-mounted inode, and reloads ClickHouse without restarting it.
"""

from datetime import datetime, timezone
import hashlib
import ipaddress
import json
import os
from pathlib import Path
import secrets
import shutil
import subprocess
import xml.etree.ElementTree as ET


def main():
    runtime = Path('/root/docker/clickhouse-loki-adapter')
    config_path = Path('/root/docker/clickstack/config/poc-users.xml')
    env_path = runtime / '.env'
    config = config_path.read_text()
    tree = ET.fromstring(config)
    name = 'clickhouse_drilldown'
    if env_path.exists() or tree.find('users/' + name) is not None or tree.find('profiles/' + name) is not None:
        raise SystemExit('Adapter credentials/profile already exist; refusing overwrite')
    if config.count('</profiles>') != 1 or config.count('</users>') != 1:
        raise SystemExit('Unexpected ClickHouse XML layout')
    network = json.loads(subprocess.check_output(['docker', 'network', 'inspect', 'clickstack_internal']))[0]
    subnets = [ipaddress.ip_network(c['Subnet']) for c in network['IPAM']['Config']]
    if len(subnets) != 1 or not subnets[0].is_private or subnets[0].version != 4:
        raise SystemExit('Review the internal network before provisioning')
    password = secrets.token_hex(32)
    profile = f'''<{name}>
      <readonly>1</readonly><max_threads>1</max_threads>
      <max_execution_time>15</max_execution_time>
      <max_memory_usage>134217728</max_memory_usage>
      <max_concurrent_queries_for_user>2</max_concurrent_queries_for_user>
      <max_result_rows>10000</max_result_rows><max_result_bytes>4194304</max_result_bytes>
      <result_overflow_mode>throw</result_overflow_mode>
      <max_rows_to_read>2000000</max_rows_to_read><read_overflow_mode>throw</read_overflow_mode>
    </{name}>'''
    user = f'''<{name}>
      <password_sha256_hex>{hashlib.sha256(password.encode()).hexdigest()}</password_sha256_hex>
      <profile>{name}</profile><quota>default</quota>
      <networks><ip>{subnets[0]}</ip></networks>
      <grants><query>GRANT SELECT ON default.otel_logs</query></grants>
    </{name}>'''
    updated = config.replace('</profiles>', profile + '\n  </profiles>').replace('</users>', user + '\n  </users>')
    ET.fromstring(updated)
    runtime.mkdir(mode=0o700, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    backup = config_path.with_name('poc-users.xml.before-clickhouse-drilldown-' + stamp)
    shutil.copy2(config_path, backup)
    backup.chmod(0o600)
    if config_path.read_text() != config:
        raise SystemExit('ClickHouse XML changed during provisioning; no write made')
    # Write the existing inode: replacing this file would leave Docker reading
    # the previous bind-mounted inode until an unrelated service recreation.
    with config_path.open('w') as stream:
        stream.write(updated)
        stream.flush()
        os.fsync(stream.fileno())
    reloaded = subprocess.run(['docker', 'exec', '-i', 'clickstack-ch-server-1', 'clickhouse-client', '--multiquery'],
                              input='SYSTEM RELOAD CONFIG;', text=True, capture_output=True)
    if reloaded.returncode:
        raise SystemExit('ClickHouse reload failed; inspect private backup and remove only new entries if necessary')
    fd = os.open(env_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, 'w') as stream:
        stream.write('CLICKHOUSE_PASSWORD=' + password + '\n')
        stream.write('SOURCE_OFFER_URL=https://grafana.h.frcv.net/api/datasources/proxy/uid/clickhouse-loki-go/source\n')
    print('Created SELECT-only clickhouse_drilldown reader; private runtime .env written')
    print('Current-configuration rollback backup:', backup)


if __name__ == '__main__':
    main()
