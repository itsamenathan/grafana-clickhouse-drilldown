-- Public synthetic fixture credentials. Never use these for production.
CREATE DATABASE fixture;
CREATE TABLE fixture.otel_logs (
    Timestamp DateTime64(9, 'UTC'),
    Body String,
    ServiceName String,
    SeverityText String,
    SeverityNumber UInt8,
    LogAttributes Map(String, String),
    ResourceAttributes Map(String, String),
    TraceId String,
    SpanId String
) ENGINE = MergeTree ORDER BY (Timestamp, ServiceName);

INSERT INTO fixture.otel_logs VALUES
('2026-01-01 00:00:00.000000001', 'hello api', 'raw-service', 'INFO', 9, {'journald.CONTAINER_NAME':'fixture-api','journald._SYSTEMD_UNIT':'fixture-unit'}, {}, '', ''),
('2026-01-01 00:00:00.000000002', 'hello worker', 'fixture-worker', 'debug', 5, {}, {}, '', ''),
('2026-01-01 00:00:00.000000003', 'ERROR café 雪', 'fixture-api', 'Error', 17, {}, {}, '', ''),
('2026-01-01 00:00:00.000000004', 'health check', 'fixture-api', 'warning', 13, {}, {}, '', ''),
('2026-01-01 00:00:00.000000005', 'a\nb', 'fixture-api', '', 9, {}, {}, '', ''),
('2026-01-01 00:00:00.000000006', '{"nested":{"key":"value"},"duration":12,"a.b":1,"a_b":2}', 'fixture-api', 'critical', 21, {'a.b':'dot','a_b':'underscore'}, {'host.name':'fixture-host'}, '0123456789abcdef0123456789abcdef', '0123456789abcdef'),
('2026-01-01 00:00:00.000000007', 'duration=12 message="quoted value"', 'fixture-api', 'unrecognized', 0, {'duration':'12'}, {}, '', ''),
('2026-01-01 00:00:00.000000008', 'quote " backtick ` slash \\', 'fixture-api', 'trace', 1, {}, {}, '', ''),
('2026-01-01 00:00:00.000000009', '', '', '', 0, {}, {}, '', ''),
('2026-01-01 00:00:00.000000010', 'systemd fallback', 'raw-service', 'INFO', 9, {'journald.CONTAINER_NAME':'','journald._SYSTEMD_UNIT':'fixture-unit'}, {}, '', ''),
('2026-01-01 00:00:00.000000011', 'duplicate', 'fixture-api', 'info', 9, {}, {}, '', ''),
('2026-01-01 00:00:00.000000011', 'duplicate', 'fixture-api', 'info', 9, {}, {}, '', ''),
('2025-12-31 23:59:59.999999999', 'out of order boundary', 'fixture-api', 'fatal', 21, {}, {}, '', '');

CREATE USER fixture_reader IDENTIFIED WITH sha256_password BY 'fixture-reader-not-production';
GRANT SELECT ON fixture.otel_logs TO fixture_reader;
-- Loki deduplicates identical events. Use this common reference view for
-- differential equality, and test preservation of base-table duplicates
-- separately rather than hiding the intentional policy difference.
CREATE VIEW fixture.otel_logs_unique AS SELECT DISTINCT * FROM fixture.otel_logs;
GRANT SELECT ON fixture.otel_logs_unique TO fixture_reader;
-- Large synthetic result proves LIMIT is applied in SQL only when safe.
CREATE TABLE fixture.otel_logs_large AS fixture.otel_logs;
INSERT INTO fixture.otel_logs_large
SELECT fromUnixTimestamp64Nano(1767225600000000000 + toInt64(number)),
       repeat('x', 6000), 'fixture-bulk', 'info', 9, map(), map(), '', ''
FROM numbers(1000);
GRANT SELECT ON fixture.otel_logs_large TO fixture_reader;
ALTER USER fixture_reader SETTINGS readonly = 1, max_threads = 1,
    max_concurrent_queries_for_user = 2,
    max_memory_usage = 134217728, max_execution_time = 15,
    max_result_rows = 10000, max_result_bytes = 4194304,
    result_overflow_mode = 'throw', max_rows_to_read = 2000000,
    read_overflow_mode = 'throw';

-- Container health must not consume the adapter reader's two query slots.
CREATE USER fixture_probe IDENTIFIED WITH sha256_password BY 'fixture-probe-not-production';
GRANT SELECT ON fixture.otel_logs TO fixture_probe;
ALTER USER fixture_probe SETTINGS readonly = 1, max_threads = 1,
    max_memory_usage = 16777216, max_execution_time = 3;
