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
-- Repeating templates for pattern detection, plus rare noise below Loki's
-- 30-sample pruning threshold. One line per second from the base time.
CREATE TABLE fixture.otel_logs_patterns AS fixture.otel_logs;
INSERT INTO fixture.otel_logs_patterns
SELECT fromUnixTimestamp64Nano(1767225600000000000 + toInt64(number) * 1000000000),
       multiIf(number % 3 = 0, concat('order ', toString(number * 17), ' shipped to warehouse ', toString(number % 7)),
               number % 3 = 1, concat('payment ', toString(number * 31), ' declined for card ', toString(number * 5)),
               number < 80, concat('cache ', toString(number), ' rebuilt in ', toString(number * 2), ' ms'),
               concat('rare noise ', toString(number), ' seen once today')),
       'fixture-patterns', if(number % 3 = 1, 'error', 'info'), if(number % 3 = 1, 17, 9),
       map('order', toString(number)), map(), '', ''
FROM numbers(120);
GRANT SELECT ON fixture.otel_logs_patterns TO fixture_reader;
-- More rows at one nanosecond than one page may hold: paging cannot split
-- them, so scans must fail explicitly rather than return a partial page.
CREATE TABLE fixture.otel_logs_tie AS fixture.otel_logs;
INSERT INTO fixture.otel_logs_tie
SELECT fromUnixTimestamp64Nano(1767225600000000000), concat('tie ', toString(number)), 'fixture-tie', 'info', 9, map(), map(), '', ''
FROM numbers(4001);
GRANT SELECT ON fixture.otel_logs_tie TO fixture_reader;
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

-- Parser guard cases and independently sized/unsafe reader profiles.
CREATE TABLE fixture.otel_logs_parser_guards AS fixture.otel_logs;
INSERT INTO fixture.otel_logs_parser_guards VALUES ('2026-01-01 00:00:00.000000001', '{"n":12,"service.name":"other","detected.level":"other","log_x":"collision"}', 'fixture-guard-safe', 'info', 9, {'x':'metadata'}, {}, '', ''),
('2026-01-01 00:00:00.000000002', 'n=12 service_name=other detected_level=other log_x=collision', 'fixture-guard-safe', 'info', 9, {'x':'metadata'}, {}, '', ''),
('2026-01-01 00:00:00.000000003', 'not json', 'fixture-guard-safe', 'info', 9, {'x':'metadata'}, {}, '', ''),
('2026-01-01 00:00:00.000000004', 'n="unterminated', 'fixture-guard-safe', 'info', 9, {'x':'metadata'}, {}, '', ''),
('2026-01-01 00:00:00.000000005', '{"__error__":"body"}', 'fixture-guard-reserved', 'info', 9, {'x':'metadata'}, {}, '', ''),
('2026-01-01 00:00:00.000000006', '!!error!!=body', 'fixture-guard-sanitized', 'info', 9, {'x':'metadata'}, {}, '', ''),
('2026-01-01 00:00:00.000000007', '{"_":{"error":{"_":"body"}}}', 'fixture-guard-nested', 'info', 9, {'x':'metadata'}, {}, '', ''),
('2026-01-01 00:00:00.000000008', '{"\\u005f_error__":"body"}', 'fixture-guard-escaped', 'info', 9, {'x':'metadata'}, {}, '', ''),
('2026-01-01 00:00:00.000000009', 'service_name=from_body', '', 'trace', 1, {'x':'metadata'}, {}, '', '');
INSERT INTO fixture.otel_logs_parser_guards VALUES
('2026-01-01 00:00:00.000000010', '{"path":"\\/api\\/v1","e\\/rror":"safe"}', 'fixture-guard-safe', 'info', 9, {}, {}, '', '');
INSERT INTO fixture.otel_logs_parser_guards VALUES
('2026-01-01 00:00:00.000000011', '{"__\\u0065rror__":"body"}', 'fixture-guard-escaped', 'info', 9, {}, {}, '', '');
GRANT SELECT ON fixture.otel_logs_parser_guards TO fixture_reader;
CREATE USER fixture_small_reader IDENTIFIED WITH sha256_password BY 'fixture-reader-not-production';
GRANT SELECT ON fixture.otel_logs_large TO fixture_small_reader;
ALTER USER fixture_small_reader SETTINGS readonly=1, max_threads=2, max_result_rows=100, max_result_bytes=1048576, result_overflow_mode='throw', read_overflow_mode='throw';
CREATE USER fixture_unsafe_reader IDENTIFIED WITH sha256_password BY 'fixture-reader-not-production';
ALTER USER fixture_unsafe_reader SETTINGS readonly=1, result_overflow_mode='break';
