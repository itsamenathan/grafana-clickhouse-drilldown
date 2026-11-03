-- Public local benchmark passwords. Synthetic data only.
CREATE DATABASE bench;
CREATE TABLE bench.otel_logs
(
    Timestamp DateTime64(9) CODEC(Delta(8), ZSTD(1)),
    TraceId String CODEC(ZSTD(1)),
    SpanId String CODEC(ZSTD(1)),
    TraceFlags UInt8,
    SeverityText LowCardinality(String) CODEC(ZSTD(1)),
    SeverityNumber UInt8,
    ServiceName LowCardinality(String) CODEC(ZSTD(1)),
    Body String CODEC(ZSTD(1)),
    ResourceSchemaUrl String CODEC(ZSTD(1)),
    ResourceAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    ScopeSchemaUrl String CODEC(ZSTD(1)),
    ScopeName String CODEC(ZSTD(1)),
    ScopeVersion String CODEC(ZSTD(1)),
    ScopeAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    LogAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    INDEX idx_trace_id TraceId TYPE bloom_filter(0.001) GRANULARITY 1,
    INDEX idx_res_attr_key mapKeys(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_res_attr_value mapValues(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_log_attr_key mapKeys(LogAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_log_attr_value mapValues(LogAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_body Body TYPE tokenbf_v1(32768, 3, 0) GRANULARITY 1
)
ENGINE = MergeTree
PARTITION BY toDate(Timestamp)
ORDER BY (toStartOfFiveMinutes(Timestamp), ServiceName, Timestamp)
SETTINGS index_granularity = 8192, old_parts_lifetime = 30,
    cleanup_delay_period = 5, cleanup_delay_period_random_add = 0;

CREATE USER bench_reader IDENTIFIED WITH sha256_password BY 'bench-reader-not-production';
CREATE USER bench_reader_wide IDENTIFIED WITH sha256_password BY 'bench-reader-not-production';
GRANT SELECT ON bench.otel_logs TO bench_reader;
GRANT SELECT ON bench.otel_logs TO bench_reader_wide;
ALTER USER bench_reader SETTINGS readonly = 1, max_threads = 1,
    max_concurrent_queries_for_user = 2, max_memory_usage = 134217728,
    max_execution_time = 15, max_result_rows = 10000,
    max_result_bytes = 4194304, result_overflow_mode = 'throw',
    max_rows_to_read = 2000000, read_overflow_mode = 'throw';
-- Exactly one setting differs from the production-equivalent reader.
ALTER USER bench_reader_wide SETTINGS readonly = 1, max_threads = 1,
    max_concurrent_queries_for_user = 2, max_memory_usage = 134217728,
    max_execution_time = 15, max_result_rows = 10000,
    max_result_bytes = 4194304, result_overflow_mode = 'throw',
    max_rows_to_read = 200000000, read_overflow_mode = 'throw';
