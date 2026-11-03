# Service identity primary-key evidence

Local 100M-row benchmark, ClickHouse 26.9.20.23. One-hour range
2026-10-27 14:20:00Z to 15:20:00Z. Predicates use the shared compiler
forms and typed, hex-encoded String parameters.

## default equality

```sql
EXPLAIN indexes=1 SELECT count() FROM bench.otel_logs WHERE Timestamp >= fromUnixTimestamp64Nano({start_ns:Int64}) AND Timestamp < fromUnixTimestamp64Nano({end_ns:Int64}) AND (ServiceName = unhex({filter_0:String}))
```

```text
Output: count()

Aggregating
│  Keys:
│  Aggregates: count()
│  Skip merging: 0
└──ReadFromMergeTree (bench.otel_logs)
      Read type: Default
      Parts: 6 | Granules: 21
      Output: Empty
      Prewhere filter
      Prewhere filter column:  Timestamp < \'2026-10-27 15:20:00.000000000\' AND Timestamp >= \'2026-10-27 14:20:00.000000000\' AND ServiceName = \'archive-sweeper\'
      Indexes:
        Min-Max
          Keys:
            Timestamp
          Condition: and((Timestamp in (-Inf, \'1793114400\')), (Timestamp in [\'1793110800\', +Inf)))
          Parts: 6/66
          Granules: 2255/18778
        Partition
          Keys:
            toDate(Timestamp)
          Condition: and((toDate(Timestamp) in (-Inf, 20753]), (toDate(Timestamp) in [20753, +Inf)))
          Parts: 6/6
          Granules: 2255/2255
        PrimaryKey
          Keys:
            ServiceName
            Timestamp
          Condition: and((ServiceName in [\'archive-sweeper\', \'archive-sweeper\']), and((Timestamp in (-Inf, \'1793114400\')), (Timestamp in [\'1793110800\', +Inf))))
          Parts: 6/6
          Granules: 21/2255
          Search Algorithm: generic exclusion search
        Ranges: 17
```

## default finite regex

```sql
EXPLAIN indexes=1 SELECT count() FROM bench.otel_logs WHERE Timestamp >= fromUnixTimestamp64Nano({start_ns:Int64}) AND Timestamp < fromUnixTimestamp64Nano({end_ns:Int64}) AND (match(ServiceName,unhex({filter_0:String}))) AND (ServiceName IN (unhex({filter_1:String})))
```

```text
Output: count()

Aggregating
│  Keys:
│  Aggregates: count()
│  Skip merging: 0
└──ReadFromMergeTree (bench.otel_logs)
      Read type: Default
      Parts: 6 | Granules: 21
      Output: Empty
      Prewhere filter
      Prewhere filter column:  Timestamp < \'2026-10-27 15:20:00.000000000\' AND Timestamp >= \'2026-10-27 14:20:00.000000000\' AND ServiceName IN (\'archive-sweeper\') AND match(ServiceName, \'(?s:\\\\A(?:archive-sweeper)\\\\z)\')
      Indexes:
        Min-Max
          Keys:
            Timestamp
          Condition: and((Timestamp in (-Inf, \'1793114400\')), (Timestamp in [\'1793110800\', +Inf)))
          Parts: 6/66
          Granules: 2255/18778
        Partition
          Keys:
            toDate(Timestamp)
          Condition: and((toDate(Timestamp) in (-Inf, 20753]), (toDate(Timestamp) in [20753, +Inf)))
          Parts: 6/6
          Granules: 2255/2255
        PrimaryKey
          Keys:
            ServiceName
            Timestamp
          Condition: and((ServiceName in 1-element set), and((Timestamp in (-Inf, \'1793114400\')), (Timestamp in [\'1793110800\', +Inf))))
          Parts: 6/6
          Granules: 21/2255
          Search Algorithm: generic exclusion search
        Ranges: 17
```

## default prefix regex

```sql
EXPLAIN indexes=1 SELECT count() FROM bench.otel_logs WHERE Timestamp >= fromUnixTimestamp64Nano({start_ns:Int64}) AND Timestamp < fromUnixTimestamp64Nano({end_ns:Int64}) AND (match(ServiceName,unhex({filter_0:String}))) AND startsWith(ServiceName,unhex({filter_1:String}))
```

```text
Output: count()

Aggregating
│  Keys:
│  Aggregates: count()
│  Skip merging: 0
└──ReadFromMergeTree (bench.otel_logs)
      Read type: Default
      Parts: 6 | Granules: 21
      Output: Empty
      Prewhere filter
      Prewhere filter column:  Timestamp < \'2026-10-27 15:20:00.000000000\' AND Timestamp >= \'2026-10-27 14:20:00.000000000\' AND startsWith(ServiceName, \'archive-\') AND match(ServiceName, \'(?s:\\\\A(?:archive-.*)\\\\z)\')
      Indexes:
        Min-Max
          Keys:
            Timestamp
          Condition: and((Timestamp in (-Inf, \'1793114400\')), (Timestamp in [\'1793110800\', +Inf)))
          Parts: 6/66
          Granules: 2255/18778
        Partition
          Keys:
            toDate(Timestamp)
          Condition: and((toDate(Timestamp) in (-Inf, 20753]), (toDate(Timestamp) in [20753, +Inf)))
          Parts: 6/6
          Granules: 2255/2255
        PrimaryKey
          Keys:
            ServiceName
            Timestamp
          Condition: and((ServiceName in [\'archive-\', \'archive.\')), and((Timestamp in (-Inf, \'1793114400\')), (Timestamp in [\'1793110800\', +Inf))))
          Parts: 6/6
          Granules: 21/2255
          Search Algorithm: generic exclusion search
        Ranges: 17
```

## journald equality

```sql
EXPLAIN indexes=1 SELECT count() FROM bench.otel_logs WHERE Timestamp >= fromUnixTimestamp64Nano({start_ns:Int64}) AND Timestamp < fromUnixTimestamp64Nano({end_ns:Int64}) AND (coalesce(nullIf(LogAttributes['journald.CONTAINER_NAME'], ''), nullIf(LogAttributes['journald._SYSTEMD_UNIT'], ''), ServiceName) = unhex({filter_0:String}))
```

```text
Output: count()

Aggregating
│  Keys:
│  Aggregates: count()
│  Skip merging: 0
└──ReadFromMergeTree (bench.otel_logs)
      Read type: Default
      Parts: 6 | Granules: 2255
      Output: Empty
      Prewhere filter
      Prewhere filter column:  Timestamp < \'2026-10-27 15:20:00.000000000\' AND Timestamp >= \'2026-10-27 14:20:00.000000000\' AND coalesce(nullIf(LogAttributes.key_journald.CONTAINER_NAME, \'\'), nullIf(LogAttributes.key_journald._SYSTEMD_UNIT, \'\'), ServiceName) = \'archive-sweeper\'
      Indexes:
        Min-Max
          Keys:
            Timestamp
          Condition: and((Timestamp in (-Inf, \'1793114400\')), (Timestamp in [\'1793110800\', +Inf)))
          Parts: 6/66
          Granules: 2255/18778
        Partition
          Keys:
            toDate(Timestamp)
          Condition: and((toDate(Timestamp) in (-Inf, 20753]), (toDate(Timestamp) in [20753, +Inf)))
          Parts: 6/6
          Granules: 2255/2255
        PrimaryKey
          Keys:
            Timestamp
          Condition: and((Timestamp in (-Inf, \'1793114400\')), (Timestamp in [\'1793110800\', +Inf)))
          Parts: 6/6
          Granules: 2255/2255
          Search Algorithm: generic exclusion search
        Ranges: 6
```
