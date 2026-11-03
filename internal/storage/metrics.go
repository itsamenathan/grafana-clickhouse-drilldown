// SPDX-License-Identifier: AGPL-3.0-only
package storage

import (
	"context"
	"errors"
	"strconv"
	"unicode/utf8"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"grafana-clickhouse-drilldown/internal/chcontext"
	"grafana-clickhouse-drilldown/internal/record"
	"grafana-clickhouse-drilldown/internal/sqlgen"
)

// MetricBuckets transfers counts and byte totals, never messages or maps.
// The caller proves the residual pipeline and final labels are SQL-complete.
// Intervals are (start,end], with start/end sharing one evaluation phase. Adaptive splits
// keep every response below the restricted reader's result budget.
func (c *Client) MetricBuckets(ctx context.Context, predicate sqlgen.Predicate, start, end, step int64, groups []string) ([]record.MetricBucket, error) {
	if !c.validRange(start, end) || step <= 0 || (end-start)%step != 0 {
		return nil, errors.New("invalid metric bucket range")
	}
	predicate = c.service.Bind(predicate)
	service, level := "''", "''"
	for _, group := range groups {
		expression, err := sqlgen.LabelExpression(group)
		if err != nil {
			return nil, err
		}
		if group == "service_name" {
			service = c.service.Expression()
		} else {
			level = expression
		}
	}
	result := []record.MetricBucket{}
	size := 0
	var read func(window) error
	read = func(span window) error {
		if span.start == span.end {
			return nil
		}
		rows, err := c.metricPage(ctx, predicate, span, step, service, level)
		if errors.Is(err, errResultLimit) {
			count := (span.end - span.start) / step
			if count < 2 {
				return ErrScanLimit
			}
			middle := span.start + (count/2)*step
			if err := read(window{span.start, middle}); err != nil {
				return err
			}
			return read(window{middle, span.end})
		}
		if err != nil {
			return err
		}
		if len(rows) > c.limits.MaxMetricBuckets-len(result) {
			return ErrScanLimit
		}
		for _, row := range rows {
			size += len(row.Service) + len(row.Level) + 64
			if size > c.limits.MaxPageDecodedBytes {
				return ErrScanLimit
			}
		}
		result = append(result, rows...)
		return nil
	}
	if err := read(window{start, end}); err != nil {
		return nil, err
	}
	return result, nil
}

func (c *Client) metricPage(ctx context.Context, predicate sqlgen.Predicate, span window, step int64, service, level string) ([]record.MetricBucket, error) {
	release, err := c.gate.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	parameters := scanParameters(predicate, span)
	parameters["step_ns"] = strconv.FormatInt(step, 10)
	guard := "toUInt64(0)"
	if predicate.ParserGuard {
		// sanitizeLabelKey/appendSanitized preserve ASCII letters, so producing
		// __error__ requires literal "error", unless an escape decoded it. Guard
		// every backslash except exact escaped slashes, which cannot produce
		// ASCII letters, including all Unicode/hex/octal possibilities. Empty
		// services are guarded because parsers can create a missing service_name.
		parameters["parser_error"] = "6572726f72"
		parameters["parser_escape"] = "5c"
		parameters["parser_slash"] = "5c2f"
		guard = "countIf(position(Body, unhex({parser_error:String})) > 0 OR position(replaceAll(Body, unhex({parser_slash:String}), '/'), unhex({parser_escape:String})) > 0 OR " + c.service.Expression() + " = '')"
	}
	byteTotal := "sum(length(Body))"
	if predicate.CountOnly {
		byteTotal = "toUInt64(0)"
	}
	// Subtract one nanosecond before division so boundary rows belong to the
	// bucket ending at that boundary, rather than the following bucket.
	statement := "SELECT intDiv(toUnixTimestamp64Nano(Timestamp) - {start_ns:Int64} - 1, {step_ns:Int64}) AS bucket, " + service + " AS service_name, " + level + " AS detected_level, count(), " + byteTotal + ", " + guard + " FROM " + c.table +
		" WHERE Timestamp > fromUnixTimestamp64Nano({start_ns:Int64}) AND Timestamp <= fromUnixTimestamp64Nano({end_ns:Int64}) AND (" + predicate.SQL + ") GROUP BY bucket, service_name, detected_level ORDER BY bucket, service_name, detected_level LIMIT {bucket_limit:UInt64}"
	parameters["bucket_limit"] = strconv.Itoa(c.pageRows + 1)
	queryCtx := clickhouse.Context(chcontext.ReadOnly(ctx), clickhouse.WithParameters(parameters))
	rows, err := retryBusy(queryCtx, func() (driver.Rows, error) { return c.conn.Query(queryCtx, statement) })
	if err != nil {
		return nil, pageError(ctx, err)
	}
	defer rows.Close()
	result := []record.MetricBucket{}
	size := 0
	for rows.Next() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		var index int64
		var b record.MetricBucket
		if err := rows.Scan(&index, &b.Service, &b.Level, &b.Entries, &b.Bytes, &b.ParserGuard); err != nil {
			return nil, errors.New("ClickHouse metric bucket schema mismatch")
		}
		if index < 0 || index >= (span.end-span.start)/step || b.Entries == 0 {
			return nil, errors.New("ClickHouse metric buckets are inconsistent")
		}
		b.Timestamp = span.start + (index+1)*step
		size += len(b.Service) + len(b.Level) + 64
		if len(result) >= c.pageRows || size > c.pageBytes {
			return nil, errResultLimit
		}
		if !utf8.ValidString(b.Service) || !utf8.ValidString(b.Level) || len(b.Service)+len(b.Level) > c.limits.MaxRecordBytes {
			return nil, ErrScanLimit
		}
		result = append(result, b)
	}
	if err := rows.Err(); err != nil {
		return nil, pageError(ctx, err)
	}
	return result, nil
}
