// SPDX-License-Identifier: AGPL-3.0-only
package storage

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"
	"unicode/utf8"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"grafana-clickhouse-drilldown/internal/chcontext"
	"grafana-clickhouse-drilldown/internal/limits"
	"grafana-clickhouse-drilldown/internal/record"
	"grafana-clickhouse-drilldown/internal/sqlgen"
)

var ErrScanLimit = errors.New("selected log data exceeds the bounded scan budget; narrow the time range or selector")

const MaxScanRows = 10000
const MaxScanBytes = 8 << 20
const MaxRecordBytes = 64 << 10

// Scan never returns successful partial results. Endpoint callers must finish
// this scan and any residual evaluation before committing their HTTP response.
// Time bounds are [start,end); metric callers extend the upper bound by 1 ns
// to include the final evaluation timestamp.
func (c *Client) Scan(ctx context.Context, predicate sqlgen.Predicate, start, end int64, visit func(record.Raw) error) error {
	return c.scan(ctx, predicate, start, end, "ASC", MaxScanRows+1, visit)
}

// ScanPage is only for a proven row-preserving pipeline. SQL selects the
// requested global page before transmitting large metadata maps. Backward
// pages are reversed into ascending complete-row order for the shared engine.
func (c *Client) ScanPage(ctx context.Context, predicate sqlgen.Predicate, start, end int64, direction string, limit int, visit func(record.Raw) error) error {
	if limit < 1 || limit > 2000 || direction != "forward" && direction != "backward" {
		return errors.New("invalid storage log page")
	}
	if direction == "forward" {
		return c.scan(ctx, predicate, start, end, "ASC", limit, visit)
	}
	page := make([]record.Raw, 0, limit)
	if err := c.scan(ctx, predicate, start, end, "DESC", limit, func(row record.Raw) error {
		page = append(page, row)
		return nil
	}); err != nil {
		return err
	}
	for i := len(page) - 1; i >= 0; i-- {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := visit(page[i]); err != nil {
			return err
		}
	}
	return nil
}

func (c *Client) scan(ctx context.Context, predicate sqlgen.Predicate, start, end int64, order string, limit int, visit func(record.Raw) error) error {
	if start < 0 || end < start || end-start > int64(2*time.Hour+time.Millisecond) {
		return errors.New("invalid storage time range")
	}
	if start == end {
		return nil
	}
	release, err := c.gate.Acquire(ctx)
	if err != nil {
		return err
	}
	defer release()
	parameters := clickhouse.Parameters{}
	for key, value := range predicate.Parameters {
		parameters[key] = value
	}
	parameters["start_ns"], parameters["end_ns"] = strconv.FormatInt(start, 10), strconv.FormatInt(end, 10)
	ctx = clickhouse.Context(chcontext.ReadOnly(ctx), clickhouse.WithParameters(parameters))
	statement := "SELECT Timestamp, Body, " + sqlgen.ServiceExpression + " AS service_name, " + sqlgen.LevelExpression + " AS detected_level, LogAttributes, ResourceAttributes, TraceId, SpanId FROM " + c.table +
		" WHERE Timestamp >= fromUnixTimestamp64Nano({start_ns:Int64}) AND Timestamp < fromUnixTimestamp64Nano({end_ns:Int64}) AND (" + predicate.SQL +
		") ORDER BY Timestamp " + order + ", service_name " + order + ", detected_level " + order + ", Body " + order + ", TraceId " + order + ", SpanId " + order + ", toJSONString(LogAttributes) " + order + ", toJSONString(ResourceAttributes) " + order + " LIMIT " + strconv.Itoa(limit)
	rows, err := retryBusy(ctx, func() (driver.Rows, error) { return c.conn.Query(ctx, statement) })
	if err != nil {
		return queryError(ctx, err)
	}
	defer rows.Close()
	count, size := 0, 0
	for rows.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		var timestamp time.Time
		var row record.Raw
		if err := rows.Scan(&timestamp, &row.Body, &row.Service, &row.Level, &row.LogAttributes, &row.ResourceAttributes, &row.TraceID, &row.SpanID); err != nil {
			return errors.New("ClickHouse log schema mismatch")
		}
		row.Timestamp = timestamp.UnixNano()
		for _, value := range []string{row.Body, row.Service, row.Level, row.TraceID, row.SpanID} {
			if !utf8.ValidString(value) {
				return errors.New("binary/non-UTF-8 log values are not supported")
			}
		}
		rowBytes := len(row.Body) + len(row.Service) + len(row.Level) + len(row.TraceID) + len(row.SpanID)
		for _, attributes := range []map[string]string{row.LogAttributes, row.ResourceAttributes} {
			for key, value := range attributes {
				if !utf8.ValidString(value) {
					return errors.New("binary/non-UTF-8 attribute values are not supported")
				}
				rowBytes += len(key)*4 + len(value) + 64
			}
		}
		count++
		size += rowBytes
		if count > MaxScanRows || size > MaxScanBytes || rowBytes > MaxRecordBytes || len(row.LogAttributes)+len(row.ResourceAttributes) > 128 {
			return ErrScanLimit
		}
		if err := visit(row); err != nil {
			return err
		}
	}
	if err := rows.Err(); err != nil {
		return queryError(ctx, err)
	}
	return nil
}

// Preserve cancellation and expose only the numeric server code. Exception
// messages can contain SQL, values and log data, so never forward them.
func queryError(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	var exception *clickhouse.Exception
	if errors.As(err, &exception) {
		if exception.Code == 202 {
			return fmt.Errorf("%w: ClickHouse reader remains busy after bounded retry", limits.ErrOverloaded)
		}
		if exception.Code == 396 {
			return fmt.Errorf("%w (code %d)", ErrScanLimit, exception.Code)
		}
		return fmt.Errorf("ClickHouse query failed or exceeded a server limit (code %d)", exception.Code)
	}
	return errors.New("ClickHouse query failed or exceeded a server limit")
}
