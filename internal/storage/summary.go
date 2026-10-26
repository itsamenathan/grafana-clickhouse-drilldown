// SPDX-License-Identifier: AGPL-3.0-only
package storage

import (
	"context"
	"errors"
	"strconv"
	"time"
	"unicode/utf8"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"grafana-clickhouse-drilldown/internal/chcontext"
	"grafana-clickhouse-drilldown/internal/record"
	"grafana-clickhouse-drilldown/internal/sqlgen"
)

// Summarize transfers only exact per-service counts and message-byte totals.
// Reader memory, execution/read limits and admission remain unchanged. This
// must never receive a selection with unevaluated residual pipeline stages.
func (c *Client) Summarize(ctx context.Context, predicate sqlgen.Predicate, start, end int64) ([]record.ServiceSummary, error) {
	if start < 0 || end < start || end-start > int64(time.Hour) {
		return nil, errors.New("invalid summary time range")
	}
	result := []record.ServiceSummary{}
	if start == end {
		return result, nil
	}
	release, err := c.gate.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	parameters := clickhouse.Parameters{}
	for key, value := range predicate.Parameters {
		parameters[key] = value
	}
	parameters["start_ns"], parameters["end_ns"] = strconv.FormatInt(start, 10), strconv.FormatInt(end, 10)
	ctx = clickhouse.Context(chcontext.ReadOnly(ctx), clickhouse.WithParameters(parameters))
	statement := "SELECT " + sqlgen.ServiceExpression + " AS service_name, count(), sum(length(Body)) FROM " + c.table +
		" WHERE Timestamp >= fromUnixTimestamp64Nano({start_ns:Int64}) AND Timestamp < fromUnixTimestamp64Nano({end_ns:Int64}) AND (" + predicate.SQL +
		") GROUP BY service_name ORDER BY service_name LIMIT 5001"
	rows, err := retryBusy(ctx, func() (driver.Rows, error) { return c.conn.Query(ctx, statement) })
	if err != nil {
		return nil, queryError(ctx, err)
	}
	defer rows.Close()
	size := 0
	for rows.Next() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		var summary record.ServiceSummary
		if err := rows.Scan(&summary.Service, &summary.Entries, &summary.Bytes); err != nil {
			return nil, errors.New("ClickHouse summary schema mismatch")
		}
		size += len(summary.Service) + 64
		if !utf8.ValidString(summary.Service) || len(summary.Service) > MaxRecordBytes || size > MaxScanBytes || len(result) >= 5000 {
			return nil, errors.New("service summary exceeds bounded result budget")
		}
		result = append(result, summary)
	}
	if err := rows.Err(); err != nil {
		return nil, queryError(ctx, err)
	}
	return result, nil
}
