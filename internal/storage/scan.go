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

// errResultLimit is the reader's server-side result-size error. A page that
// hits it is split and refetched; it never reaches a visitor partially.
var errResultLimit = errors.New("page exceeds the reader result budget")

const (
	// MaxScanRows bounds the records one request may read across all pages.
	MaxScanRows = 200000
	// MaxRecordBytes and maxRecordAttributes bound a single decoded record.
	MaxRecordBytes      = 64 << 10
	maxRecordAttributes = 128
	// Pages stay well below the reader's 10,000-row and 4 MiB result limits.
	// The estimate is ClickHouse's in-memory size, which can differ from the
	// native block size, hence the factor-of-two margin.
	pageRows  = 4000
	pageBytes = 2 << 20
	// A buffered page is held until it is complete, so a server limit hit
	// partway through can be refetched without visiting records twice.
	maxPageDecodedBytes = 16 << 20
	planBuckets         = 512
	maxPlanDepth        = 5
	maxSplitDepth       = 6
	rowOverhead         = 128
)

type window struct{ start, end int64 }

type bucket struct {
	window
	rows, bytes uint64
}

// Scan reads [start,end) in pages that each fit the reader's result budget.
// A cheap aggregate first sizes the selection; pages are then read in the
// requested direction with deterministic complete-row ordering, so a backward
// scan is exactly the reverse of a forward one. Each page is buffered whole
// before it is visited. A visitor returning record.ErrStop ends the scan
// successfully; any other error, or exceeding MaxScanRows, fails the scan.
func (c *Client) Scan(ctx context.Context, predicate sqlgen.Predicate, start, end int64, options record.ScanOptions, visit func(record.Raw) error) error {
	if !c.validRange(start, end) {
		return errors.New("invalid storage time range")
	}
	if options.Limit < 0 || options.Limit > c.limits.MaxScanRows {
		return errors.New("invalid storage row limit")
	}
	predicate = c.service.Bind(predicate)
	scanned, remaining := 0, options.Limit
	var scannedBytes uint64
	span := window{start, end}
	width := end - start
	if options.EdgeWindows || options.Limit > 0 {
		width = min(width, int64(time.Second))
	}
	for span.start < span.end {
		if err := ctx.Err(); err != nil {
			return err
		}
		edge := edgeWindow(span, width, options.Backward)
		pages, err := c.plan(ctx, predicate, edge, options.Metadata, 0)
		if err != nil {
			return err
		}
		for i := range pages {
			page := pages[i]
			if options.Backward {
				page = pages[len(pages)-1-i]
			}
			limit := 0
			if options.Limit > 0 {
				limit = remaining
			}
			rows, err := c.fetch(ctx, predicate, page, options, limit, 0)
			if err != nil {
				return err
			}
			// Count the whole buffered page, including rows beyond an early stop.
			if len(rows) > c.limits.MaxScanRows-scanned {
				return ErrScanLimit
			}
			scanned += len(rows)
			for _, row := range rows {
				size, err := decodedSizeWithLimits(row, c.limits)
				if err != nil {
					return err
				}
				if uint64(size) > c.limits.MaxScanBytes-scannedBytes {
					return ErrScanLimit
				}
				scannedBytes += uint64(size)
			}
			for _, row := range rows {
				if err := ctx.Err(); err != nil {
					return err
				}
				if err := visit(row); err != nil {
					if errors.Is(err, record.ErrStop) {
						return nil
					}
					return err
				}
			}
			if options.Limit > 0 {
				remaining -= len(rows)
				if remaining <= 0 {
					return nil
				}
			}
		}
		if options.Backward {
			span.end = edge.start
		} else {
			span.start = edge.end
		}
		// Saturating growth avoids overflow near the largest supported timestamp.
		growth := int64(4)
		if len(pages) == 0 {
			growth = 16
		}
		width = min(width, (span.end-span.start)/growth) * growth
		if width == 0 {
			width = span.end - span.start
		}
	}
	return nil
}

func edgeWindow(span window, width int64, backward bool) window {
	width = min(width, span.end-span.start)
	if backward {
		return window{span.end - width, span.end}
	}
	return window{span.start, span.start + width}
}

// plan groups the selection into contiguous pages within the page budget.
// A bucket that alone exceeds the budget is planned again at a finer
// resolution. A single nanosecond that still exceeds it fails explicitly.
func (c *Client) plan(ctx context.Context, predicate sqlgen.Predicate, span window, metadata bool, depth int) ([]window, error) {
	width := 1 + (span.end-span.start-1)/int64(c.planBuckets)
	buckets, err := c.buckets(ctx, predicate, span, width, metadata)
	if err != nil {
		return nil, err
	}
	var pages []window
	var current *bucket
	flush := func() {
		if current != nil {
			pages = append(pages, current.window)
			current = nil
		}
	}
	for _, b := range buckets {
		if b.rows > uint64(c.pageRows) || b.bytes > uint64(c.pageBytes) {
			flush()
			if width == 1 || depth >= maxPlanDepth {
				return nil, ErrScanLimit
			}
			finer, err := c.plan(ctx, predicate, b.window, metadata, depth+1)
			if err != nil {
				return nil, err
			}
			pages = append(pages, finer...)
			continue
		}
		if current != nil && (current.rows+b.rows > uint64(c.pageRows) || current.bytes+b.bytes > uint64(c.pageBytes)) {
			flush()
		}
		if current == nil {
			current = &bucket{window: b.window}
		}
		current.end = b.end
		current.rows += b.rows
		current.bytes += b.bytes
	}
	flush()
	return pages, nil
}

func (c *Client) buckets(ctx context.Context, predicate sqlgen.Predicate, span window, width int64, metadata bool) ([]bucket, error) {
	release, err := c.gate.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	// Account for every projected string, including mapped identities even
	// on lean scans. Map entry overhead matches decodedSizeWithLimits.
	size := "length(Body) + length(" + c.service.Expression() + ") + length(" + sqlgen.LevelExpression + ")"
	if metadata {
		size += " + byteSize(LogAttributes) + byteSize(ResourceAttributes) + 64 * (length(mapKeys(LogAttributes)) + length(mapKeys(ResourceAttributes))) + length(TraceId) + length(SpanId)"
	}
	parameters := scanParameters(predicate, span)
	parameters["width_ns"] = strconv.FormatInt(width, 10)
	queryCtx := clickhouse.Context(chcontext.ReadOnly(ctx), clickhouse.WithParameters(parameters))
	statement := "SELECT intDiv(toUnixTimestamp64Nano(Timestamp) - {start_ns:Int64}, {width_ns:Int64}) AS bucket, count(), sum(" + size + ") FROM " + c.table +
		" WHERE " + timeCondition + " AND (" + predicate.SQL + ") GROUP BY bucket ORDER BY bucket LIMIT " + strconv.Itoa(c.planBuckets+1)
	rows, err := retryBusy(queryCtx, func() (driver.Rows, error) { return c.conn.Query(queryCtx, statement) })
	if err != nil {
		return nil, queryError(ctx, err)
	}
	defer rows.Close()
	var result []bucket
	for rows.Next() {
		var index int64
		var b bucket
		if err := rows.Scan(&index, &b.rows, &b.bytes); err != nil {
			return nil, errors.New("ClickHouse scan plan schema mismatch")
		}
		b.start = span.start + index*width
		b.end = b.start + min(width, span.end-b.start)
		b.bytes += b.rows * rowOverhead
		if index < 0 || b.start >= span.end || len(result) >= c.planBuckets {
			return nil, errors.New("ClickHouse scan plan is inconsistent")
		}
		result = append(result, b)
	}
	if err := rows.Err(); err != nil {
		return nil, queryError(ctx, err)
	}
	return result, nil
}

// fetch reads one page completely. If ClickHouse still rejects the page for
// its result size, the page is split in time and refetched; nothing from the
// rejected attempt has been visited.
func (c *Client) fetch(ctx context.Context, predicate sqlgen.Predicate, page window, options record.ScanOptions, limit, depth int) ([]record.Raw, error) {
	remainingBytes := c.limits.MaxPageDecodedBytes
	return c.fetchWithinBudget(ctx, predicate, page, options, limit, depth, &remainingBytes)
}

func (c *Client) fetchWithinBudget(ctx context.Context, predicate sqlgen.Predicate, page window, options record.ScanOptions, limit, depth int, remainingBytes *int) ([]record.Raw, error) {
	rows, err := c.page(ctx, predicate, page, options, limit, remainingBytes)
	if !errors.Is(err, errResultLimit) {
		return rows, err
	}
	if depth >= maxSplitDepth || page.end-page.start < 2 {
		return nil, ErrScanLimit
	}
	middle := page.start + (page.end-page.start)/2
	halves := [2]window{{page.start, middle}, {middle, page.end}}
	if options.Backward {
		halves[0], halves[1] = halves[1], halves[0]
	}
	first, err := c.fetchWithinBudget(ctx, predicate, halves[0], options, limit, depth+1, remainingBytes)
	if err != nil || limit > 0 && len(first) >= limit {
		return first, err
	}
	if limit > 0 {
		limit -= len(first)
	}
	second, err := c.fetchWithinBudget(ctx, predicate, halves[1], options, limit, depth+1, remainingBytes)
	if err != nil {
		return nil, err
	}
	return append(first, second...), nil
}

const timeCondition = "Timestamp >= fromUnixTimestamp64Nano({start_ns:Int64}) AND Timestamp < fromUnixTimestamp64Nano({end_ns:Int64})"

func scanParameters(predicate sqlgen.Predicate, span window) clickhouse.Parameters {
	parameters := clickhouse.Parameters{}
	for key, value := range predicate.Parameters {
		parameters[key] = value
	}
	parameters["start_ns"], parameters["end_ns"] = strconv.FormatInt(span.start, 10), strconv.FormatInt(span.end, 10)
	return parameters
}

func (c *Client) page(ctx context.Context, predicate sqlgen.Predicate, page window, options record.ScanOptions, limit int, remainingBytes *int) ([]record.Raw, error) {
	release, err := c.gate.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	order := "ASC"
	if options.Backward {
		order = "DESC"
	}
	columns := "Timestamp, Body, " + c.service.Expression() + " AS service_name, " + sqlgen.LevelExpression + " AS detected_level"
	ordering := "Timestamp " + order + ", service_name " + order + ", detected_level " + order + ", Body " + order
	if options.Metadata {
		columns += ", LogAttributes, ResourceAttributes, TraceId, SpanId"
		ordering += ", TraceId " + order + ", SpanId " + order + ", toJSONString(LogAttributes) " + order + ", toJSONString(ResourceAttributes) " + order
	}
	statement := "SELECT " + columns + " FROM " + c.table + " WHERE " + timeCondition + " AND (" + predicate.SQL + ") ORDER BY " + ordering
	parameters := scanParameters(predicate, page)
	if limit > 0 {
		statement += " LIMIT {row_limit:UInt64}"
		parameters["row_limit"] = strconv.Itoa(limit)
	}
	queryCtx := clickhouse.Context(chcontext.ReadOnly(ctx), clickhouse.WithParameters(parameters))
	rows, err := retryBusy(queryCtx, func() (driver.Rows, error) { return c.conn.Query(queryCtx, statement) })
	if err != nil {
		return nil, pageError(ctx, err)
	}
	defer rows.Close()
	var result []record.Raw
	size := 0
	for rows.Next() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		var timestamp time.Time
		var row record.Raw
		targets := []any{&timestamp, &row.Body, &row.Service, &row.Level}
		if options.Metadata {
			targets = append(targets, &row.LogAttributes, &row.ResourceAttributes, &row.TraceID, &row.SpanID)
		}
		if err := rows.Scan(targets...); err != nil {
			return nil, errors.New("ClickHouse log schema mismatch")
		}
		row.Timestamp = timestamp.UnixNano()
		rowBytes, err := decodedSizeWithLimits(row, c.limits)
		if err != nil {
			return nil, err
		}
		size += rowBytes
		if rowBytes > c.limits.MaxRecordBytes || size > *remainingBytes {
			return nil, ErrScanLimit
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		return nil, pageError(ctx, err)
	}
	// Rejected server attempts retain no rows and consume no budget. Every
	// successful child shares this budget with its siblings before append.
	*remainingBytes -= size
	return result, nil
}

// decodedSize validates a record's text and approximates its retained size.
func decodedSize(row record.Raw) (int, error) {
	costs, _ := (Limits{}).defaults()
	return decodedSizeWithLimits(row, costs)
}

func decodedSizeWithLimits(row record.Raw, costs Limits) (int, error) {
	for _, value := range []string{row.Body, row.Service, row.Level, row.TraceID, row.SpanID} {
		if !utf8.ValidString(value) {
			return 0, errors.New("binary/non-UTF-8 log values are not supported")
		}
	}
	if len(row.LogAttributes)+len(row.ResourceAttributes) > costs.MaxRecordAttributes {
		return 0, ErrScanLimit
	}
	size := len(row.Body) + len(row.Service) + len(row.Level) + len(row.TraceID) + len(row.SpanID) + rowOverhead
	for _, attributes := range []map[string]string{row.LogAttributes, row.ResourceAttributes} {
		for key, value := range attributes {
			if !utf8.ValidString(key) || !utf8.ValidString(value) {
				return 0, errors.New("binary/non-UTF-8 attribute values are not supported")
			}
			size += len(key) + len(value) + 64
		}
	}
	return size, nil
}

func pageError(ctx context.Context, err error) error {
	var exception *clickhouse.Exception
	if ctx.Err() == nil && errors.As(err, &exception) && exception.Code == 396 {
		return errResultLimit
	}
	return queryError(ctx, err)
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
		if exception.Code == 396 || exception.Code == 158 {
			return fmt.Errorf("%w (code %d)", ErrScanLimit, exception.Code)
		}
		return fmt.Errorf("ClickHouse query failed or exceeded a server limit (code %d)", exception.Code)
	}
	return errors.New("ClickHouse query failed or exceeded a server limit")
}
