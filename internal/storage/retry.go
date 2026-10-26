// SPDX-License-Identifier: AGPL-3.0-only
package storage

import (
	"context"
	"errors"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// The caller retains its admission permit throughout this bounded retry.
// Native cancellation/end-of-stream can precede the server removing the query
// from its process list. Retry only that transient per-reader busy response,
// never a memory/read/result limit or arbitrary database/transport error.
func retryBusy(ctx context.Context, run func() (driver.Rows, error)) (driver.Rows, error) {
	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		rows, err := run()
		var exception *clickhouse.Exception
		if err == nil || !errors.As(err, &exception) || exception.Code != 202 || attempt >= 7 {
			return rows, err
		}
		if rows != nil {
			rows.Close()
		}
		timer := time.NewTimer((5 * time.Millisecond) << attempt)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}
