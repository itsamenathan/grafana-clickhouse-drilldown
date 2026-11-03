// SPDX-License-Identifier: AGPL-3.0-only
package storage

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"grafana-clickhouse-drilldown/internal/limits"
)

func TestRetryOnlyTransientReaderBusy(t *testing.T) {
	calls := 0
	_, err := retryBusy(context.Background(), func() (driver.Rows, error) {
		calls++
		if calls <= 2 {
			return nil, &clickhouse.Exception{Code: 202}
		}
		return nil, nil
	})
	if err != nil || calls != 3 {
		t.Fatal("transient busy response was not retried")
	}
	for _, code := range []int32{241, 396, 497} {
		calls = 0
		_, err = retryBusy(context.Background(), func() (driver.Rows, error) {
			calls++
			return nil, &clickhouse.Exception{Code: code}
		})
		if err == nil || calls != 1 {
			t.Fatal("non-transient server limit was retried")
		}
	}
}

func TestBusyRetryIsBoundedAndCancelable(t *testing.T) {
	calls := 0
	_, err := retryBusy(context.Background(), func() (driver.Rows, error) {
		calls++
		return nil, &clickhouse.Exception{Code: 202}
	})
	if calls != 8 || !errors.Is(queryError(context.Background(), err), limits.ErrOverloaded) {
		t.Fatal("persistent reader contention did not become explicit overload")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	calls = 0
	_, err = retryBusy(ctx, func() (driver.Rows, error) {
		calls++
		return nil, &clickhouse.Exception{Code: 202}
	})
	if !errors.Is(err, context.DeadlineExceeded) || calls > 1 {
		t.Fatal("busy retry ignored the original deadline")
	}
}

// nilRows mirrors the driver's typed nil *rows returned with an error.
type nilRows struct{ driver.Rows }

func (r *nilRows) Close() error { return r.Rows.Close() }

func TestBusyRetryNeverClosesTypedNilRows(t *testing.T) {
	calls := 0
	_, err := retryBusy(context.Background(), func() (driver.Rows, error) {
		calls++
		if calls == 1 {
			var rows *nilRows
			return rows, &clickhouse.Exception{Code: 202}
		}
		return nil, nil
	})
	if err != nil || calls != 2 {
		t.Fatalf("busy retry with typed nil rows: calls=%d err=%v", calls, err)
	}
}
