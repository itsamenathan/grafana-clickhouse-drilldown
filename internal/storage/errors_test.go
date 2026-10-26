// SPDX-License-Identifier: AGPL-3.0-only
package storage

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
)

func TestQueryErrorRetainsCancellationWithoutLeakingExceptions(t *testing.T) {
	exception := &clickhouse.Exception{Code: 241, Message: "private SQL and log contents"}
	err := queryError(context.Background(), exception)
	if !strings.Contains(err.Error(), "code 241") || strings.Contains(err.Error(), "private") {
		t.Fatal("server exception code was lost or its sensitive message leaked")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if !errors.Is(queryError(ctx, exception), context.Canceled) {
		t.Fatal("query cancellation became an unrelated database error")
	}
	err = queryError(context.Background(), &clickhouse.Exception{Code: 396, Message: "private result details"})
	if !errors.Is(err, ErrScanLimit) || !strings.Contains(err.Error(), "narrow") || strings.Contains(err.Error(), "private") {
		t.Fatal("result budget error must be actionable without leaking server details")
	}
}
