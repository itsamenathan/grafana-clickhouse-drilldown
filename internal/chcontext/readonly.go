// SPDX-License-Identifier: AGPL-3.0-only

// Package chcontext adapts contexts only at the native ClickHouse driver boundary.
package chcontext

import (
	"context"
	"time"
)

// ReadOnly preserves cancellation, errors and values, but prevents
// clickhouse-go v2.40.3 from converting a deadline into a max_execution_time
// setting change. Its context.go queryOptions otherwise adds five seconds and
// overwrites that setting, which a readonly=1 account rejects. The server
// profile still enforces its fixed execution limit; Done still fires at the
// original deadline and the native driver cancels the query on that signal.
// Apply only when passing a context to this pinned native driver, never when
// acquiring admission or deriving the application's request deadline.
func ReadOnly(ctx context.Context) context.Context { return withoutDriverDeadline{ctx} }

type withoutDriverDeadline struct{ context.Context }

func (withoutDriverDeadline) Deadline() (time.Time, bool) { return time.Time{}, false }
