// SPDX-License-Identifier: AGPL-3.0-only
package chcontext

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestPreservesCancellationAndValues(t *testing.T) {
	type key struct{}
	parent, cancel := context.WithCancel(context.WithValue(context.Background(), key{}, "fixture"))
	defer cancel()
	ctx := ReadOnly(parent)
	if ctx.Value(key{}) != "fixture" || ctx.Done() != parent.Done() {
		t.Fatal("driver context lost values or cancellation signal")
	}
	cancel()
	if !errors.Is(ctx.Err(), context.Canceled) {
		t.Fatal("driver context lost cancellation error")
	}
}

func TestDeadlineStillCancels(t *testing.T) {
	parent, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	ctx := ReadOnly(parent)
	if _, ok := ctx.Deadline(); ok {
		t.Fatal("driver will attempt to change max_execution_time")
	}
	select {
	case <-ctx.Done():
	default:
		t.Fatal("original deadline did not cancel driver context")
	}
	if !errors.Is(ctx.Err(), context.DeadlineExceeded) {
		t.Fatal("driver context lost deadline error")
	}
}
