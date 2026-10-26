// SPDX-License-Identifier: AGPL-3.0-only
package limits

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
)

func TestNoWaitingRoom(t *testing.T) {
	g, err := New(1, 0)
	if err != nil {
		t.Fatal(err)
	}
	release, err := g.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := g.Acquire(context.Background()); !errors.Is(err, ErrOverloaded) {
		t.Fatalf("expected overload, got %v", err)
	}
	release()
	release() // defensive idempotence
	release, err = g.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	release()
}

func TestCanceledQueuedRequestFreesAdmission(t *testing.T) {
	g, _ := New(1, 1)
	release, _ := g.Acquire(context.Background())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	// Simulate an occupied queue admission deterministically, then check a
	// canceled waiter releases it through the real Acquire path below.
	g.total <- struct{}{}
	if _, err := g.Acquire(context.Background()); !errors.Is(err, ErrOverloaded) {
		t.Fatal("queue capacity was not enforced")
	}
	<-g.total
	admitted := make(chan struct{})
	observed := &observedContext{Context: ctx, admitted: admitted}
	go func() { _, err := g.Acquire(observed); result <- err }()
	<-admitted
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("expected cancellation: %v", err)
	}
	release()
	if len(g.total) != 0 || len(g.active) != 0 {
		t.Fatal("cancellation leaked capacity")
	}
}

// Acquire evaluates Done only after taking a queue slot. Observe that point
// without sleeps, so this test cancels an actually queued request.
type observedContext struct {
	context.Context
	admitted chan struct{}
	once     sync.Once
}

func (c *observedContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.admitted) })
	return c.Context.Done()
}

func TestConcurrencyBound(t *testing.T) {
	g, _ := New(2, 128)
	var running atomic.Int64
	var failed atomic.Bool
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			release, err := g.Acquire(context.Background())
			if err != nil {
				failed.Store(true)
				return
			}
			if running.Add(1) > 2 {
				failed.Store(true)
			}
			running.Add(-1)
			release()
		}()
	}
	wg.Wait()
	if failed.Load() || len(g.active) != 0 || len(g.total) != 0 {
		t.Fatal("concurrency/admission bound violated")
	}
}

func TestInvalidLimits(t *testing.T) {
	for _, tc := range [][2]int{{0, 0}, {3, 0}, {1, -1}, {2, 129}} {
		if _, err := New(tc[0], tc[1]); err == nil {
			t.Fatalf("accepted %v", tc)
		}
	}
}
