// SPDX-License-Identifier: AGPL-3.0-only
package limits

import (
	"context"
	"errors"
	"sync"
)

var ErrOverloaded = errors.New("query queue is full")

type Gate struct {
	active chan struct{}
	total  chan struct{}
}

func New(active, queued int) (*Gate, error) {
	if active < 1 || active > 2 || queued < 0 || queued > 128 {
		return nil, errors.New("invalid query concurrency limits")
	}
	return &Gate{active: make(chan struct{}, active), total: make(chan struct{}, active+queued)}, nil
}

// Acquire limits both running work and queued callers. Queue admission never
// blocks. Cancellation while waiting frees the slot without starting work.
func (g *Gate) Acquire(ctx context.Context) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	select {
	case g.total <- struct{}{}:
	default:
		return nil, ErrOverloaded
	}
	select {
	case g.active <- struct{}{}:
		if err := ctx.Err(); err != nil {
			<-g.active
			<-g.total
			return nil, err
		}
		var once sync.Once
		return func() { once.Do(func() { <-g.active; <-g.total }) }, nil
	case <-ctx.Done():
		<-g.total
		return nil, ctx.Err()
	}
}
