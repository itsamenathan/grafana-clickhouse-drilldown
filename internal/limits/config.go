// SPDX-License-Identifier: AGPL-3.0-only
package limits

import (
	"errors"
	"math"
	"time"
)

// Config bounds work per request. A zero MaxQueryRange disables the optional
// time cap. The other zero values select defaults for embedded callers.
type Config struct {
	MaxQueryRange    time.Duration
	RequestTimeout   time.Duration
	TailLookback     time.Duration
	PatternLines     int
	MaxRetainedBytes int
}

func (c Config) WithDefaults() Config {
	if c.RequestTimeout == 0 {
		c.RequestTimeout = 15 * time.Second
	}
	if c.TailLookback == 0 {
		c.TailLookback = time.Hour
	}
	if c.PatternLines == 0 {
		// Enough lines for Drain's 30-sample pruning. Lines stream into Drain;
		// only its models count against the retained-memory budget.
		c.PatternLines = 50000
	}
	if c.MaxRetainedBytes == 0 {
		c.MaxRetainedBytes = 32 << 20
	}
	return c
}

func (c Config) Validate() error {
	if c.MaxQueryRange < 0 || c.RequestTimeout <= 0 || c.RequestTimeout > time.Duration(1<<63-1)-10*time.Second || c.TailLookback <= 0 || c.PatternLines <= 0 || c.PatternLines == math.MaxInt || c.MaxRetainedBytes <= 0 {
		return errors.New("invalid query limits: range must be non-negative; timeout, tail lookback, pattern lines and retained bytes must be positive")
	}
	return nil
}
