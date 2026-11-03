// SPDX-License-Identifier: AGPL-3.0-only
package storage

import (
	"math"
	"testing"
	"time"
)

func TestStorageLimits(t *testing.T) {
	l, err := (Limits{}).defaults()
	if err != nil || l.MaxScanRows != MaxScanRows || l.MaxScanSpan != 0 {
		t.Fatalf("bad defaults: %+v %v", l, err)
	}
	c := Client{limits: l}
	if !c.validRange(0, int64(30*24*time.Hour)) || c.validRange(-1, 0) || c.validRange(1, 0) {
		t.Fatal("default range validation")
	}
	c.limits.MaxScanSpan = time.Hour
	if c.validRange(0, int64(2*time.Hour)) {
		t.Fatal("configured span ignored")
	}
	if _, err := (Limits{MaxScanRows: -1}).defaults(); err == nil {
		t.Fatal("accepted negative budget")
	}
}

func TestEdgeWindowsCoverRangeInOrder(t *testing.T) {
	for _, backward := range []bool{false, true} {
		span := window{1, math.MaxInt64}
		width := int64(time.Second)
		previous := span.start
		if backward {
			previous = span.end
		}
		for n := 0; span.start < span.end; n++ {
			if n > 100 {
				t.Fatal("growth did not terminate")
			}
			edge := edgeWindow(span, width, backward)
			if edge.start >= edge.end {
				t.Fatal("empty edge")
			}
			if backward {
				if edge.end != previous {
					t.Fatal("backward gap/overlap")
				}
				previous = edge.start
				span.end = edge.start
			} else {
				if edge.start != previous {
					t.Fatal("forward gap/overlap")
				}
				previous = edge.end
				span.start = edge.end
			}
			width = min(width, (span.end-span.start)/2) * 2
			if width == 0 {
				width = span.end - span.start
			}
		}
	}
}
