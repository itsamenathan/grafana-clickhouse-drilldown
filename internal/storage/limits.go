// SPDX-License-Identifier: AGPL-3.0-only
package storage

import (
	"errors"
	"time"
)

// Limits are per-request/client cost bounds. Zero uses the default, except
// MaxScanSpan where zero means no time cap. Reader result limits are discovered at startup.
type Limits struct {
	MaxScanRows         int
	MaxScanBytes        uint64
	MaxRecordBytes      int
	MaxRecordAttributes int
	MaxPageDecodedBytes int
	MaxMetricBuckets    int
	MaxScanSpan         time.Duration
	QueryTimeout        time.Duration
}

func (l Limits) defaults() (Limits, error) {
	if l.MaxScanRows < 0 || l.MaxRecordBytes < 0 || l.MaxRecordAttributes < 0 || l.MaxPageDecodedBytes < 0 || l.MaxMetricBuckets < 0 || l.MaxScanSpan < 0 || l.QueryTimeout < 0 {
		return l, errors.New("invalid storage limits")
	}
	if l.MaxScanRows == 0 {
		l.MaxScanRows = MaxScanRows
	}
	if l.MaxRecordBytes == 0 {
		l.MaxRecordBytes = MaxRecordBytes
	}
	if l.MaxRecordAttributes == 0 {
		l.MaxRecordAttributes = maxRecordAttributes
	}
	if l.MaxPageDecodedBytes == 0 {
		l.MaxPageDecodedBytes = maxPageDecodedBytes
	}
	if l.MaxMetricBuckets == 0 {
		l.MaxMetricBuckets = 100000
	}
	if l.MaxScanBytes == 0 {
		l.MaxScanBytes = 256 << 20
	}
	if l.QueryTimeout == 0 {
		l.QueryTimeout = 15 * time.Second
	}
	return l, nil
}

func (c *Client) validRange(start, end int64) bool {
	return start >= 0 && end >= start && (c.limits.MaxScanSpan == 0 || end-start <= int64(c.limits.MaxScanSpan))
}
