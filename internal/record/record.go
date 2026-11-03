// SPDX-License-Identifier: AGPL-3.0-only
package record

import (
	"encoding/hex"
	"errors"
	"strings"
)

type ServiceSummary struct {
	Service string
	Entries uint64
	Bytes   uint64
}

// ErrStop ends a scan early from its visitor. The scan then reports success:
// callers use it only after they hold a complete answer, never to truncate.
var ErrStop = errors.New("scan stopped by its consumer")

// ScanOptions select record order and columns. Metadata transfers attribute
// maps and trace identifiers; without it, records carry only the timestamp,
// body and SQL-computed service/level. Limit is a SQL row limit and is valid
// only for selections whose residual pipeline preserves every row.
type ScanOptions struct {
	Backward bool
	Metadata bool
	Limit    int
	// EdgeWindows plans from the requested edge, for consumers that stop early.
	EdgeWindows bool
}

type Raw struct {
	Timestamp                         int64
	Body, Service, Level              string
	LogAttributes, ResourceAttributes map[string]string
	TraceID, SpanID                   string
}

// Alias gives each attribute scope and original UTF-8 key a reversible name.
// Escaping underscores as well as dots prevents a.b and a_b from colliding.
func Alias(scope, key string) string {
	var b strings.Builder
	b.WriteString(scope)
	b.WriteByte('_')
	for _, c := range []byte(key) {
		switch {
		case c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9':
			b.WriteByte(c)
		case c == '_':
			b.WriteString("_u")
		case c == '.':
			b.WriteString("_d")
		default:
			b.WriteString("_x")
			b.WriteString(hex.EncodeToString([]byte{c}))
		}
	}
	return b.String()
}

func Unalias(alias string) (string, string, error) {
	scope, rest, found := strings.Cut(alias, "_")
	if !found || scope != "log" && scope != "resource" {
		return "", "", errors.New("invalid attribute alias")
	}
	var key []byte
	for i := 0; i < len(rest); i++ {
		if rest[i] != '_' {
			key = append(key, rest[i])
			continue
		}
		i++
		if i >= len(rest) {
			return "", "", errors.New("invalid attribute escape")
		}
		switch rest[i] {
		case 'u':
			key = append(key, '_')
		case 'd':
			key = append(key, '.')
		case 'x':
			if i+2 >= len(rest) {
				return "", "", errors.New("invalid attribute escape")
			}
			value, err := hex.DecodeString(rest[i+1 : i+3])
			if err != nil {
				return "", "", errors.New("invalid attribute escape")
			}
			key = append(key, value[0])
			i += 2
		default:
			return "", "", errors.New("invalid attribute escape")
		}
	}
	if Alias(scope, string(key)) != alias {
		return "", "", errors.New("noncanonical attribute alias")
	}
	return scope, string(key), nil
}

func (r Raw) Metadata() map[string]string {
	m := make(map[string]string, len(r.LogAttributes)+len(r.ResourceAttributes)+2)
	for key, value := range r.LogAttributes {
		if value != "" {
			m[Alias("log", key)] = value
		}
	}
	for key, value := range r.ResourceAttributes {
		if value != "" {
			m[Alias("resource", key)] = value
		}
	}
	if r.TraceID != "" {
		m["trace_id"] = r.TraceID
	}
	if r.SpanID != "" {
		m["span_id"] = r.SpanID
	}
	return m
}

// MetricBucket covers (Timestamp-step,Timestamp], including exact nanosecond
// edge semantics. Only explicitly requested SQL grouping labels are populated.
type MetricBucket struct {
	Timestamp      int64
	Service, Level string
	Entries, Bytes uint64
	ParserGuard    uint64
}
