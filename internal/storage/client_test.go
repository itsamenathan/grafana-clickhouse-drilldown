// SPDX-License-Identifier: AGPL-3.0-only
package storage

import "testing"

func TestIdentifier(t *testing.T) {
	for _, s := range []string{"default", "otel_logs", "_logs2", "Logs"} {
		if !Identifier(s) {
			t.Fatalf("rejected %q", s)
		}
	}
	for _, s := range []string{"", "2logs", "a.b", "logs; DROP TABLE logs", "`logs`", "雪", "logs--", "logs\n"} {
		if Identifier(s) {
			t.Fatalf("accepted %q", s)
		}
	}
}

func TestPageBudgets(t *testing.T) {
	for _, tc := range []struct {
		rows, bytes         uint64
		wantRows, wantBytes int
	}{
		{0, 0, 4000, 2 << 20}, {10000, 4 << 20, 4000, 2 << 20}, {100, 1024, 50, 512}, {200000, 100 << 20, 4000, 2 << 20},
	} {
		r, b := pageBudgets(tc.rows, tc.bytes)
		if r != tc.wantRows || b != tc.wantBytes {
			t.Fatalf("%+v got %d/%d", tc, r, b)
		}
	}
}
