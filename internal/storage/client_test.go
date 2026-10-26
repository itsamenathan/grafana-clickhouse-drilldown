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
