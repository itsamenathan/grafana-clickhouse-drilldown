// SPDX-License-Identifier: AGPL-3.0-only
package record

import "testing"

func TestAliasesAreScopedReversibleAndCollisionSafe(t *testing.T) {
	seen := map[string]bool{}
	for _, scope := range []string{"log", "resource"} {
		for _, key := range []string{"a.b", "a_b", "a_db", "a_ub", "", "雪", "space key", "__error__", "trace_id", "_x2e", "0"} {
			alias := Alias(scope, key)
			if seen[alias] {
				t.Fatalf("alias collision: %s", alias)
			}
			seen[alias] = true
			gotScope, gotKey, err := Unalias(alias)
			if err != nil || gotScope != scope || gotKey != key {
				t.Fatalf("round trip %s %s: %s %s %v", scope, key, gotScope, gotKey, err)
			}
		}
	}
}

func TestEmptyMetadataHasLokiMissingLabelSemantics(t *testing.T) {
	metadata := (Raw{LogAttributes: map[string]string{"empty": "", "a.b": "dot", "a_b": "underscore"}}).Metadata()
	if _, exists := metadata["log_empty"]; exists {
		t.Fatal("Loki 3.7 drops empty metadata values at ingestion")
	}
	if metadata["log_a_db"] != "dot" || metadata["log_a_ub"] != "underscore" {
		t.Fatal("nonempty attributes collided or were lost")
	}
}
