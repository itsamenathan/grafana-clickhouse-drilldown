// SPDX-License-Identifier: AGPL-3.0-only
package query

import (
	"context"
	"testing"
)

// Reserved label names in the body prevent eliding these stages in metrics.
func TestMetricParserDropsLeaveParsedErrorLabels(t *testing.T) {
	for _, stages := range []string{`| json | logfmt | drop __error__, __error_details__`, `| logfmt --strict | drop __error__, __error_details__`} {
		s, err := Parse(`{service_name="api"} ` + stages)
		if err != nil {
			t.Fatal(err)
		}
		for body, want := range map[string]string{`bad json`: "", `{"log_x":"parsed","__error__":"body"}`: "body", `bad="unterminated`: "", `n=12 __error__="body"`: "body"} {
			// JSON and logfmt each parse their own reserved-label example.
			if stages == `| logfmt --strict | drop __error__, __error_details__` && body[0] == '{' {
				continue
			}
			e, err := s.NewEvaluator()
			if err != nil {
				t.Fatal(err)
			}
			got, ok, err := e.Process(context.Background(), 1, body, map[string]string{"service_name": "api"}, map[string]string{"detected_level": "info"})
			if err != nil || !ok || got.Parsed["__error__"] != want {
				t.Fatalf("%s %s: parsed error=%q want=%q ok=%v err=%v", stages, body, got.Parsed["__error__"], want, ok, err)
			}
		}
	}
}
