// This isolated module evaluates a dependency. It is not the adapter parser.
package parserprobe

import (
	"testing"

	"github.com/grafana/loki/v3/pkg/logql/syntax"
)

func TestCandidateSyntax(t *testing.T) {
	for _, query := range []string{
		"{service_name=`fixture-api`}",
		`{service_name="fixture-api", detected_level!="debug"} |= "hello" != "ignore" |~ "(?i)error" !~ "health.*"`,
		`{service_name=~".+", service_name!~"fixture-worker"}`,
		`{service_name="fixture-api"} | json | __error__="" | drop __error__, __error_details__`,
		`{service_name="fixture-api"} | logfmt | duration > 10`,
		`sum by (service_name) (count_over_time({service_name="fixture-api"}[1m]))`,
		`sum(count_over_time({service_name="fixture-api"} | detected_level="error" [1m]))`,
	} {
		t.Run(query, func(t *testing.T) {
			if _, err := syntax.ParseExpr(query); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestCandidateRejectsMalformedQueries(t *testing.T) {
	for _, query := range []string{
		`{service_name="fixture-api"} trailing garbage`,
		`{service_name="fixture-api"} | unknown_stage`,
		`{service_name="fixture-api"} |=`,
		"{service_name=`unterminated}",
		`{service_name="fixture-api"} {service_name="fixture-worker"}`,
	} {
		t.Run(query, func(t *testing.T) {
			if _, err := syntax.ParseExpr(query); err == nil {
				t.Fatal("accepted malformed query")
			}
		})
	}
}

func TestInvalidLineRegexNeedsSemanticValidation(t *testing.T) {
	expr, err := syntax.ParseExpr(`{service_name="fixture-api"} |~ "["`)
	if err != nil {
		t.Fatalf("candidate's syntax-only behavior changed: %v", err)
	}
	selector, ok := expr.(syntax.LogSelectorExpr)
	if !ok {
		t.Fatal("expected log selector")
	}
	if _, err := selector.Pipeline(); err == nil {
		t.Fatal("invalid regex survived pipeline semantic validation")
	}
}

// Valid syntax is not proof that the adapter can execute it. These must be
// rejected by the future adapter's whole-AST validator until implemented.
func TestValidSyntaxStillNeedsAdapterValidation(t *testing.T) {
	for _, query := range []string{
		`{service_name="fixture-api"} | line_format "{{.service_name}}"`,
		`quantile_over_time(0.99, {service_name="fixture-api"} | json | unwrap duration [5m])`,
	} {
		if _, err := syntax.ParseExpr(query); err != nil {
			t.Fatalf("candidate unexpectedly rejected valid syntax %q: %v", query, err)
		}
	}
}
