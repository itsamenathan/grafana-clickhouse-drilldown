// SPDX-License-Identifier: AGPL-3.0-only
package sqlgen

import (
	"errors"
	"strings"
)

// ServiceMapping is immutable after validation. Its zero value is OTel's
// ServiceName column, allowing the MergeTree primary key to prune services.
type ServiceMapping struct{ expression string }

// ParseServiceMapping accepts ordered attribute keys. Unqualified keys are log
// attributes; resource: and log: select a scope. Keys are strictly restricted
// ASCII literals, never query text. Empty attribute values fall through.
func ParseServiceMapping(raw string) (ServiceMapping, error) {
	if raw == "" {
		return ServiceMapping{}, nil
	}
	keys := strings.Split(raw, ",")
	if len(keys) > 16 {
		return ServiceMapping{}, errors.New("too many service name attributes")
	}
	expressions := make([]string, 0, len(keys)+1)
	for _, key := range keys {
		key = strings.TrimSpace(key)
		column := "LogAttributes"
		if strings.HasPrefix(key, "resource:") {
			column = "ResourceAttributes"
			key = strings.TrimPrefix(key, "resource:")
		} else {
			key = strings.TrimPrefix(key, "log:")
		}
		if len(key) == 0 || len(key) > 256 {
			return ServiceMapping{}, errors.New("invalid service name attribute")
		}
		for _, r := range key {
			if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || strings.ContainsRune("_.:/-", r)) {
				return ServiceMapping{}, errors.New("invalid service name attribute")
			}
		}
		expressions = append(expressions, "nullIf("+column+"['"+key+"'], '')")
	}
	expressions = append(expressions, ServiceExpression)
	return ServiceMapping{expression: "coalesce(" + strings.Join(expressions, ", ") + ")"}, nil
}

func (m ServiceMapping) Expression() string {
	if m.expression == "" {
		return ServiceExpression
	}
	return m.expression
}

// Bind resolves the compiler's service column at the storage boundary. Query
// values are typed parameters, so they can never contain this SQL identifier.
// All service selections and output/grouping expressions use this same mapping.
func (m ServiceMapping) Bind(p Predicate) Predicate {
	p.SQL = strings.ReplaceAll(p.SQL, ServiceExpression, m.Expression())
	return p
}
