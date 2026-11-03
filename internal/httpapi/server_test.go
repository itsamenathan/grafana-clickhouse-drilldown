// SPDX-License-Identifier: AGPL-3.0-only
package httpapi

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type readinessFunc func(context.Context) error

func (f readinessFunc) Ready(ctx context.Context) error { return f(ctx) }

func TestReadinessIsNotCannedHealth(t *testing.T) {
	for _, ready := range []bool{false, true} {
		called := false
		handler := New(readinessFunc(func(ctx context.Context) error {
			called = true
			if _, ok := ctx.Deadline(); !ok {
				t.Fatal("missing request deadline")
			}
			if !ready {
				return errors.New("private database error with password")
			}
			return nil
		}), "dev")
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest("GET", "/ready", nil))
		want := http.StatusServiceUnavailable
		if ready {
			want = http.StatusOK
		}
		if !called || response.Code != want || strings.Contains(response.Body.String(), "password") {
			t.Fatalf("invalid readiness: %d %s", response.Code, response.Body.String())
		}
	}
}

func TestUnsupportedEndpointsNeverSucceed(t *testing.T) {
	handler := New(readinessFunc(func(context.Context) error { return nil }), "dev")
	for _, path := range []string{
		"/loki/api/v1/query", "/loki/api/v1/query_range", "/loki/api/v1/labels",
		"/loki/api/v1/patterns", "/loki/api/v1/push", "/loki/api/v1/tail", "/loki/api/v1/rules", "/unknown",
	} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest("GET", path, nil))
		if response.Code != http.StatusNotImplemented || !strings.Contains(response.Body.String(), `"status":"error"`) {
			t.Fatalf("unsupported endpoint succeeded: %s %d", path, response.Code)
		}
	}
}

func TestTenantRoutingRejected(t *testing.T) {
	handler := New(readinessFunc(func(context.Context) error {
		t.Fatal("tenant request reached database")
		return nil
	}), "dev")
	request := httptest.NewRequest("GET", "/ready", nil)
	request.Header.Set("X-Scope-OrgID", "tenant")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest {
		t.Fatal("tenant routing accepted")
	}
}
