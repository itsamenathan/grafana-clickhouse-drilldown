// SPDX-License-Identifier: AGPL-3.0-only
package httpapi

import (
	"errors"
	"fmt"
	"mime"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// Timestamp accepts Loki integer nanoseconds, exact decimal seconds and RFC3339.
// Never pass nanosecond epochs through float64.
func timestamp(value string, fallback int64) (int64, error) {
	if value == "" {
		return fallback, nil
	}
	if len(value) > 64 {
		return 0, errors.New("invalid timestamp")
	}
	if t, err := time.Parse(time.RFC3339Nano, value); err == nil {
		ns := t.UnixNano()
		if ns < 0 || !time.Unix(0, ns).Equal(t) {
			return 0, errors.New("timestamp outside supported range")
		}
		return ns, nil
	}
	if strings.Contains(value, ".") {
		d, err := time.ParseDuration(value + "s")
		if err != nil || d < 0 {
			return 0, errors.New("invalid decimal timestamp")
		}
		return int64(d), nil
	}
	n, err := strconv.ParseInt(value, 10, 64)
	if err != nil || n < 0 {
		return 0, errors.New("invalid nanosecond timestamp")
	}
	return n, nil
}

func duration(value string, fallback time.Duration) (time.Duration, error) {
	return durationLimit(value, fallback, 0)
}

func durationLimit(value string, fallback, maximum time.Duration) (time.Duration, error) {
	if value == "" {
		return fallback, nil
	}
	if len(value) > 32 {
		return 0, errors.New("invalid duration")
	}
	d, err := time.ParseDuration(value)
	if err != nil {
		d, err = time.ParseDuration(value + "s")
	}
	if err != nil || d <= 0 {
		return 0, errors.New("duration must be positive")
	}
	if maximum > 0 && d > maximum {
		return 0, errors.New("duration must be at most " + maximum.String())
	}
	return d, nil
}

func integer(r *http.Request, name string, fallback, max int) (int, error) {
	value := r.Form.Get(name)
	if value == "" {
		return fallback, nil
	}
	n, err := strconv.Atoi(value)
	if err != nil || n < 1 || n > max {
		return 0, errors.New("invalid or excessive " + name)
	}
	return n, nil
}

func (a *queryAPI) bounds(r *http.Request) (int64, int64, error) {
	return boundsLimit(r, a.limits.MaxQueryRange)
}

func boundsLimit(r *http.Request, maximum time.Duration) (int64, int64, error) {
	now := time.Now().UnixNano()
	end, err := timestamp(r.Form.Get("end"), now)
	if err != nil {
		return 0, 0, err
	}
	start, err := timestamp(r.Form.Get("start"), max(0, end-int64(time.Hour)))
	if err != nil {
		return 0, 0, err
	}
	if start < 0 || end < start {
		return 0, 0, errors.New("time range must be non-negative and ordered")
	}
	if maximum > 0 && end-start > int64(maximum) {
		return 0, 0, fmt.Errorf("time range exceeds configured maximum %s", maximum)
	}
	return start, end, nil
}

func parameters(r *http.Request, allowed string) error {
	if r.Method != http.MethodGet && r.Method != http.MethodPost {
		return errors.New("only GET and form-encoded POST are supported")
	}
	if r.Method == http.MethodPost {
		mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
		if err != nil || mediaType != "application/x-www-form-urlencoded" {
			return errors.New("POST requires application/x-www-form-urlencoded")
		}
	}
	if len(r.URL.RawQuery) > 32768 {
		return errors.New("request parameters exceed size limit")
	}
	if err := r.ParseForm(); err != nil {
		return errors.New("invalid request parameters")
	}
	names := map[string]bool{}
	for _, name := range strings.Fields(allowed) {
		names[name] = true
	}
	for name, values := range r.Form {
		if !names[name] {
			return errors.New("unsupported request parameter")
		}
		if name != "match[]" && len(values) != 1 {
			return errors.New("repeated singleton request parameter")
		}
		for _, value := range values {
			if len(value) > 16384 {
				return errors.New("request parameter exceeds size limit")
			}
		}
	}
	return nil
}
