// SPDX-License-Identifier: AGPL-3.0-only
package main

import (
	"math"
	"strconv"
	"testing"
	"time"
)

func TestParseConfigDefaultsAndOverrides(t *testing.T) {
	config, err := parseConfig(func(string) string { return "" })
	if err != nil || config.Limits.MaxQueryRange != 0 || config.Limits.RequestTimeout != 15*time.Second || config.Limits.TailLookback != time.Hour || config.Limits.PatternLines != 50000 || config.Limits.MaxRetainedBytes != 32<<20 || config.MaxScanRows != 200000 || config.MaxScanBytes != 256<<20 {
		t.Fatalf("unexpected defaults: %+v %v", config, err)
	}
	values := map[string]string{"ADAPTER_MAX_QUERY_RANGE": "721h", "ADAPTER_REQUEST_TIMEOUT": "30s", "ADAPTER_TAIL_LOOKBACK": "2h", "ADAPTER_PATTERN_LINES": "100000", "ADAPTER_MAX_RETAINED_BYTES": "67108864", "ADAPTER_MAX_SCAN_ROWS": "1000000", "ADAPTER_MAX_SCAN_BYTES": "536870912"}
	config, err = parseConfig(func(name string) string { return values[name] })
	if err != nil || config.Limits.MaxQueryRange != 721*time.Hour || config.Limits.RequestTimeout != 30*time.Second || config.Limits.TailLookback != 2*time.Hour || config.Limits.PatternLines != 100000 || config.Limits.MaxRetainedBytes != 64<<20 || config.MaxScanRows != 1000000 || config.MaxScanBytes != 512<<20 {
		t.Fatalf("overrides lost: %+v %v", config, err)
	}
	for _, zero := range []string{"0", "0s"} {
		if _, err := parseConfig(func(name string) string {
			if name == "ADAPTER_MAX_QUERY_RANGE" {
				return zero
			}
			return ""
		}); err != nil {
			t.Fatalf("zero must disable the time cap: %v", err)
		}
	}
}

func TestParseConfigRejectsInvalidLimits(t *testing.T) {
	for name, values := range map[string][]string{
		"ADAPTER_MAX_QUERY_RANGE":    {"-1s", "1d", "garbage", "999999999999999999h"},
		"ADAPTER_REQUEST_TIMEOUT":    {"0", "-1s", "15", "2562047h47m16s"},
		"ADAPTER_TAIL_LOOKBACK":      {"0s", "-1h"},
		"ADAPTER_PATTERN_LINES":      {"0", "-1", "1.5", strconv.Itoa(math.MaxInt)},
		"ADAPTER_MAX_RETAINED_BYTES": {"0", "-1", "32MiB"},
		"ADAPTER_MAX_SCAN_ROWS":      {"0", "-1", "18446744073709551615"},
		"ADAPTER_MAX_SCAN_BYTES":     {"0", "-1", "256MiB"},
	} {
		for _, raw := range values {
			t.Run(name+"="+raw, func(t *testing.T) {
				_, err := parseConfig(func(key string) string {
					if key == name {
						return raw
					}
					return ""
				})
				if err == nil {
					t.Fatal("invalid configuration accepted")
				}
			})
		}
	}
}
