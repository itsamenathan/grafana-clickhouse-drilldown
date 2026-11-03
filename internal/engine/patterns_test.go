// SPDX-License-Identifier: AGPL-3.0-only
package engine

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"reflect"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/grafana/loki/v3/pkg/pattern/drain"
	"github.com/grafana/loki/v3/pkg/util/constants"
	"github.com/prometheus/common/model"

	"grafana-clickhouse-drilldown/internal/record"
	"grafana-clickhouse-drilldown/internal/sqlgen"
)

func TestRecentPatternsTrainAscendingAndKeepCompleteCutoff(t *testing.T) {
	store := &fakeStore{}
	base := int64(1767225600) * int64(time.Second)
	for i := 0; i < 100; i++ {
		store.rows = append(store.rows, record.Raw{Timestamp: base + int64(i)*int64(time.Second), Body: fmt.Sprintf("request %d completed for user %d", i, i*7), Service: "api", Level: "info"})
	}
	result, err := New(store).PatternsRecent(context.Background(), source(t, `{service_name="api"}`), base, base+int64(100*time.Second), int64(10*time.Second), PatternOptions{Lines: 60})
	if err != nil {
		t.Fatal(err)
	}
	total := int64(0)
	for _, pattern := range result.Patterns {
		for _, sample := range pattern.Samples {
			total += sample[1]
		}
	}
	if total != 60 || result.CoveredStart != base+int64(40*time.Second) || !result.Sampled || result.Lines != 60 || store.visited != 121 || !store.options[0].Backward || store.options[0].Metadata || store.options[0].Limit != 61 || len(store.options) != 2 || store.options[1].Backward || store.options[1].Metadata || store.options[1].Limit != 121 {
		t.Fatalf("wrong newest-line training: %+v total=%d visited=%d options=%+v", result, total, store.visited, store.options)
	}
}

func TestRecentPatternsExcludeIncompleteTimestampCohort(t *testing.T) {
	store := &fakeStore{}
	base := int64(1767225600) * int64(time.Second)
	for i := 0; i < 100; i++ {
		store.rows = append(store.rows, record.Raw{Timestamp: base + int64(i/10)*int64(time.Second), Body: fmt.Sprintf("request %d completed for user %d", i, i*7), Service: "api", Level: "info"})
	}
	result, err := New(store).PatternsRecent(context.Background(), source(t, `{service_name="api"}`), base, base+int64(10*time.Second), int64(10*time.Second), PatternOptions{Lines: 65})
	if err != nil {
		t.Fatal(err)
	}
	total := int64(0)
	for _, pattern := range result.Patterns {
		for _, sample := range pattern.Samples {
			total += sample[1]
		}
	}
	if result.CoveredStart != base+int64(3*time.Second)+1 || result.Lines != 60 || total != 60 {
		t.Fatalf("partial cutoff cohort was reported as complete: %+v total=%d", result, total)
	}
}

func TestRecentPatternsKeepRequestedStartWhenComplete(t *testing.T) {
	store := &fakeStore{rows: []record.Raw{{Timestamp: 100, Body: "request 1 completed for user 2", Service: "api", Level: "info"}}}
	result, err := New(store).PatternsRecent(context.Background(), source(t, `{service_name="api"}`), 0, 200, int64(time.Second), PatternOptions{Lines: 1})
	if err != nil || result.Sampled || result.CoveredStart != 0 || result.Lines != 1 || result.Patterns == nil {
		t.Fatalf("complete short selection: %+v %v", result, err)
	}
}

func TestRecentPatternsAccountForDrainModelMemory(t *testing.T) {
	store := &fakeStore{rows: []record.Raw{{Timestamp: 100, Body: "request 1 completed for user 2", Service: "api", Level: "info"}}}
	_, err := New(store).PatternsRecent(context.Background(), source(t, `{service_name="api"}`), 0, 200, int64(time.Second), PatternOptions{RetainedBytes: 1000})
	if !errors.Is(err, ErrRetainedLimit) {
		t.Fatalf("model exceeded memory budget: %v", err)
	}
}

func TestRecentPatternsStreamInputUnderModelBudget(t *testing.T) {
	store := &fakeStore{}
	body := "request 1 completed for user 7 " + strings.Repeat("x", 2500)
	for i := 0; i < 50000; i++ {
		store.rows = append(store.rows, record.Raw{Timestamp: int64(i), Body: body, Service: "api", Level: "info"})
	}
	// Over 125 MB of bodies must not occupy the 512 KiB model budget.
	result, err := New(store).PatternsRecent(context.Background(), source(t, `{service_name="api"}`), 0, 50000, int64(time.Second), PatternOptions{RetainedBytes: 512 << 10})
	if err != nil || result.Lines != 50000 || len(result.Patterns) != 1 || result.Patterns[0].Samples[0][1] != 50000 {
		t.Fatalf("input was retained or training was lost: %+v %v", result, err)
	}
	for _, options := range store.options {
		if options.Metadata {
			t.Fatal("pattern scan transferred metadata")
		}
	}
}

func TestPatternSampleChunkAllowance(t *testing.T) {
	detector := drain.New("", drain.DefaultConfig(), tokenizableFields{}, drain.FormatUnknown, nil)
	cluster := detector.Train("request 1 completed for user 2", 0)
	chunk := cluster.Chunks[0]
	// The default 361 x 16-byte samples round to a 6 KiB Go allocation.
	// Include doubling of the outer slice capacity as new chunks arrive.
	const patternSampleChunkBytes = 6 << 10
	bytes := cap(chunk.Samples)*int(reflect.TypeOf(chunk.Samples[0]).Size()) + 2*int(reflect.TypeOf(chunk).Size())
	if bytes > patternSampleChunkBytes {
		t.Fatalf("Drain sample allocation %d exceeds allowance %d", bytes, patternSampleChunkBytes)
	}
}

func TestRecentPatternsSparseHistoryUsesChunkCapacity(t *testing.T) {
	store := &fakeStore{}
	for hour := 0; hour < 1000; hour++ {
		for i := 0; i < 3; i++ {
			store.rows = append(store.rows, record.Raw{Timestamp: int64(hour)*int64(time.Hour) + int64(i), Body: "request 1 completed for user 2", Service: "api", Level: "info"})
		}
	}
	// 1000 chunks hold about 5.8 MB of samples, not 64 MB.
	result, err := New(store).PatternsRecent(context.Background(), source(t, `{service_name="api"}`), 0, int64(1000*time.Hour), int64(time.Hour), PatternOptions{RetainedBytes: 16 << 20})
	if err != nil || result.Lines != 3000 || len(result.Patterns) != 1 || len(result.Patterns[0].Samples) != 1000 {
		t.Fatalf("sparse sample storage was overcharged: %+v %v", result, err)
	}
}

// Two small detectors now cost about 30 KiB, rather than the old 256 KiB.
// This budget still distinguishes one detector from two or a large envelope.
func TestRecentPatternsChargeJSONEnvelopeAndSeparateLevels(t *testing.T) {
	for _, largeEnvelope := range []bool{false, true} {
		store := &fakeStore{}
		for i := 0; i < 60; i++ {
			level := "info"
			padding := ""
			if i >= 30 {
				if largeEnvelope {
					padding = strings.Repeat("x", 60000)
				} else {
					level = "error"
				}
			}
			store.rows = append(store.rows, record.Raw{Timestamp: int64(i), Body: fmt.Sprintf(`{"message":"request 1 completed for user 2","padding":%q}`, padding), Service: "api", Level: level})
		}
		_, err := New(store).PatternsRecent(context.Background(), source(t, `{service_name="api"}`), 0, 60, int64(time.Second), PatternOptions{RetainedBytes: 28 << 10})
		if !errors.Is(err, ErrRetainedLimit) {
			t.Fatalf("model or borrowed input escaped accounting, envelope=%v: %v", largeEnvelope, err)
		}
	}
}

func TestRecentPatternsLongLinesKeepDrainSemantics(t *testing.T) {
	for _, format := range []string{"plain", "logfmt", "json", "json-long-message", "utf8"} {
		t.Run(format, func(t *testing.T) {
			store := &fakeStore{}
			for i := 0; i < 30; i++ {
				message := fmt.Sprintf("request %d completed for user %d", i, i*7)
				body := message + strings.Repeat("x", 3001)
				switch format {
				case "logfmt":
					body = "msg=hello user=123 padding=" + strings.Repeat("x", 3001)
				case "json":
					body = fmt.Sprintf(`{"message":%q,"padding":%q}`, message, strings.Repeat("x", 10000))
				case "json-long-message":
					body = fmt.Sprintf(`{"message":%q}`, body)
				case "utf8":
					// Loki checks bytes, so fewer than 3000 runes still fail.
					body = message + strings.Repeat("界", 1100)
				}
				store.rows = append(store.rows, record.Raw{Timestamp: int64(i), Body: body, Service: "api", Level: "info"})
			}
			budget := 512 << 10
			if format == "plain" || format == "logfmt" || format == "utf8" {
				budget = 128 << 10 // Drain still sees rejected lines, without retaining bodies.
			}
			result, err := New(store).PatternsRecent(context.Background(), source(t, `{service_name="api"}`), 0, 30, int64(time.Second), PatternOptions{RetainedBytes: budget})
			if err != nil || result.Lines != 30 || result.Sampled || result.CoveredStart != 0 {
				t.Fatalf("incorrect coverage for skipped input: %+v %v", result, err)
			}
			want := 0
			if format == "json" {
				want = 1
			}
			if len(result.Patterns) != want {
				t.Fatalf("long-line behavior differs from Drain: %+v", result)
			}
		})
	}
}

type changingPatternStore struct {
	fakeStore
	change func(*fakeStore)
}

func (s *changingPatternStore) Scan(ctx context.Context, predicate sqlgen.Predicate, start, end int64, options record.ScanOptions, visit func(record.Raw) error) error {
	if len(s.options) == 1 {
		s.change(&s.fakeStore)
	}
	return s.fakeStore.Scan(ctx, predicate, start, end, options, visit)
}

func TestRecentPatternsTrainRowsArrivingBetweenScans(t *testing.T) {
	row := record.Raw{Timestamp: 1, Body: "request 1 completed for user 2", Service: "api", Level: "info"}
	for name, tc := range map[string]struct {
		change func(*fakeStore)
		lines  int
	}{
		"late arrival": {func(s *fakeStore) { s.rows = append(s.rows, row) }, 2},
		"expired":      {func(s *fakeStore) { s.rows = nil }, 0},
	} {
		store := &changingPatternStore{fakeStore: fakeStore{rows: []record.Raw{row}}, change: tc.change}
		result, err := New(store).PatternsRecent(context.Background(), source(t, `{service_name="api"}`), 0, 10, int64(time.Second), PatternOptions{})
		if err != nil || result.Lines != tc.lines || result.Patterns == nil {
			t.Fatalf("%s: %+v %v", name, result, err)
		}
	}
	// Growth beyond one more line budget is too volatile to stay bounded.
	store := &changingPatternStore{fakeStore: fakeStore{rows: []record.Raw{row}}, change: func(s *fakeStore) { s.rows = append(s.rows, row, row) }}
	result, err := New(store).PatternsRecent(context.Background(), source(t, `{service_name="api"}`), 0, 10, int64(time.Second), PatternOptions{Lines: 1})
	if err == nil || !strings.Contains(err.Error(), "grew too fast") || result.Patterns != nil {
		t.Fatalf("unbounded growth returned success: %+v %v", result, err)
	}
}

func TestRecentPatternsExcludeEntireOversizedCohort(t *testing.T) {
	store := &fakeStore{}
	for i := 0; i < 100; i++ {
		store.rows = append(store.rows, record.Raw{Timestamp: 5, Body: "request 1 completed for user 2", Service: "api", Level: "info"})
	}
	result, err := New(store).PatternsRecent(context.Background(), source(t, `{service_name="api"}`), 0, 10, int64(time.Second), PatternOptions{Lines: 60})
	if err != nil || result.Lines != 0 || result.CoveredStart != 6 || !result.Sampled || result.Patterns == nil || len(result.Patterns) != 0 || len(store.options) != 1 {
		t.Fatalf("oversized timestamp cohort was partially trained: %+v %v", result, err)
	}
}

func TestRecentPatternsMatchCollectedDrainTraining(t *testing.T) {
	base := int64(1767225600) * int64(time.Second)
	store := &fakeStore{}
	for i := 0; i < 400; i++ {
		service := []string{"plain-a", "plain-b", "json", "logfmt"}[i%4]
		message := fmt.Sprintf("request %d completed for user %d", i, i*7)
		if service == "json" {
			message = fmt.Sprintf(`{"message":%q}`, message)
		} else if service == "logfmt" {
			message = fmt.Sprintf("action=request request=%d user=%d result=completed", i, i*7)
		}
		store.rows = append(store.rows, record.Raw{Timestamp: base + int64(i/2)*int64(time.Second), Body: message, Service: service, Level: []string{"info", "error", "custom"}[i%3]})
	}
	for _, limit := range []int{400, 399, 398, 350} {
		result, err := New(store).PatternsRecent(context.Background(), source(t, `{service_name=~".+"}`), base, base+int64(200*time.Second), int64(30*time.Second), PatternOptions{Lines: limit})
		if err != nil {
			t.Fatal(err)
		}
		// The previous algorithm collected newest-first and reversed the slice.
		lines := slices.Clone(store.rows)
		slices.Reverse(lines)
		if len(lines) > limit {
			probe := lines[limit].Timestamp
			lines = lines[:limit]
			for len(lines) > 0 && lines[len(lines)-1].Timestamp == probe {
				lines = lines[:len(lines)-1]
			}
		}
		slices.Reverse(lines)
		// Compare against direct Loki training and sample iteration, without
		// the new cutoff/streaming implementation or its accounting.
		type key struct{ service, level string }
		detectors := map[key]*drain.Drain{}
		formats := map[string]string{}
		for _, line := range lines {
			if _, ok := formats[line.Service]; !ok {
				formats[line.Service] = drain.DetectLogFormat(line.Body)
			}
			level := line.Level
			if !slices.Contains(constants.LogLevels, level) {
				level = constants.LogLevelUnknown
			}
			k := key{line.Service, level}
			if detectors[k] == nil {
				detectors[k] = drain.New("", drain.DefaultConfig(), tokenizableFields{}, formats[line.Service], nil)
			}
			detectors[k].Train(line.Body, line.Timestamp)
		}
		type seriesKey struct{ pattern, level string }
		totals := map[seriesKey]map[int64]int64{}
		for k, detector := range detectors {
			for _, cluster := range detector.Clusters() {
				sk := seriesKey{cluster.String(), k.level}
				if totals[sk] == nil {
					totals[sk] = map[int64]int64{}
				}
				it := cluster.Iterator(k.level, model.Time(base/1e6), model.Time((base+int64(200*time.Second))/1e6), 30000, drain.TimeResolution)
				for it.Next() {
					sample := it.At()
					totals[sk][sample.Timestamp.Unix()] += sample.Value
				}
				if err := it.Err(); err != nil {
					t.Fatal(err)
				}
				it.Close()
			}
		}
		want := map[seriesKey]map[int64]int64{}
		for k, samples := range totals {
			var total int64
			for _, n := range samples {
				total += n
			}
			if total >= minPatternSamples {
				want[k] = samples
			}
		}
		got := map[seriesKey]map[int64]int64{}
		for _, pattern := range result.Patterns {
			k := seriesKey{pattern.Pattern, pattern.Level}
			got[k] = map[int64]int64{}
			for _, sample := range pattern.Samples {
				got[k][sample[0]] = sample[1]
			}
		}
		if result.Lines != len(lines) || !reflect.DeepEqual(got, want) || len(want) == 0 {
			t.Fatalf("limit %d differs from collected training: got=%+v want=%+v lines=%d/%d", limit, got, want, result.Lines, len(lines))
		}
	}
}

// All words in a template differ so Drain's wildcard branches cannot merge
// unrelated templates just because numeric IDs and boilerplate are similar.
func patternTemplate(template, row int) string {
	name := func(n int) string {
		return string([]byte{'a' + byte(n/676%26), 'a' + byte(n/26%26), 'a' + byte(n%26)})
	}
	words := make([]string, 0, 12)
	for i := 0; i < 10; i++ {
		words = append(words, "component"+name(template)+name(i))
	}
	return strings.Join(words, " ") + fmt.Sprintf(" %d %d", row, row*7)
}

func TestRecentPatternsVaried36HoursWithinDefaultBudget(t *testing.T) {
	const lines = 16000
	const span = int64(36 * time.Hour)
	store := &fakeStore{}
	// About 450/hour, short bursts of distinct templates and recurring traffic.
	// Each burst belongs to one of three levels. No detector reaches its LRU
	// limit: eviction refunds cannot fix this old-constant failure.
	old := map[string]*patternDetector{}
	maxBody := 0
	for i := 0; i < lines; i++ {
		level := []string{"info", "warn", "error"}[i/30%3]
		body := patternTemplate(i/30, i)
		if i%4 == 0 {
			body = "request completed successfully 123"
		}
		store.rows = append(store.rows, record.Raw{Timestamp: int64(i) * span / lines, Body: body, Service: "api", Level: level})
		if old[level] == nil {
			old[level] = newPatternDetector(drain.FormatUnknown)
		}
		old[level].train(body, int64(i)*span/lines)
		maxBody = max(maxBody, len(body))
		if i == lines*2/3 {
			// Even an upper bound for the old accounting fits at 24 hours,
			// including its reserve for one more hypothetical cluster.
			upper := 1024 + 64<<10 + 8*maxBody
			for _, d := range old {
				upper += 64<<10 + maxBody
				for cluster := range d.clusters {
					upper += 64<<10 + 8*maxBody + (len(cluster.Chunks)-1)*(8<<10)
				}
			}
			if upper >= 32<<20 {
				t.Fatalf("fixture already exceeds old budget at 24h: %d", upper)
			}
		}
	}
	oldMinimum, modelBytes := 0, 0
	for _, d := range old {
		oldMinimum += len(d.drain.Clusters()) * (64 << 10)
		modelBytes += d.bytes
		if len(d.clusters) >= drain.DefaultConfig().MaxClusters {
			t.Fatal("fixture unexpectedly exercised eviction")
		}
	}
	t.Logf("36h: old cluster-only lower bound=%d; new live model estimate=%d", oldMinimum, modelBytes)
	if oldMinimum <= 32<<20 {
		t.Fatalf("fixture no longer reproduces old model failure: %d", oldMinimum)
	}
	result, err := New(store).PatternsRecent(context.Background(), source(t, `{service_name="api"}`), 0, span, int64(10*time.Minute), PatternOptions{})
	if err != nil || result.Lines != lines || result.Sampled || result.CoveredStart != 0 {
		t.Fatalf("moderately busy varied service: lines=%d patterns=%d sampled=%v start=%d err=%v", result.Lines, len(result.Patterns), result.Sampled, result.CoveredStart, err)
	}
}

func TestRecentPatternsHugeLiveModelFailsExplicitly(t *testing.T) {
	store := &fakeStore{}
	// Only 9,600 lines, but 48 recurring templates each keep 200 hourly chunks.
	for hour := 0; hour < 200; hour++ {
		for template := 0; template < 48; template++ {
			store.rows = append(store.rows, record.Raw{Timestamp: int64(hour)*int64(time.Hour) + int64(template), Body: patternTemplate(template, hour), Service: "api", Level: "info"})
		}
	}
	result, err := New(store).PatternsRecent(context.Background(), source(t, `{service_name="api"}`), 0, int64(200*time.Hour), int64(time.Hour), PatternOptions{})
	if !errors.Is(err, ErrRetainedLimit) || result.Patterns != nil {
		t.Fatalf("oversized live model returned partial success: %+v %v", result, err)
	}
}

// A fresh process per workload excludes fakeStore row copies, other tests'
// heaps, and package initialization. GC before and after measures retained live
// heap, including the accountant itself. KeepAlive prevents dead-code liveness
// from removing the very models being measured. No process/RSS deltas are used.
func TestPatternMemoryCalibration(t *testing.T) {
	workloads := []string{"few_templates_high_volume", "many_templates_ids_levels", "long_json", "long_logfmt", "multi_day_chunks", "eviction_churn", "json_scratch_history"}
	selected := os.Getenv("ADAPTER_TEST_PATTERN_MEMORY")
	if selected == "" {
		for _, workload := range workloads {
			t.Run(workload, func(t *testing.T) {
				command := exec.Command(os.Args[0], "-test.run=^TestPatternMemoryCalibration$", "-test.v")
				command.Env = append(os.Environ(), "ADAPTER_TEST_PATTERN_MEMORY="+workload)
				output, err := command.CombinedOutput()
				t.Logf("%s", output)
				if err != nil {
					t.Fatal(err)
				}
			})
		}
		return
	}
	if !slices.Contains(workloads, selected) {
		t.Fatalf("unknown calibration workload %q", selected)
	}
	// Warm formatting/runtime paths before taking the baseline.
	_ = patternTemplate(5, 7)
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	detectors := make([]*patternDetector, 0, 12)
	for replica := 0; replica < 4; replica++ {
		levels, templates, lines, hours, format := 1, 3, 12000, 6, drain.FormatUnknown
		switch selected {
		case "many_templates_ids_levels":
			levels, templates, lines, hours = 3, 120, 12000, 12
		case "long_json":
			templates, lines, hours, format = 24, 2400, 4, drain.FormatJSON
		case "long_logfmt":
			templates, lines, hours, format = 24, 2400, 4, drain.FormatLogfmt
		case "json_scratch_history":
			templates, lines, hours, format = 75, 75, 1, drain.FormatJSON
		case "multi_day_chunks":
			levels, templates, lines, hours = 3, 12, 6000, 120
		case "eviction_churn":
			levels, templates, lines, hours = 3, 900, 18000, 36
		}
		group := make([]*patternDetector, levels)
		for level := range group {
			group[level] = newPatternDetector(format)
			detectors = append(detectors, group[level])
		}
		for row := 0; row < lines; row++ {
			template := row / levels % templates
			if selected == "eviction_churn" {
				template = row / (levels * 5) % templates
			}
			body := patternTemplate(template, row)
			if selected == "long_json" {
				body = fmt.Sprintf(`{"message":%q,"padding":%q}`, body+" "+strings.Repeat("x", 2200), strings.Repeat("p", 24000+row%1000))
			} else if selected == "json_scratch_history" {
				// Descending token counts leave one older envelope in each unused
				// scratch slot. Sizes just above 32 KiB allocate 40 KiB pages.
				message := strings.TrimSpace(strings.Repeat("word ", 79-row))
				body = fmt.Sprintf(`{"message":%q,"padding":%q}`, message, strings.Repeat("p", 32768))
			} else if selected == "long_logfmt" {
				var builder strings.Builder
				for field := 0; field < 10; field++ {
					fmt.Fprintf(&builder, "key%s_field%d=%q ", strings.Fields(patternTemplate(template, field))[0], field, strings.ReplaceAll(patternTemplate(template, field), " ", "_")+"\nvalue")
				}
				fmt.Fprintf(&builder, "request=%d padding=%s", row, strings.Repeat("x", 400))
				body = builder.String()
			}
			group[row%levels].train(body, int64(row)*int64(time.Duration(hours)*time.Hour)/int64(lines))
		}
	}
	runtime.GC()
	runtime.ReadMemStats(&after)
	accounted, clusters, chunks := 0, 0, 0
	for _, d := range detectors {
		accounted += d.bytes
		for _, cluster := range d.drain.Clusters() {
			clusters++
			chunks += len(cluster.Chunks)
		}
	}
	runtime.KeepAlive(detectors)
	if after.HeapAlloc <= before.HeapAlloc {
		t.Fatalf("invalid heap delta: before=%d after=%d", before.HeapAlloc, after.HeapAlloc)
	}
	actual := int(after.HeapAlloc - before.HeapAlloc)
	ratio := float64(accounted) / float64(actual)
	t.Logf("%s: accounted=%d actual=%d ratio=%.3f clusters=%d chunks=%d", selected, accounted, actual, ratio, clusters, chunks)
	if accounted < actual || ratio > 2.05 {
		t.Fatalf("uncalibrated live memory estimate: ratio=%.3f", ratio)
	}
}

func TestPatternScratchAccountsForOlderJSONBodiesAndRefundsThem(t *testing.T) {
	d := newPatternDetector(drain.FormatJSON)
	message := strings.TrimSpace(strings.Repeat("word ", 75))
	large := fmt.Sprintf(`{"message":%q,"padding":%q}`, message, strings.Repeat("p", 60000))
	d.train(large, 0)
	small := `{"message":"request completed successfully 123"}`
	d.train(small, 1)
	// Train resets slice length, but the unused slots still alias large.
	if len(d.bodies) != 2 || d.scratchBytes < len(large)+len(small) {
		t.Fatalf("old JSON envelope escaped accounting: bodies=%+v scratch=%d", d.bodies, d.scratchBytes)
	}
	before := d.scratchBytes
	d.train(fmt.Sprintf(`{"message":%q}`, message), 2)
	if len(d.bodies) != 1 || before-d.scratchBytes < 60000 {
		t.Fatalf("overwritten aliases were retained: bodies=%+v before=%d after=%d", d.bodies, before, d.scratchBytes)
	}
}

func TestPatternEvictionRefundKeepsTreeCharges(t *testing.T) {
	d := newPatternDetector(drain.FormatUnknown)
	for i := 0; i < 600; i++ {
		d.train(patternTemplate(i, i), int64(i))
	}
	if len(d.clusters) != drain.DefaultConfig().MaxClusters {
		t.Fatalf("wrong live cluster count: %d", len(d.clusters))
	}
	live := d.drain.Clusters()
	for cluster := range d.clusters {
		if !slices.Contains(live, cluster) {
			t.Fatal("evicted cluster retained by accountant")
		}
	}
	if d.treeBytes == 0 || d.bytes < d.treeBytes+d.scratchBytes+patternDetectorBytes {
		t.Fatalf("eviction refunded surviving tree or scratch: %+v", d)
	}
}
