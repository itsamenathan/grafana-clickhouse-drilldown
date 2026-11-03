// SPDX-License-Identifier: AGPL-3.0-only
package engine

import (
	"context"
	"errors"
	"math"
	"reflect"
	"slices"
	"sort"
	"strconv"
	"unsafe"

	"github.com/grafana/loki/v3/pkg/pattern/drain"
	"github.com/grafana/loki/v3/pkg/util/constants"
	"github.com/prometheus/common/model"

	"grafana-clickhouse-drilldown/internal/record"
)

// Loki v3.7.9 pattern querier thresholds (pkg/pattern/ingester_querier.go):
// patterns with fewer than 30 samples in range are dropped, then the 300
// largest are kept.
const (
	minPatternSamples = 30
	maxPatterns       = 300
	maxPatternStreams = 500
)

// Loki's default pattern_ingester_tokenizable_json_fields_default limit.
type tokenizableFields struct{}

func (tokenizableFields) PatternIngesterTokenizableJSONFields(string) []string {
	return []string{"log", "message", "msg", "msg_", "_msg", "content"}
}

type Pattern struct {
	Pattern string     `json:"pattern"`
	Level   string     `json:"level"`
	Samples [][2]int64 `json:"samples"`
}

type patternStream struct {
	format string
	drains map[string]*patternDetector
}

// patternDetector accounts for the pinned Drain implementation, not input volume.
// Cluster tokenizers Clone every token string and the []int TokenState. Only
// tokenizer scratch aliases the input body (including JSON envelopes).
// Tree nodes are private and Train never prunes them. Charge each newly created
// path node, including strings that tree keys keep after cluster eviction.
type patternDetector struct {
	drain                          *drain.Drain
	clusters                       map[*drain.LogCluster]int
	bytes, treeBytes, scratchBytes int
	tokens, state, decoderLine     reflect.Value
	bodies                         []patternBody
	root                           reflect.Value
	nodes                          map[uintptr]int
	decoderKey, decoderValue       reflect.Value
}

const patternDetectorBytes = 4096 // Drain/config/tokenizer/root/limiter, cache/map headers

// Go 1.26 small size classes can waste nearly 25% at a boundary. Include
// that slack and object headers; larger objects round to 8 KiB heap pages.
func patternAllocation(bytes int) int {
	if bytes == 0 {
		return 0
	}
	if bytes > 32760 {
		return (bytes + 8 + 8191) &^ 8191
	}
	return (bytes + bytes/4 + 31) &^ 15
}

// patternBody records an input allocation without keeping it alive itself.
// Scratch's unused token slots can retain several older JSON envelopes, not just
// the latest or longest body. Drain owns those references while they are live.
type patternBody struct {
	start uintptr
	size  int
	used  bool
}

func comparePatternBody(b patternBody, pointer uintptr) int {
	if b.start < pointer {
		return -1
	}
	if b.start > pointer {
		return 1
	}
	return 0
}

func newPatternDetector(format string) *patternDetector {
	d := &patternDetector{
		drain:    drain.New("", drain.DefaultConfig(), tokenizableFields{}, format, nil),
		clusters: map[*drain.LogCluster]int{},
		nodes:    map[uintptr]int{},
		bytes:    patternDetectorBytes,
	}
	// Read-only reflection inspects the pinned implementation's private
	// scratch slices and prefix-tree paths. Public Clusters exposes cloned
	// tokens, not these aliases.
	// No unsafe field access/mutation or copy of Drain's tokenizer algorithm.
	v := reflect.ValueOf(d.drain).Elem()
	d.tokens, d.state = v.FieldByName("tokens"), v.FieldByName("state")
	d.root = v.FieldByName("rootNode")
	if format == drain.FormatLogfmt {
		decoder := v.FieldByName("tokenizer").Elem().Elem().FieldByName("LineTokenizer").Elem().Elem().FieldByName("dec").Elem()
		d.decoderLine = decoder.FieldByName("line")
		d.decoderKey, d.decoderValue = decoder.FieldByName("key"), decoder.FieldByName("value")
	}
	return d
}

func (d *patternDetector) scratch(body string) int {
	if len(body) > 0 {
		start := uintptr(unsafe.Pointer(unsafe.StringData(body)))
		i, found := slices.BinarySearchFunc(d.bodies, start, comparePatternBody)
		if found {
			d.bodies[i].size = max(d.bodies[i].size, len(body))
		} else {
			d.bodies = slices.Insert(d.bodies, i, patternBody{start: start, size: len(body)})
		}
	}
	bytes := patternAllocation(d.tokens.Cap() * 16)
	if !d.state.IsNil() {
		bytes += 32 + patternAllocation(d.state.Elem().Cap()*int(unsafe.Sizeof(int(0))))
	}
	charge := func(pointer uintptr, length int) {
		if length == 0 {
			return
		}
		i, _ := slices.BinarySearchFunc(d.bodies, pointer, comparePatternBody)
		if i == len(d.bodies) || d.bodies[i].start > pointer {
			i--
		}
		if i >= 0 && pointer-d.bodies[i].start < uintptr(d.bodies[i].size) {
			d.bodies[i].used = true
		} else {
			// Independently allocated unescaped logfmt values, or ParamString.
			bytes += patternAllocation(length)
		}
	}
	tokens := d.tokens.Slice(0, d.tokens.Cap())
	for i := 0; i < tokens.Len(); i++ {
		token := tokens.Index(i).String()
		charge(uintptr(unsafe.Pointer(unsafe.StringData(token))), len(token))
	}
	if d.decoderLine.IsValid() {
		charge(d.decoderLine.Pointer(), d.decoderLine.Len())
		charge(d.decoderKey.Pointer(), d.decoderKey.Len())
		charge(d.decoderValue.Pointer(), d.decoderValue.Len())
	}
	live := d.bodies[:0]
	for _, allocation := range d.bodies {
		if allocation.used {
			bytes += patternAllocation(allocation.size)
			allocation.used = false
			live = append(live, allocation)
		}
	}
	d.bodies = live
	return bytes + patternAllocation(cap(d.bodies)*int(unsafe.Sizeof(patternBody{})))
}

func patternClusterBytes(cluster *drain.LogCluster) int {
	// LogCluster itself, Stringer closure, LRU entry, LRU and accounting map
	// entries (including spare capacity). These maps grow with live clusters;
	// simplelru.NewLRU does not preallocate MaxClusters entries.
	bytes := 384 + patternAllocation(cap(cluster.Tokens)*int(unsafe.Sizeof("")))
	for _, token := range cluster.Tokens {
		bytes += patternAllocation(len(token))
	}
	if state, ok := cluster.TokenState.([]int); ok {
		bytes += 32 + patternAllocation(cap(state)*int(unsafe.Sizeof(int(0))))
	}
	bytes += patternAllocation(cap(cluster.Chunks) * int(unsafe.Sizeof(drain.Chunk{})))
	for _, chunk := range cluster.Chunks {
		// 361 x 16 bytes by default, allocated even for a single sample.
		bytes += patternAllocation(cap(chunk.Samples) * int(unsafe.Sizeof(chunk.Samples[0])))
	}
	return bytes
}

// Follow the path Train just created, using read-only views of the private
// Nodes. Token-count grouping and exact-token / ParamString lookup are the
// same as treeSearch. This avoids charging shared wildcard paths repeatedly,
// without maintaining a second copy of Drain's insertion algorithm or tree.
func (d *patternDetector) chargeTreePath(cluster *drain.LogCluster) int {
	config := drain.DefaultConfig()
	node := d.root
	bytes := 0
	depth := min(len(cluster.Tokens), config.LogClusterDepth-2)
	for i := 0; i < depth; i++ {
		key := strconv.Itoa(len(cluster.Tokens))
		if i > 0 {
			key = cluster.Tokens[i-1]
		}
		children := node.Elem().FieldByName("keyToChildNode")
		child := children.MapIndex(reflect.ValueOf(key))
		if !child.IsValid() {
			key = config.ParamString
			child = children.MapIndex(reflect.ValueOf(key))
		}
		node = child
		pointer := node.Pointer()
		previous, seen := d.nodes[pointer]
		if !seen {
			// Node (32), map header (48), small map group (208), spare
			// table capacity, plus this accountant's node-map entry (48).
			bytes += 368 + patternAllocation(len(key))
		}
		ids := patternAllocation(node.Elem().FieldByName("clusterIDs").Cap() * int(unsafe.Sizeof(int(0))))
		bytes += ids - previous
		d.nodes[pointer] = ids
	}
	d.treeBytes += bytes
	return bytes
}

func (d *patternDetector) train(body string, timestamp int64) int {
	before := d.bytes
	config := drain.DefaultConfig()
	cluster := d.drain.Train(body, timestamp)
	scratch := d.scratch(body)
	d.bytes += scratch - d.scratchBytes
	d.scratchBytes = scratch
	if cluster != nil {
		old, known := d.clusters[cluster]
		if !known {
			d.bytes += d.chargeTreePath(cluster)
		}
		size := patternClusterBytes(cluster)
		d.bytes += size - old
		d.clusters[cluster] = size
		if len(d.clusters) > config.MaxClusters {
			// At most 301 entries. Drop strong references as well as the charge.
			live := d.drain.Clusters()
			for candidate, bytes := range d.clusters {
				if !slices.Contains(live, candidate) {
					d.bytes -= bytes
					delete(d.clusters, candidate)
				}
			}
		}
	}
	return d.bytes - before
}

// PatternOptions bounds the newest-line training set and retained Drain
// models/output. Input is streamed. Zero values use request defaults.
type PatternOptions struct {
	Lines         int
	RetainedBytes int
}

type PatternResult struct {
	Patterns     []Pattern
	CoveredStart int64
	Sampled      bool
	Lines        int
}

// Patterns keeps the engine's original API for embedded callers.
func (e *Engine) Patterns(ctx context.Context, source Source, start, end, step int64) ([]Pattern, error) {
	result, err := e.PatternsRecent(ctx, source, start, end, step, PatternOptions{})
	return result.Patterns, err
}

// PatternsRecent finds the newest-line cutoff without retaining input, then
// streams that complete interval ascending. Drain drops out-of-order samples.
// Exclude an incomplete cutoff cohort rather than claiming full coverage.
func (e *Engine) PatternsRecent(ctx context.Context, source Source, start, end, step int64, options PatternOptions) (PatternResult, error) {
	if options.Lines == 0 {
		options.Lines = 50000
	}
	if options.RetainedBytes == 0 {
		options.RetainedBytes = e.retainedBytes
	}
	if start < 0 || end < start || step <= 0 || options.Lines < 1 || options.Lines == math.MaxInt || options.RetainedBytes < 1 {
		return PatternResult{}, errors.New("invalid pattern bounds or budgets")
	}
	result := PatternResult{CoveredStart: start}
	count, cohort := 0, 0
	var cutoff int64
	err := e.Each(ctx, source, start, end, record.ScanOptions{Backward: true, Limit: options.Lines + 1, EdgeWindows: true}, func(event Event) error {
		if count == options.Lines {
			result.Sampled = true
			result.CoveredStart = cutoff
			if event.Timestamp == cutoff {
				result.CoveredStart++
				count -= cohort
			}
			return record.ErrStop
		}
		if count == 0 || event.Timestamp != cutoff {
			cutoff, cohort = event.Timestamp, 0
		}
		cohort++
		count++
		return nil
	})
	if err != nil {
		return PatternResult{}, err
	}
	retained := 0
	streams := map[string]*patternStream{}
	if count > 0 {
		// Rows still arriving inside the covered window between the two scans
		// are genuine data, so train on them too. Bound that growth to one more
		// line budget; beyond it the selection is too volatile to stay bounded.
		maxLines := count + options.Lines
		err = e.Each(ctx, source, result.CoveredStart, end, record.ScanOptions{Limit: maxLines + 1}, func(event Event) error {
			if result.Lines == maxLines {
				return errors.New("pattern selection grew too fast during detection; retry the request")
			}
			result.Lines++
			key := Key(event.Stream)
			stream := streams[key]
			if stream == nil {
				if len(streams) >= maxPatternStreams {
					return errors.New("too many streams for pattern detection; narrow the selector")
				}
				if 1024+len(key) > options.RetainedBytes-retained {
					return ErrRetainedLimit
				}
				retained += 1024 + len(key)
				stream = &patternStream{format: drain.DetectLogFormat(event.Body), drains: map[string]*patternDetector{}}
				streams[key] = stream
			}
			level := event.Metadata[constants.LevelLabel]
			if !slices.Contains(constants.LogLevels, level) {
				level = constants.LogLevelUnknown
			}
			detector := stream.drains[level]
			if detector == nil {
				if patternDetectorBytes > options.RetainedBytes-retained {
					return ErrRetainedLimit
				}
				retained += patternDetectorBytes
				detector = newPatternDetector(stream.format)
				stream.drains[level] = detector
			}
			// Inspect actual capacities after each bounded Train mutation. This
			// avoids reserving a new cluster on every match near the budget. One
			// call can add only one path/cluster/chunk; failure releases the model.
			retained += detector.train(event.Body, event.Timestamp)
			if retained > options.RetainedBytes {
				return ErrRetainedLimit
			}
			return nil
		})
		if err != nil {
			return PatternResult{}, err
		}
	}

	// Match Loki's querier: millisecond bounds, rounding the end up, and a
	// step no finer than Drain's ten-second sample resolution.
	// Drain timestamps label ten-second buckets. Include the first partial
	// bucket; its count contains only the collected records.
	from := model.Time(result.CoveredStart/1e6) / drain.TimeResolution * drain.TimeResolution
	through := model.Time(end / 1e6)
	if end%1e6 != 0 {
		through++
	}
	resolution := max(model.Time(step/1e6), drain.TimeResolution)
	interval := model.Time(drain.DefaultConfig().SampleInterval.Milliseconds())
	type seriesKey struct{ pattern, level string }
	totals := map[seriesKey]map[int64]int64{}
	for _, stream := range streams {
		for level, detector := range stream.drains {
			for _, cluster := range detector.drain.Clusters() {
				if err := ctx.Err(); err != nil {
					return PatternResult{}, err
				}
				pattern := cluster.String()
				if pattern == "" {
					continue
				}
				it := cluster.Iterator(level, from, through, resolution, interval)
				for it.Next() {
					if err := ctx.Err(); err != nil {
						it.Close()
						return PatternResult{}, err
					}
					key := seriesKey{pattern, level}
					if totals[key] == nil {
						if size := 2*len(pattern) + 256; size > options.RetainedBytes-retained {
							it.Close()
							return PatternResult{}, ErrRetainedLimit
						} else {
							retained += size
						}
						totals[key] = map[int64]int64{}
					}
					sample := it.At()
					timestamp := sample.Timestamp.Unix()
					if _, exists := totals[key][timestamp]; !exists {
						if retained > options.RetainedBytes-128 {
							it.Close()
							return PatternResult{}, ErrRetainedLimit
						}
						retained += 128
					}
					totals[key][timestamp] += sample.Value
				}
				if err := it.Err(); err != nil {
					it.Close()
					return PatternResult{}, err
				}
				it.Close()
			}
		}
		if err := ctx.Err(); err != nil {
			return PatternResult{}, err
		}
	}
	type ranked struct {
		Pattern
		total int64
	}
	var rankedPatterns []ranked
	for key, samples := range totals {
		if err := ctx.Err(); err != nil {
			return PatternResult{}, err
		}
		series := ranked{Pattern: Pattern{Pattern: key.pattern, Level: key.level}}
		for timestamp, value := range samples {
			series.Samples = append(series.Samples, [2]int64{timestamp, value})
			series.total += value
		}
		if series.total < minPatternSamples {
			continue
		}
		sort.Slice(series.Samples, func(i, j int) bool { return series.Samples[i][0] < series.Samples[j][0] })
		rankedPatterns = append(rankedPatterns, series)
	}
	sort.Slice(rankedPatterns, func(i, j int) bool {
		if rankedPatterns[i].total != rankedPatterns[j].total {
			return rankedPatterns[i].total > rankedPatterns[j].total
		}
		if rankedPatterns[i].Pattern.Pattern != rankedPatterns[j].Pattern.Pattern {
			return rankedPatterns[i].Pattern.Pattern < rankedPatterns[j].Pattern.Pattern
		}
		return rankedPatterns[i].Level < rankedPatterns[j].Level
	})
	patterns := make([]Pattern, 0, min(len(rankedPatterns), maxPatterns))
	for i := 0; i < len(rankedPatterns) && i < maxPatterns; i++ {
		patterns = append(patterns, rankedPatterns[i].Pattern)
	}
	result.Patterns = patterns
	return result, nil
}
