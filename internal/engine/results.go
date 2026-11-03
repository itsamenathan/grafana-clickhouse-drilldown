// SPDX-License-Identifier: AGPL-3.0-only
package engine

import (
	"context"
	"errors"
	"grafana-clickhouse-drilldown/internal/sqlgen"
	"math"
	"sort"
	"strconv"

	"github.com/grafana/loki/v3/pkg/logql/log"
	"github.com/prometheus/prometheus/model/labels"

	"grafana-clickhouse-drilldown/internal/query"
	"grafana-clickhouse-drilldown/internal/record"
)

type Stream struct {
	Labels map[string]string `json:"stream"`
	Values [][]any           `json:"values"`
}

// Streams applies the requested global limit before grouping into streams.
// Exact duplicate events remain separate. Equal timestamps use the
// deterministic complete-row order established by the storage query,
// reversed for backward. Timestamp-only pagination with a tie larger than the
// limit is unsupported; clients must not infer a unique event cursor from a
// timestamp.
func Streams(events []Event, direction string, limit int, categorize bool) []Stream {
	result := []Stream{}
	indices := map[string]int{}
	for n := 0; n < len(events) && n < limit; n++ {
		i := n
		if direction == "backward" {
			i = len(events) - 1 - n
		}
		event := events[i]
		labels := event.Stream
		if !categorize {
			labels = event.Labels()
		}
		key := Key(labels)
		index, exists := indices[key]
		if !exists {
			index = len(result)
			indices[key] = index
			result = append(result, Stream{labels, [][]any{}})
		}
		value := []any{strconv.FormatInt(event.Timestamp, 10), event.Body}
		if categorize {
			categories := map[string]any{}
			if len(event.Metadata) != 0 {
				categories["structuredMetadata"] = event.Metadata
			}
			if len(event.Parsed) != 0 {
				categories["parsed"] = event.Parsed
			}
			value = append(value, categories)
		}
		result[index].Values = append(result[index].Values, value)
	}
	sort.Slice(result, func(i, j int) bool { return Key(result[i].Labels) < Key(result[j].Labels) })
	return result
}

type Series struct {
	Metric map[string]string `json:"metric"`
	Values [][]any           `json:"values,omitempty"`
	Value  []any             `json:"value,omitempty"`
}

// metricSample is the only per-record state a metric query retains. Its value
// is the line's byte length, or the unwrapped label value. Grouped averages
// also retain count-only samples with count=0 and meanCount=1, since Loki's
// denominator can accept rows that the numeric extractor rejects.
type metricSample struct {
	timestamp int64
	value     float64
	count     uint64
	meanCount uint64
	group     int32
	errored   bool
}

type metricGroup struct {
	labels      map[string]string
	count       uint64
	sum         float64
	meanCount   uint64
	meanSamples float64
}

// maxUnwrapWork bounds sample visits when unwrapped windows are recomputed.
const maxUnwrapWork = 20_000_000

// metricAccumulator reduces streamed events to compact samples in ascending
// time order, grouped by their output labels.
type metricAccumulator struct {
	metric           query.Metric
	wanted           map[string]bool
	keys             map[string]int32
	groups           []*metricGroup
	samples          []metricSample
	retained, budget int
	sumAndCount      bool
}

func newMetricAccumulator(metric query.Metric) (*metricAccumulator, error) {
	lines := metric.Function == "count_over_time" || metric.Function == "rate" || metric.Function == "bytes_over_time" || metric.Function == "bytes_rate"
	unwrapped := metric.Function == "avg_over_time" || metric.Function == "sum_over_time"
	if metric.Window <= 0 || !(lines && !metric.Unwrap() || unwrapped && metric.Unwrap()) {
		return nil, errors.New("unsupported metric plan")
	}
	wanted := map[string]bool{}
	for _, name := range metric.Groups {
		wanted[name] = true
	}
	return &metricAccumulator{metric: metric, wanted: wanted, keys: map[string]int32{}, budget: MaxRetainedBytes, sumAndCount: metric.AverageUsesSumAndCount()}, nil
}

func (a *metricAccumulator) add(event Event) error {
	labels := event.Labels()
	if a.metric.Sum {
		for key := range labels {
			if a.wanted[key] == a.metric.Without {
				delete(labels, key)
			}
		}
	}
	return a.addSample(event.Timestamp, labels, float64(len(event.Body)), event.Parsed["__error__"] != "")
}

// addSample records one sample under its final output labels.
func (a *metricAccumulator) addSample(timestamp int64, labels map[string]string, value float64, errored bool) error {
	key := Key(labels)
	group, found := a.keys[key]
	if !found {
		if len(a.groups) >= 1000 {
			return errors.New("too many metric series; add grouping or narrow the selection")
		}
		size := len(key) + 128
		for name, value := range labels {
			size += len(name) + len(value) + 64
		}
		if size > a.budget-a.retained {
			return ErrRetainedLimit
		}
		a.retained += size
		group = int32(len(a.groups))
		a.keys[key] = group
		a.groups = append(a.groups, &metricGroup{labels: labels})
	}
	if n := len(a.samples); n > 0 && a.samples[n-1].timestamp > timestamp {
		return errors.New("metric events must arrive in ascending time order")
	}
	if 48 > a.budget-a.retained {
		return ErrRetainedLimit
	}
	a.retained += 48
	a.samples = append(a.samples, metricSample{timestamp: timestamp, value: value, count: 1, group: group, errored: errored})
	return nil
}

// Metric uses exact SQL buckets when its pipeline, labels and evaluation grid
// permit it. Otherwise it streams the selection once, omitting metadata only
// when neither the residual stages nor output labels need it.
func (e *Engine) Metric(ctx context.Context, source Source, metric query.Metric, start, end, step int64, instant bool) ([]Series, error) {
	accumulator, err := newMetricAccumulator(metric)
	if err != nil {
		return nil, err
	}
	accumulator.budget = e.retainedBytes
	if start < int64(metric.Window) || end < start || end == math.MaxInt64 || step <= 0 || (end-start)/step >= MaxPoints {
		return nil, errors.New("metric bounds exceed supported timestamps")
	}
	if store, ok := e.store.(metricStore); ok && (source.selection.CanAggregate(metric) || source.selection.GuardedAggregate(metric)) && int64(metric.Window)%step == 0 {
		source.compiled.Predicate.CountOnly = metric.Function == "count_over_time" || metric.Function == "rate"
		source.compiled.Predicate.ParserGuard = source.selection.GuardedAggregate(metric)
		result, err := e.metricBuckets(ctx, store, source, metric, accumulator, start, start+(end-start)/step*step, step, instant)
		if !errors.Is(err, errParserGuard) {
			return result, err
		}
	}
	if metric.HasExtractor() {
		if err := e.extractMetric(ctx, source, metric, accumulator, start-int64(metric.Window)+1, end+1); err != nil {
			return nil, err
		}
		return accumulator.evaluate(ctx, start, end, step, instant)
	}
	options := record.ScanOptions{Metadata: !metric.RetainsOnly("service_name", "detected_level")}
	if err := e.Each(ctx, source, start-int64(metric.Window)+1, end+1, options, accumulator.add); err != nil {
		return nil, err
	}
	return accumulator.evaluate(ctx, start, end, step, instant)
}

// extractMetric uses Loki's sample extractor, including outer-sum parser
// hints, error handling and grouping, after the shared SQL selection.
func (e *Engine) extractMetric(ctx context.Context, source Source, metric query.Metric, accumulator *metricAccumulator, start, end int64) error {
	extractor, err := metric.NewExtractor()
	if err != nil {
		return err
	}
	var countExtractor log.SampleExtractor
	if metric.AverageUsesSumAndCount() {
		countExtractor, err = metric.NewAverageCountExtractor()
		if err != nil {
			return err
		}
	}
	return e.store.Scan(ctx, source.compiled.Predicate, start, end, record.ScanOptions{Metadata: metric.Unwrap() || source.labelFilters || !metric.RetainsOnly("service_name", "detected_level")}, func(row record.Raw) error {
		stream, metadata := rowLabels(row)
		if countExtractor != nil {
			counts, ok := countExtractor.ForStream(labels.FromMap(stream)).ProcessString(row.Timestamp, row.Body, labels.FromMap(metadata))
			if ok {
				for _, count := range counts {
					grouped := count.Labels.Labels()
					if grouped.Len() > 256 {
						return errors.New("too many fields in a log record")
					}
					if err := accumulator.addSample(row.Timestamp, grouped.Map(), 0, grouped.Get("__error__") != ""); err != nil {
						return err
					}
					sample := &accumulator.samples[len(accumulator.samples)-1]
					sample.count, sample.meanCount = 0, 1
				}
			}
		}
		samples, ok := extractor.ForStream(labels.FromMap(stream)).ProcessString(row.Timestamp, row.Body, labels.FromMap(metadata))
		if !ok {
			return nil
		}
		for _, sample := range samples {
			grouped := sample.Labels.Labels()
			if grouped.Len() > 256 {
				return errors.New("too many fields in a log record")
			}
			if err := accumulator.addSample(row.Timestamp, grouped.Map(), sample.Value, grouped.Get("__error__") != ""); err != nil {
				return err
			}
		}
		return nil
	})
}

// Metrics evaluates events that are already in memory, in ascending order.
func Metrics(ctx context.Context, events []Event, metric query.Metric, start, end, step int64, instant bool) ([]Series, error) {
	accumulator, err := newMetricAccumulator(metric)
	if err != nil {
		return nil, err
	}
	for _, event := range events {
		if err := accumulator.add(event); err != nil {
			return nil, err
		}
	}
	return accumulator.evaluate(ctx, start, end, step, instant)
}

// evaluate computes real sliding windows (t-window,t]. It does not substitute
// disjoint histogram buckets. Empty windows produce no sample, as in Loki.
func (a *metricAccumulator) evaluate(ctx context.Context, start, end, step int64, instant bool) ([]Series, error) {
	if start < 0 || end < start || step <= 0 || (end-start)/step >= MaxPoints {
		return nil, errors.New("invalid metric evaluation range or too many points")
	}
	window := int64(a.metric.Window)
	output := map[int32]*Series{}
	left, right, samples, work, pipelineErrors := 0, 0, 0, 0, 0
	for timestamp := start; timestamp <= end; {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		for right < len(a.samples) && a.samples[right].timestamp <= timestamp {
			group := a.groups[a.samples[right].group]
			group.count += a.samples[right].count
			group.meanCount += a.samples[right].meanCount
			if a.samples[right].errored {
				pipelineErrors++
			}
			group.sum += a.samples[right].value
			right++
		}
		for left < right && a.samples[left].timestamp <= timestamp-window {
			group := a.groups[a.samples[left].group]
			group.count -= a.samples[left].count
			group.meanCount -= a.samples[left].meanCount
			if a.samples[left].errored {
				pipelineErrors--
			}
			group.sum -= a.samples[left].value
			left++
		}
		if pipelineErrors != 0 {
			return nil, errors.New("metric pipeline contains parser/conversion errors; filter or drop error labels explicitly")
		}
		if a.metric.Unwrap() {
			// Running float sums drift; recompute each window in time order,
			// as Loki does, so values do not depend on earlier windows.
			if work += right - left; work > maxUnwrapWork {
				return nil, errors.New("unwrapped metric needs too many window evaluations; increase the step or narrow the selection")
			}
			for _, group := range a.groups {
				group.sum = 0
				group.meanSamples = 0
			}
			for i := left; i < right; i++ {
				sample := a.samples[i]
				if sample.count == 0 {
					continue
				}
				group := a.groups[sample.group]
				if a.metric.Function == "avg_over_time" && !a.sumAndCount {
					// Same operation order and Inf/NaN rules as Loki's
					// range_vector.go avgOverTime and AvgOverTime.agg.
					group.meanSamples++
					mean, value := group.sum, sample.value
					if math.IsInf(mean, 0) && (math.IsInf(value, 0) && (mean > 0) == (value > 0) || !math.IsInf(value, 0) && !math.IsNaN(value)) {
						continue
					}
					group.sum += value/group.meanSamples - mean/group.meanSamples
				} else {
					group.sum += sample.value
				}
			}
		}
		for index, group := range a.groups {
			if group.count == 0 {
				continue
			}
			samples++
			if samples > MaxSamples {
				return nil, errors.New("metric result exceeds the sample budget")
			}
			value := float64(group.count)
			switch a.metric.Function {
			case "bytes_over_time", "bytes_rate", "sum_over_time":
				value = group.sum
			case "avg_over_time":
				value = group.sum
				if a.sumAndCount {
					if group.meanCount == 0 {
						continue
					}
					value /= float64(group.meanCount)
				}
			}
			if a.metric.Function == "rate" || a.metric.Function == "bytes_rate" {
				value /= a.metric.Window.Seconds()
			}
			series, found := output[int32(index)]
			if !found {
				series = &Series{Metric: group.labels}
				output[int32(index)] = series
			}
			// Prometheus model.SampleValue.String, used by Loki's API.
			encoded := strconv.FormatFloat(value, 'f', -1, 64)
			pointBytes := 64 + len(encoded)
			if pointBytes > a.budget-a.retained {
				return nil, ErrRetainedLimit
			}
			a.retained += pointBytes
			point := []any{Seconds(timestamp), encoded}
			if instant {
				series.Value = point
			} else {
				series.Values = append(series.Values, point)
			}
		}
		if end-timestamp < step {
			break
		}
		timestamp += step
	}
	result := []Series{}
	for _, series := range output {
		result = append(result, *series)
	}
	sort.Slice(result, func(i, j int) bool { return Key(result[i].Metric) < Key(result[j].Metric) })
	return result, nil
}

// Optional store capability preserves existing Store implementations.
type metricStore interface {
	MetricBuckets(context.Context, sqlgen.Predicate, int64, int64, int64, []string) ([]record.MetricBucket, error)
}

var errParserGuard = errors.New("parser guard requires exact streaming evaluation")

func (e *Engine) metricBuckets(ctx context.Context, store metricStore, source Source, metric query.Metric, accumulator *metricAccumulator, start, end, step int64, instant bool) ([]Series, error) {
	buckets, err := store.MetricBuckets(ctx, source.compiled.Predicate, start-int64(metric.Window), end, step, metric.Groups)
	if err != nil {
		return nil, err
	}
	for _, bucket := range buckets {
		if bucket.ParserGuard != 0 {
			return nil, errParserGuard
		}
	}
	var entries, bytes uint64
	for _, b := range buckets {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		group := map[string]string{}
		// An empty service label is omitted by rowLabels on the streaming path.
		for _, name := range metric.Groups {
			if name == "service_name" && b.Service != "" {
				group[name] = b.Service
			}
			if name == "detected_level" {
				group[name] = b.Level
			}
		}
		if b.Entries == 0 || b.Entries > (1<<53)-entries || b.Bytes > (1<<53)-bytes {
			return nil, errors.New("metric bucket exceeds exact numeric budget")
		}
		entries += b.Entries
		bytes += b.Bytes
		if err := accumulator.addSample(b.Timestamp, group, float64(b.Bytes), false); err != nil {
			return nil, err
		}
		accumulator.samples[len(accumulator.samples)-1].count = b.Entries
	}
	return accumulator.evaluate(ctx, start, end, step, instant)
}
