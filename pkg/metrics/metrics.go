// Package metrics contains a sink interface to be used by clients to implement sink.
// It also provides a default NoopSink and LogSink for convenience
package metrics

import (
	"context"
	"time"

	"github.com/siddontang/loggers"
)

// Metric types.
const (
	UNKNOWN byte = iota
	COUNTER
	GAUGE

	SinkTimeout = 1 * time.Second

	ChunkProcessingTimeMetricName    = "chunk_processing_time"
	ChunkLogicalRowsCountMetricName  = "chunk_num_logical_rows"
	ChunkAffectedRowsCountMetricName = "chunk_num_affected_rows"
)

// Metrics are collection of MetricValues.
type Metrics struct {
	Values []MetricValue
}

type MetricValue struct {
	// Name is the metric name
	Name string

	// Value is the value of the metric.
	Value float64

	// Type is the metric type: GAUGE, COUNTER, and other const.
	Type byte
}

// Sink sends metrics to an external destination.
type Sink interface {
	// Send sends metrics to the sink. It must respect the context timeout, if any.
	Send(context.Context, *Metrics) error
}

// noopSink is the default sink which does nothing
type noopSink struct{}

func (s *noopSink) Send(ctx context.Context, m *Metrics) error {
	return nil
}

var _ Sink = &noopSink{}

func NewNoopSink() *noopSink {
	return &noopSink{}
}

// NoopSink is a Noop Sink instance to be used in tests
var NoopSink = NewNoopSink()

// logSink logs metrics
type logSink struct {
	logger loggers.Advanced
}

func (l *logSink) Send(ctx context.Context, m *Metrics) error {
	for _, v := range m.Values {
		switch v.Type {
		case COUNTER:
			l.logger.Infof("metric: name: %s, type: counter, value: %f", v.Name, v.Value)
		case GAUGE:
			l.logger.Infof("metric: name: %s, type: gauge, value: %f", v.Name, v.Value)
		default:
			l.logger.Errorf("Received invalid metric type: %s, name: %s, value: %f", v.Type, v.Name, v.Value)
		}
	}
	return nil
}

var _ Sink = &logSink{}

func NewLogSink(logger loggers.Advanced) *logSink {
	return &logSink{
		logger: logger,
	}
}
