package metrics

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/rs/zerolog"
)

// JSONLExporter exports Prometheus metrics to a JSONL (JSON Lines) file periodically
type JSONLExporter struct {
	filePath string
	interval time.Duration
	gatherer prometheus.Gatherer
	log      *zerolog.Logger
}

// MetricSample represents a single metric sample in JSONL format
type MetricSample struct {
	Timestamp string            `json:"timestamp"`
	Name      string            `json:"name"`
	Type      string            `json:"type"`
	Value     float64           `json:"value"`
	Labels    map[string]string `json:"labels"`
}

// NewJSONLExporter creates a new JSONL metrics exporter
func NewJSONLExporter(filePath string, interval time.Duration, log *zerolog.Logger) (*JSONLExporter, error) {
	// Ensure parent directory exists
	dir := filepath.Dir(filePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory for metrics file: %w", err)
	}

	return &JSONLExporter{
		filePath: filePath,
		interval: interval,
		gatherer: prometheus.DefaultGatherer,
		log:      log,
	}, nil
}

// Run starts the periodic metrics export loop
func (e *JSONLExporter) Run(ctx context.Context) error {
	e.log.Info().
		Str("file", e.filePath).
		Dur("interval", e.interval).
		Msg("Starting JSONL metrics exporter")

	ticker := time.NewTicker(e.interval)
	defer ticker.Stop()

	// Export immediately on start
	if err := e.exportMetrics(); err != nil {
		e.log.Err(err).Msg("Failed to export metrics on startup")
	}

	for {
		select {
		case <-ctx.Done():
			e.log.Info().Msg("JSONL metrics exporter shutting down")
			// Export final metrics before shutdown
			if err := e.exportMetrics(); err != nil {
				e.log.Err(err).Msg("Failed to export final metrics")
			}
			return nil
		case <-ticker.C:
			if err := e.exportMetrics(); err != nil {
				e.log.Err(err).Msg("Failed to export metrics")
			}
		}
	}
}

// exportMetrics gathers and exports all metrics to the JSONL file
func (e *JSONLExporter) exportMetrics() error {
	// Gather metrics from Prometheus
	metricFamilies, err := e.gatherer.Gather()
	if err != nil {
		return fmt.Errorf("failed to gather metrics: %w", err)
	}

	// Open file in append mode
	file, err := os.OpenFile(e.filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open metrics file: %w", err)
	}
	defer file.Close()

	timestamp := time.Now().UTC().Format(time.RFC3339)
	encoder := json.NewEncoder(file)

	// Convert and write each metric
	for _, mf := range metricFamilies {
		if err := e.writeMetricFamily(encoder, mf, timestamp); err != nil {
			e.log.Err(err).Str("metric", mf.GetName()).Msg("Failed to write metric family")
			continue
		}
	}

	return nil
}

// writeMetricFamily writes all metrics in a metric family to the JSONL file
func (e *JSONLExporter) writeMetricFamily(encoder *json.Encoder, mf *dto.MetricFamily, timestamp string) error {
	metricName := mf.GetName()
	metricType := mf.GetType().String()

	for _, m := range mf.GetMetric() {
		labels := e.extractLabels(m)

		// Handle different metric types
		switch mf.GetType() {
		case dto.MetricType_COUNTER:
			if err := e.writeSample(encoder, timestamp, metricName, metricType, m.GetCounter().GetValue(), labels); err != nil {
				return err
			}
		case dto.MetricType_GAUGE:
			if err := e.writeSample(encoder, timestamp, metricName, metricType, m.GetGauge().GetValue(), labels); err != nil {
				return err
			}
		case dto.MetricType_SUMMARY:
			summary := m.GetSummary()
			// Write quantiles
			for _, q := range summary.GetQuantile() {
				quantileLabels := e.copyLabels(labels)
				quantileLabels["quantile"] = fmt.Sprintf("%g", q.GetQuantile())
				if err := e.writeSample(encoder, timestamp, metricName, metricType, q.GetValue(), quantileLabels); err != nil {
					return err
				}
			}
			// Write sum and count
			sumLabels := e.copyLabels(labels)
			sumLabels["stat"] = "sum"
			if err := e.writeSample(encoder, timestamp, metricName, metricType, summary.GetSampleSum(), sumLabels); err != nil {
				return err
			}
			countLabels := e.copyLabels(labels)
			countLabels["stat"] = "count"
			if err := e.writeSample(encoder, timestamp, metricName, metricType, float64(summary.GetSampleCount()), countLabels); err != nil {
				return err
			}
		case dto.MetricType_HISTOGRAM:
			histogram := m.GetHistogram()
			// Write buckets
			for _, b := range histogram.GetBucket() {
				bucketLabels := e.copyLabels(labels)
				bucketLabels["le"] = fmt.Sprintf("%g", b.GetUpperBound())
				if err := e.writeSample(encoder, timestamp, metricName+"_bucket", metricType, float64(b.GetCumulativeCount()), bucketLabels); err != nil {
					return err
				}
			}
			// Write sum and count
			sumLabels := e.copyLabels(labels)
			sumLabels["stat"] = "sum"
			if err := e.writeSample(encoder, timestamp, metricName+"_sum", metricType, histogram.GetSampleSum(), sumLabels); err != nil {
				return err
			}
			countLabels := e.copyLabels(labels)
			countLabels["stat"] = "count"
			if err := e.writeSample(encoder, timestamp, metricName+"_count", metricType, float64(histogram.GetSampleCount()), countLabels); err != nil {
				return err
			}
		case dto.MetricType_UNTYPED:
			if err := e.writeSample(encoder, timestamp, metricName, metricType, m.GetUntyped().GetValue(), labels); err != nil {
				return err
			}
		}
	}

	return nil
}

// writeSample writes a single metric sample to the JSONL file
func (e *JSONLExporter) writeSample(encoder *json.Encoder, timestamp, name, metricType string, value float64, labels map[string]string) error {
	sample := MetricSample{
		Timestamp: timestamp,
		Name:      name,
		Type:      metricType,
		Value:     value,
		Labels:    labels,
	}

	if err := encoder.Encode(sample); err != nil {
		return fmt.Errorf("failed to encode metric sample: %w", err)
	}

	return nil
}

// extractLabels extracts labels from a Prometheus metric
func (e *JSONLExporter) extractLabels(m *dto.Metric) map[string]string {
	labels := make(map[string]string)
	for _, lp := range m.GetLabel() {
		labels[lp.GetName()] = lp.GetValue()
	}
	return labels
}

// copyLabels creates a copy of a label map
func (e *JSONLExporter) copyLabels(labels map[string]string) map[string]string {
	copy := make(map[string]string, len(labels))
	for k, v := range labels {
		copy[k] = v
	}
	return copy
}

// ExportToWriter exports current metrics to an io.Writer (useful for testing)
func (e *JSONLExporter) ExportToWriter(w io.Writer) error {
	metricFamilies, err := e.gatherer.Gather()
	if err != nil {
		return fmt.Errorf("failed to gather metrics: %w", err)
	}

	timestamp := time.Now().UTC().Format(time.RFC3339)
	encoder := json.NewEncoder(w)

	for _, mf := range metricFamilies {
		if err := e.writeMetricFamily(encoder, mf, timestamp); err != nil {
			return err
		}
	}

	return nil
}
