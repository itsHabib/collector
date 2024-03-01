package collector

import (
	"github.com/itsHabib/collector/internal/statsd"
	"sync"
	"time"
)

const (
	collectorMetricPrefix = "collector."
)

type stats struct {
	bytesRead     stat[uint64]
	metricsParsed stat[uint]

	metricSendTimeouts  stat[uint]
	metricsParsedErrors stat[uint]
	udpReadErrors       stat[uint]
	flushErrors         stat[uint]

	heapAllocMb uint64
	allocMb     uint64

	parsePerSec uint
	bytesPerSec uint
	sendsPerSec uint
	storePerSec uint

	avgStoreCount uint

	l *sync.RWMutex
}

type statType interface {
	~uint | ~uint64
}

type stat[T statType] struct {
	name string
	val  T
}

func (s *stat[statType]) count() {
	s.val++
}

func newStats() *stats {
	return &stats{
		bytesRead:           stat[uint64]{name: "bytes_read"},
		metricsParsed:       stat[uint]{name: "metrics_parsed"},
		metricSendTimeouts:  stat[uint]{name: "metric_send_timeouts"},
		metricsParsedErrors: stat[uint]{name: "metrics_parsed_errors"},
		udpReadErrors:       stat[uint]{name: "udp_read_errors"},
		flushErrors:         stat[uint]{name: "flush_errors"},
		l:                   new(sync.RWMutex),
	}
}

func (s *stats) countBytesRead(read int) {
	s.l.Lock()
	s.bytesRead.val += uint64(read)
	s.l.Unlock()
}

func (s *stats) countMetricsParsed() {
	s.l.Lock()
	s.metricsParsed.count()
	s.l.Unlock()
}

func (s *stats) countSendTimeouts() {
	s.l.Lock()
	s.metricSendTimeouts.count()
	s.l.Unlock()
}

func (s *stats) countMetricsParsedErrors() {
	s.l.Lock()
	s.metricsParsedErrors.count()
	s.l.Unlock()
}

func (s *stats) countUDPReadErrors() {
	s.l.Lock()
	s.udpReadErrors.count()
	s.l.Unlock()
}

func (s *stats) countFlushErrors() {
	s.l.Lock()
	s.flushErrors.count()
	s.l.Unlock()
}

func (s *stats) getBytesRead() uint64 {
	s.l.RLock()
	val := s.bytesRead.val
	s.l.RUnlock()

	return val
}

func (s *stats) getMetricsParsed() uint {
	s.l.RLock()
	val := s.metricsParsed.val
	s.l.RUnlock()

	return val
}

func (s *stats) getMetricSendTimeouts() uint {
	s.l.RLock()
	val := s.metricSendTimeouts.val
	s.l.RUnlock()

	return val
}

func (s *stats) getMetricsParsedErrors() uint {
	s.l.RLock()
	val := s.metricsParsedErrors.val
	s.l.RUnlock()

	return val
}

func (s *stats) getUDPReadErrors() uint {
	s.l.RLock()
	val := s.udpReadErrors.val
	s.l.RUnlock()

	return val
}

func (s *stats) getFlushErrors() uint {
	s.l.RLock()
	val := s.flushErrors.val
	s.l.RUnlock()

	return val
}

type Rate struct {
	rate     int
	counter  int
	interval time.Duration
}

func internalStatsMetric(name string, val float64, tags *statsd.Tags) statsd.Metric {
	fullName := collectorMetricPrefix + name
	return statsd.Metric{
		Name:      fullName,
		Value:     val,
		Timestamp: time.Now().UnixMilli(),
		Type:      statsd.CounterType,
		Tags:      tags,
		Key:       statsd.FormMetricKey(name, *tags),
	}
}
