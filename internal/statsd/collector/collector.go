package collector

import (
	"context"
	"errors"
	"fmt"
	"golang.org/x/sync/errgroup"
	"net"
	"os"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/itsHabib/collector/internal/statsd"
	"github.com/itsHabib/collector/internal/statsd/parser"
	"github.com/itsHabib/collector/internal/storage"
)

const (
	bufferSize         = 1024
	metricBufferCap    = 75000
	storeStatsInterval = time.Second * 5
)

type Collector struct {
	addr               string
	flushInterval      time.Duration
	storeStatsInterval time.Duration
	shouldStoreStats   bool
	logger             *zap.Logger
	conn               net.PacketConn
	parser             *parser.Parser
	stats              *stats
	metricBuffer       chan statsd.Metric
	counters           *counters
	storage            storage.Storer
}

func NewCollector(addr string, flushInterval time.Duration, storage storage.Storer, logger *zap.Logger, storeStats bool) *Collector {
	return &Collector{
		addr:               addr,
		flushInterval:      flushInterval,
		storeStatsInterval: storeStatsInterval,
		shouldStoreStats:   storeStats,
		logger:             logger,
		parser:             parser.NewParser(),
		stats:              newStats(),
		counters:           newCounters(),
		storage:            storage,
		metricBuffer:       make(chan statsd.Metric, metricBufferCap),
		//		internalStatsBuffer: make(chan statsd.Metric, internalBufferCap),
	}
}

func (c *Collector) Collect(ctx context.Context) error {
	c.logger.Info("starting to collect statsd metrics")

	if err := c.listen(); err != nil {
		return fmt.Errorf("unable to listen on udp address: %w", err)
	}

	g, gctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		c.read(gctx)
		return nil
	})

	g.Go(func() error {
		c.flush(gctx)
		return nil
	})

	g.Go(func() error {
		c.collectStats(gctx)
		return nil
	})

	if err := g.Wait(); err != nil {
		const msg = "error running collector"
		c.logger.Error(msg, zap.Error(err))
		return fmt.Errorf(msg+":%w", err)
	}

	return nil
}

type StatsView struct {
	BytesRead          uint64 `json:"bytesRead"`
	MetricsParsed      uint   `json:"metricsParsed"`
	MetricsParsedError uint   `json:"metricsParsedError"`
	MetricSendTimeouts uint   `json:"metricSendTimeouts"`
	FlushErrors        uint   `json:"flushErrors"`
	UDPReadErrors      uint   `json:"udpReadErrors"`
}

func (c *Collector) Stats() StatsView {
	return StatsView{
		BytesRead:          c.stats.getBytesRead(),
		MetricsParsed:      c.stats.getMetricsParsed(),
		MetricsParsedError: c.stats.getMetricsParsedErrors(),
		MetricSendTimeouts: c.stats.getMetricSendTimeouts(),
		FlushErrors:        c.stats.getFlushErrors(),
		UDPReadErrors:      c.stats.getUDPReadErrors(),
	}
}

func (c *Collector) listen() error {
	updAddr, err := net.ResolveUDPAddr("udp", c.addr)
	if err != nil {
		const msg = "unable to resolve udp address"
		c.logger.Error(msg, zap.Error(err))
		return fmt.Errorf(msg+": %w", err)
	}

	conn, err := net.ListenUDP("udp", updAddr)
	if err != nil {
		const msg = "unable to listen on udp address"
		c.logger.Error(msg, zap.Error(err))
		return fmt.Errorf(msg+": %w", err)
	}
	c.conn = conn

	return nil
}

func (c *Collector) read(ctx context.Context) {
	buffer := make([]byte, bufferSize)

	for {
		select {
		case <-ctx.Done():
			c.logger.Info("context cancelled, stopping read")
			c.stop()
			return
		default:
			_ = c.conn.SetReadDeadline(time.Now().Add(2 * time.Second))
			n, _, err := c.conn.ReadFrom(buffer)
			if err != nil && !errors.Is(err, os.ErrDeadlineExceeded) {
				c.logger.Error("unable to read from connection", zap.Error(err))
				c.stats.countUDPReadErrors()
			}
			if n == 0 {
				continue
			}
			c.stats.countBytesRead(n)

			ts := time.Now().UnixMilli()
			metric := string(buffer[:n])
			if err := c.process(ctx, metric, ts); err != nil {
				const msg = "unable to process metric"
				c.logger.Error(msg, zap.String("metric", metric), zap.Error(err))
			}
		}
	}
}

func (c *Collector) process(ctx context.Context, metric string, ts int64) error {
	if ok := c.parser.ParseMetric(metric, ts); !ok {
		c.stats.countMetricsParsedErrors()
		const msg = "unable to parse metric"
		c.logger.Error(msg, zap.String("metric", metric))
		return fmt.Errorf(msg+": %s", metric)
	}

	c.stats.countMetricsParsed()
	c.sendMetric(ctx, c.parser.ToStatsD())

	return nil
}

func (c *Collector) sendMetric(ctx context.Context, metric statsd.Metric) {
	const sendMetricTimeout = 750 * time.Millisecond
	c.send(ctx, metric, c.metricBuffer, sendMetricTimeout)
}

//	func (c *Collector) sendInternal(ctx context.Context, metric statsd.Metric) {
//		const sendInternalTimeout = 250 * time.Millisecond
//		c.send(ctx, metric, c.internalStatsBuffer, sendInternalTimeout)
//	}
func (c *Collector) send(
	ctx context.Context,
	metric statsd.Metric,
	mc chan<- statsd.Metric,
	timeout time.Duration,
) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			c.logger.Info("context cancelled, stopping metric send")
			return
		case mc <- metric:
			return
		case <-timer.C:
			c.logger.Warn("send buffer timeout reached, unable to send metric metric to store", zap.Any("metric", metric))
			c.stats.countSendTimeouts()
			return
		}
	}
}

func (c *Collector) flush(ctx context.Context) {
	ticker := time.NewTicker(c.flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			c.logger.Info("context cancelled, stopping flush")
			return
		case <-ticker.C:
			if err := c.flushBuffer(); err != nil {
				c.logger.Error("unable to flush metrics", zap.Error(err))
				c.stats.countFlushErrors()
			}
		}
	}
}

func (c *Collector) flushBuffer() error {
	batchSize := len(c.metricBuffer)
	if batchSize == 0 {
		return nil
	}

	batch := make([]statsd.Metric, 0, batchSize)
	var (
		ct int
	)

	for ct < batchSize {
		var (
			m  statsd.Metric
			ok bool
		)
		select {
		case m, ok = <-c.metricBuffer:
			if !ok {
				c.logger.Info("metrics buffer closed, unable to flush")
				return nil
			}
		default:
			return nil
		}
		switch m.Type {
		case statsd.CounterType:
			key := m.Key
			v, ok := c.counters.get(key)
			latestVal := v.Value
			if !ok {
				previousCounters := c.storage.SearchKey(key)
				if len(previousCounters) > 0 {
					latestVal = previousCounters[len(previousCounters)-1].Value
				}
			}
			m.Value += latestVal
			c.counters.set(key, m)
		default:
			batch = append(batch, m)
		}
		ct++
	}

	for k, v := range c.counters.items {
		batch = append(batch, v.Copy())
		c.counters.delete(k)
	}
	if err := c.store(batch); err != nil {
		return fmt.Errorf("unable to store metrics: %w", err)
	}

	return nil
}

func (c *Collector) store(batch []statsd.Metric) error {
	n := time.Now()
	if err := c.storage.Store(batch); err != nil {
		const msg = "unable to store metrics"
		c.logger.Error(msg, zap.Error(err), zap.Int("batchSize", len(batch)))
		return fmt.Errorf(msg+": %w", err)
	}
	c.logger.Info(
		"stored metrics",
		zap.Int("batchSize", len(batch)),
		zap.Int("count", c.storage.Count()),
		zap.Int("ms", int(time.Since(n).Milliseconds())),
	)

	return nil
}

func (c *Collector) collectStats(ctx context.Context) {
	if !c.shouldStoreStats {
		return
	}

	ticker := time.NewTicker(c.storeStatsInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			c.logger.Info("context cancelled, stopping collectStats")
			return
		case <-ticker.C:
			if err := c.storeStats(); err != nil {
				c.logger.Error("unable to store stats", zap.Error(err))
			}
		}
	}
}

func (c *Collector) storeStats() error {
	tags := new(statsd.Tags)
	tags.Add(statsd.Tag{
		Key:   "source",
		Value: "internal",
	})
	b := make([]statsd.Metric, 6)
	b[0] = internalStatsMetric(c.stats.bytesRead.name, float64(c.stats.getBytesRead()), tags)
	b[1] = internalStatsMetric(c.stats.metricsParsed.name, float64(c.stats.getMetricsParsed()), tags)
	b[2] = internalStatsMetric(c.stats.metricsParsedErrors.name, float64(c.stats.getMetricsParsedErrors()), tags)
	b[3] = internalStatsMetric(c.stats.metricSendTimeouts.name, float64(c.stats.getMetricSendTimeouts()), tags)
	b[4] = internalStatsMetric(c.stats.flushErrors.name, float64(c.stats.getFlushErrors()), tags)
	b[5] = internalStatsMetric(c.stats.udpReadErrors.name, float64(c.stats.getUDPReadErrors()), tags)

	if err := c.store(b); err != nil {
		const msg = "unable to store stats"
		c.logger.Error(msg, zap.Error(err))
		return fmt.Errorf(msg+": %w", err)
	}

	return nil
}

func (c *Collector) stop() {
	c.logger.Info("closing connection, closing metric buffer")
	close(c.metricBuffer)
	if err := c.conn.Close(); err != nil {
		c.logger.Error("unable to close connection", zap.Error(err))
	}
}

type counters struct {
	items map[string]statsd.Metric
	l     *sync.RWMutex
}

func newCounters() *counters {
	return &counters{
		items: make(map[string]statsd.Metric),
		l:     new(sync.RWMutex),
	}
}

func (c *counters) get(key string) (statsd.Metric, bool) {
	c.l.RLock()
	v, ok := c.items[key]
	c.l.RUnlock()

	return v, ok
}

func (c *counters) set(key string, value statsd.Metric) {
	c.l.Lock()
	c.items[key] = value
	c.l.Unlock()
}

func (c *counters) delete(key string) {
	c.l.Lock()
	delete(c.items, key)
	c.l.Unlock()
}
