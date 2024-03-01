package parser

import (
	"github.com/itsHabib/collector/internal/statsd"
	"strconv"
)

type Parser struct {
	cur        int
	start      int
	metric     string
	name       string
	value      float64
	metricType string
	tags       map[string]string
	ts         int64
}

type ParserOpt func(*Parser)

func WithMetric(metric string, ts int64) ParserOpt {
	return func(p *Parser) {
		p.setMetric(metric, ts)
	}
}

func NewParser(opts ...ParserOpt) *Parser {
	p := &Parser{}
	for i := range opts {
		opts[i](p)
	}

	return p
}

func (p *Parser) Metric() string {
	return p.metric
}

func (p *Parser) ParseMetric(metric string, ts int64) bool {
	p.reset()
	p.setMetric(metric, ts)

	return p.parse()
}

func (p *Parser) setMetric(metric string, ts int64) {
	p.metric = metric
	p.ts = ts
}

func (p *Parser) parse() bool {
	// collect metric name
	p.name = p.collectUntil(':')
	if p.name == "" {
		return false
	}

	// collect value
	v := p.collectUntil('|')
	if v == "" {
		return false
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return false
	}
	p.value = f

	// collect metric type
	p.metricType = p.collectUntil('|')
	if p.metricType == "" {
		return false
	}

	if p.seek() == '#' {
		p.adv()

		return p.getTags()
	}

	return true
}

func (p *Parser) getTags() bool {
	for p.seek() != 0 {
		k := p.collectUntil(':')
		if k == "" {
			return false
		}

		v := p.collectUntil(',')
		if v == "" {
			return false
		}
		if p.tags == nil {
			p.tags = make(map[string]string)
		}

		p.tags[k] = v
	}

	return true
}

func (p *Parser) collectUntil(ch byte) string {
	p.start = p.cur
	for p.seek() != ch && p.seek() != 0 {
		p.adv()
	}

	if p.start == p.cur {
		return ""
	}

	v := p.metric[p.start:p.cur]
	p.adv()

	return v
}

func (p *Parser) seek() byte {
	if p.cur >= len(p.metric) {
		return 0
	}

	return p.metric[p.cur]
}

func (p *Parser) adv() {
	p.cur++
}

func (p *Parser) reset() {
	p.cur = 0
	p.start = 0
	if p.tags != nil {
		for k := range p.tags {
			delete(p.tags, k)
		}
	}
}

func (p *Parser) ToStatsD() statsd.Metric {
	m := statsd.Metric{
		Name:      p.name,
		Value:     p.value,
		Type:      statsd.ToStatsDType(p.metricType),
		Timestamp: p.ts,
		Key:       p.name,
	}
	if len(p.tags) == 0 {
		return m
	}

	tags := new(statsd.Tags)
	for k := range p.tags {
		tags.Add(statsd.Tag{Key: k, Value: p.tags[k]})
	}
	m.Tags = tags
	m.Key += "|" + tags.String()

	return m
}
