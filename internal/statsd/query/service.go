package query

import (
	"context"
	"errors"
	"time"

	"go.uber.org/zap"

	"github.com/itsHabib/collector/internal/statsd"
	"github.com/itsHabib/collector/internal/storage"
)

const (
	queryTimeout = 20 * time.Second
)

type Service struct {
	storage storage.Storer
	logger  *zap.Logger
}

func NewService(logger *zap.Logger, s storage.Storer) *Service {
	return &Service{
		storage: s,
		logger:  logger,
	}
}

func (s *Service) Query(ctx context.Context, r Request) (*Response, error) {
	tags := statsd.FromMap(r.Tags)
	key := statsd.FormMetricKey(r.Name, tags)
	seriesC := make(chan []statsd.Metric, 1)
	timer := time.NewTimer(queryTimeout)

	go func() {
		seriesC <- s.storage.SearchKey(key)
	}()

	for {
		select {
		case <-timer.C:
			const msg = "timed out waiting for search results"
			s.logger.Error(msg)
			return nil, errors.New(msg)
		case <-ctx.Done():
			s.logger.Error("context done while searching key", zap.Error(ctx.Err()))
			return nil, errors.New("unable to search key")
		case res := <-seriesC:
			return &Response{Series: res}, nil
		}
	}
}

func (s *Service) QueryRange(ctx context.Context, r RequestRange) (*Response, error) {
	tags := statsd.FromMap(r.Tags)
	key := statsd.FormMetricKey(r.Name, tags)
	seriesC := make(chan []statsd.Metric, 1)
	timer := time.NewTimer(queryTimeout)

	go func() {
		seriesC <- s.storage.SearchKeyRange(key, storage.QueryRange{Start: r.Range.Start, End: r.Range.End})
	}()

	for {
		select {
		case <-timer.C:
			const msg = "timed out waiting for search results"
			s.logger.Error(msg)
			return nil, errors.New(msg)
		case <-ctx.Done():
			s.logger.Error("context done while searching key range", zap.Error(ctx.Err()))
			return nil, errors.New("unable to search key")
		case res := <-seriesC:
			return &Response{Series: res}, nil
		}
	}
}
