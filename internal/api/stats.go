package api

import (
	"github.com/gorilla/mux"
	"github.com/itsHabib/collector/internal/statsd/collector"
	"go.uber.org/zap"
	"net/http"
)

type Stats struct {
	logger *zap.Logger
	col    *collector.Collector
}

func NewStats(logger *zap.Logger, col *collector.Collector) *Stats {
	return &Stats{
		logger: logger,
		col:    col,
	}
}

func (s *Stats) RegisterRoutes(r *mux.Router) {
	r.HandleFunc("/stats", s.Stats)
}

func (s *Stats) Stats(w http.ResponseWriter, r *http.Request) {
	stats := s.col.Stats()
	jsonResponse(s.logger, w, 200, stats)
}
