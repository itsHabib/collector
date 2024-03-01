package api

import (
	"encoding/json"
	"io"
	"net/http"
	"time"

	"github.com/gorilla/mux"
	"go.uber.org/zap"

	"github.com/itsHabib/collector/internal/statsd/query"
)

type Query struct {
	logger *zap.Logger
	svc    *query.Service
}

func NewQuery(logger *zap.Logger, svc *query.Service) *Query {
	return &Query{
		logger: logger,
		svc:    svc,
	}
}

func (q *Query) RegisterRoutes(r *mux.Router) {
	r.HandleFunc("/query", q.Query)
	r.HandleFunc("/query_range", q.QueryRange)
}

func (q *Query) Query(w http.ResponseWriter, r *http.Request) {
	var req QueryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		const msg = "unable to decode request body"
		q.logger.Error(msg, zap.Error(err))
		http.Error(w, "bad query request", http.StatusBadRequest)
		return
	}
	defer discardBody(r)

	ctx := r.Context()
	agg := r.URL.Query().Get("agg") == "true"
	qReq := query.Request{
		Name: req.Name,
		Tags: req.Tags,
	}
	resp, err := q.svc.Query(ctx, qReq)
	if err != nil {
		const msg = "unable to query metrics"
		q.logger.Error(msg, zap.Error(err))
		http.Error(w, "unable to get query response", http.StatusInternalServerError)
		return
	}
	qResp := QueryResponse{
		Response: resp,
	}
	if agg {
		qResp.Agg = ptr(resp.Agg())
	}

	jsonResponse(q.logger, w, http.StatusOK, qResp)

}

func (q *Query) QueryRange(w http.ResponseWriter, r *http.Request) {
	var req QueryRequestRange
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		const msg = "unable to decode request body"
		q.logger.Error(msg, zap.Error(err))
		http.Error(w, "bad query request", http.StatusBadRequest)
		return
	}
	defer discardBody(r)

	if err := req.Range.Validate(); err != nil {
		const msg = "invalid range"
		q.logger.Error(msg, zap.Error(err))
		http.Error(w, msg, http.StatusBadRequest)
		return
	}

	var (
		start int64
		end   int64
	)

	if req.Range.Absolute != nil {
		start = req.Range.Absolute.Start.UnixMilli()
		end = req.Range.Absolute.End.UnixMilli()
	}

	if req.Range.Duration != "" {
		// catch error during validate
		d, err := time.ParseDuration(req.Range.Duration)
		if err != nil {
			const msg = "invalid duration"
			q.logger.Error(msg, zap.String("duration", req.Range.Duration), zap.Error(err))
			http.Error(w, msg, http.StatusBadRequest)
			return
		}
		n := time.Now()
		start = n.Add(-d).UnixMilli()
		end = n.UnixMilli()
	}

	agg := r.URL.Query().Get("agg") == "true"
	rr := query.RequestRange{
		Name: req.Name,
		Tags: req.Tags,
		Range: query.Range{
			Start: start,
			End:   end,
		},
	}
	resp, err := q.svc.QueryRange(r.Context(), rr)
	if err != nil {
		const msg = "unable to query metrics"
		q.logger.Error(msg, zap.Error(err), zap.Any("requestQuery", rr))
		http.Error(w, "unable to get query response", http.StatusInternalServerError)
		return
	}

	qResp := QueryResponse{
		Response: resp,
	}
	if agg {
		qResp.Agg = ptr(resp.Agg())
	}

	jsonResponse(q.logger, w, http.StatusOK, qResp)
}

type QueryResponse struct {
	*query.Response
	Agg *query.AggResponse `json:"agg,omitempty"`
}

type QueryRequest struct {
	Name string            `json:"name"`
	Tags map[string]string `json:"tags"`
}

type QueryRequestRange struct {
	Name  string                   `json:"name"`
	Tags  map[string]string        `json:"tags"`
	Range QueryRangeRequestOptions `json:"range"`
}

type QueryRangeRequestOptions struct {
	Absolute *AbsoluteRangeOption `json:"absolute"`
	Duration string               `json:"duration"`
}

func (q *QueryRangeRequestOptions) Validate() error {
	return nil
}

type AbsoluteRangeOption struct {
	Start time.Time `json:"start"`
	End   time.Time `json:"end"`
}

func jsonResponse(logger *zap.Logger, w http.ResponseWriter, code int, d any) {
	w.WriteHeader(code)
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(d); err != nil {
		logger.Error("unable to encode response", zap.Error(err))
		http.Error(w, "unable to encode response", http.StatusInternalServerError)
	}
}

func discardBody(r *http.Request) {
	if r == nil || r.Body == nil {
		return
	}

	_, _ = io.Copy(io.Discard, r.Body)
}

func ptr[T any](v T) *T {
	return &v
}
