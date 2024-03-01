package query

import (
	"errors"
	"fmt"
	"time"

	"github.com/itsHabib/collector/internal/statsd"
)

//type KeyQueryReq struct {
//	Name  string            `json:"name"`
//	Tags  map[string]string `json:"tags"`
//	Range *QueryRange       `json:"range"`
//}

type Range struct {
	Start int64 `json:"start"`
	End   int64 `json:"end"`
}

func (r *Range) Validate() error {
	if r.Start > r.End {
		return errors.New("start time must be before end time")
	}

	if r.Start == 0 || r.End == 0 {
		return errors.New("start and end can not be zero")
	}

	return nil
}

type QueryValues struct {
	Values   []statsd.Metric `json:"values"`
	Count    int             `json:"count"`
	SumCount float64         `json:"sumCount"`
}

type KeyQueryResp struct {
	Index QueryValues `json:"index"`
	Full  QueryValues `json:"full"`
}

type Response struct {
	Series []statsd.Metric `json:"series"`
}

func (r *Response) Agg() AggResponse {
	if len(r.Series) == 0 {
		return AggResponse{}
	}

	var (
		min = r.Series[0]
		max = r.Series[0]
		sum = 0.0
	)

	for i := range r.Series {
		sum += r.Series[i].Value

		if r.Series[i].Value < min.Value {
			min = r.Series[i]
			continue
		}

		if r.Series[i].Value > max.Value {
			max = r.Series[i]
		}
	}

	return AggResponse{
		NumItems: len(r.Series),
		Sum:      sum,
		Avg:      int(sum) / len(r.Series),
		Min:      newAggDetail(min),
		Max:      newAggDetail(max),
	}
}

type Request struct {
	Name  string            `json:"metric"`
	Tags  map[string]string `json:"tags"`
	Range *RangeOpts        `json:"range"`
}

type RequestRange struct {
	Name  string
	Tags  map[string]string
	Range Range
}

type RangeOpts struct {
	Absolute *Range        `json:"absolute"`
	Duration time.Duration `json:"duration"`
}

func (r *RangeOpts) Validate() error {
	if r.Absolute != nil && r.Duration != 0 {
		return fmt.Errorf("cant have both aboslute and duration")
	}
	if r.Absolute != nil {
		if err := r.Absolute.Validate(); err != nil {
			return fmt.Errorf("invalid absolute range: %w", err)
		}
		return nil
	}

	if r.Duration == 0 {
		return fmt.Errorf("duration must be positive number")
	}

	return nil
}

type AggResponse struct {
	NumItems int       `json:"numItems"`
	Max      AggDetail `json:"max"`
	Min      AggDetail `json:"min"`
	Sum      float64   `json:"sum"`
	Avg      int       `json:"avg"`
}

func newAggDetail(m statsd.Metric) AggDetail {
	return AggDetail{
		Value: m.Value,
		Item: AggItem{
			Name: m.Name,
			Tags: m.TagsMap(),
		},
	}
}

type AggDetail struct {
	Value float64 `json:"value"`
	Item  AggItem `json:"item"`
}

type AggItem struct {
	Name string            `json:"name"`
	Tags map[string]string `json:"tags"`
}
