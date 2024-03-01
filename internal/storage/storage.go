package storage

import (
	"sync"

	"github.com/itsHabib/collector/internal/skiplist"
	"github.com/itsHabib/collector/internal/statsd"
)

type Storer interface {
	Store(batch []statsd.Metric) error
	SearchKey(key string) []statsd.Metric
	SearchKeyRange(key string, r QueryRange) []statsd.Metric
	Count() int
}

type Range interface {
	~struct {
		Start int64
		End   int64
	}
}

type SkipListStorage struct {
	sl *skiplist.SkipList
	l  *sync.RWMutex
}

func NewSkipListStorage() *SkipListStorage {
	return &SkipListStorage{
		sl: skiplist.NewSkipList(),
		l:  new(sync.RWMutex),
	}
}

func (s *SkipListStorage) Store(batch []statsd.Metric) error {
	//	n := time.Now()
	s.l.Lock()
	for i := range batch {
		s.sl.Insert(batch[i])
	}
	s.l.Unlock()
	//	log.Printf("inserted %d metrics in %dms, count: %d\n", len(batch), time.Since(n).Milliseconds(), s.Count())

	return nil
}

func (s *SkipListStorage) SearchKey(key string) []statsd.Metric {
	s.l.RLock()
	v := s.sl.SearchIndex(key)
	s.l.RUnlock()

	return v
}

func (s *SkipListStorage) SearchKeyFull(key string) []statsd.Metric {
	s.l.RLock()
	v := s.sl.SearchKeyFull(key)
	s.l.RUnlock()

	return v
}

type QueryRange struct {
	Start int64
	End   int64
}

func (s *SkipListStorage) SearchKeyRange(key string, r QueryRange) []statsd.Metric {
	s.l.RLock()
	v := s.sl.SearchIndexRange(key, skiplist.RangeQuery{Start: r.Start, End: r.End})
	s.l.RUnlock()

	return v
}

func (s *SkipListStorage) Count() int {
	return s.sl.Count()
}

func Latest(list []statsd.Metric) *statsd.Metric {
	if len(list) == 0 {
		return nil
	}

	l := list[0]
	for i := 1; i < len(list); i++ {
		if list[i].Timestamp > l.Timestamp {
			l = list[i]
		}
	}

	return &l
}
