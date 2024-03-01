package skiplist

import (
	"math"
	"math/rand"
	"sync"

	"github.com/itsHabib/collector/internal/statsd"
)

const (
	minLevel = 5
)

type Node struct {
	Data    statsd.Metric
	Forward []*Node
}

type Index map[string][]*Node

func (i Index) insert(n *Node) {
	idx := i.searchInsert(n)
	k := n.Data.Key
	//if k == "metric.1.counter|region:region1,version:v1" {
	//	log.Printf("INSERT INDEX (%s), Val: %f @IDX: %d", k, n.Data.Value, idx)
	//}

	if idx == -1 {
		i[k] = []*Node{n}

		return
	}

	l := i[k]
	if idx == len(l) {
		l = append(l, n)
	} else {
		for i := len(l) - 1; i > idx; i-- {
			l[i] = l[i-1]
		}
		l[idx] = n
	}

	i[k] = l
}

func (i Index) searchInsert(n *Node) int {
	if n == nil {
		return -1
	}

	k := n.Data.Key
	l, ok := i[k]
	if !ok {
		return -1
	}

	var (
		low  = 0
		high = len(l) - 1
	)

	for low <= high {
		mid := low + (high-low)/2
		if l[mid].Data.Timestamp == n.Data.Timestamp {
			low = mid + 1
		} else if l[mid].Data.Timestamp < n.Data.Timestamp {
			low = mid + 1
		} else {
			high = mid - 1
		}
	}

	return low
}

type SkipList struct {
	header   *Node
	level    int
	maxLevel int
	p        float32
	count    int
	index    Index
	l        sync.RWMutex
}

func NewSkipList() *SkipList {
	return &SkipList{
		header: &Node{
			Forward: make([]*Node, minLevel),
		},
		maxLevel: minLevel,
		level:    1,
		p:        0.5,
		index:    make(map[string][]*Node),
	}
}

func (s *SkipList) Insert(d statsd.Metric) {
	update := make([]*Node, s.maxLevel)
	cur := s.header
	//if d.Key == "metric.1.counter|region:region1,version:v1" {
	//	log.Printf("INSERT SKIP LIST (%s), Val: %f", d.Key, d.Value)
	//}

	for i := s.level - 1; i >= 0; i-- {
		for cur.Forward[i] != nil && cur.Forward[i].Data.Timestamp < d.Timestamp {
			cur = cur.Forward[i]
		}
		update[i] = cur
	}

	level := s.randomLevel()
	if level >= s.maxLevel {
		s.updateMaxLevel()

	}
	if level > s.level {
		for i := s.level; i < level; i++ {
			update[i] = s.header
		}
		s.level = level
	}

	node := &Node{
		Data:    d,
		Forward: make([]*Node, level),
	}
	for i := 0; i < level; i++ {
		node.Forward[i] = update[i].Forward[i]
		update[i].Forward[i] = node
	}

	s.index.insert(node)

	s.addCount()
}

func (s *SkipList) Search(ts int64) *statsd.Metric {
	cur := s.header

	for i := s.level - 1; i >= 0; i-- {
		// if next node is < ts, move forward
		for cur.Forward[i] != nil && cur.Forward[i].Data.Timestamp < ts {
			cur = cur.Forward[i]
		}

		// if node == ts, return
		if cur.Forward[i] != nil && cur.Forward[i].Data.Timestamp == ts {
			return &cur.Forward[i].Data
		}
	}

	return nil
}

func (s *SkipList) SearchIndex(k string) []statsd.Metric {
	n := s.index[k]
	list := make([]statsd.Metric, len(n))
	for i := range n {
		list[i] = n[i].Data
	}

	return list
}

func (s *SkipList) SearchIndexRange(k string, r RangeQuery) []statsd.Metric {
	n := s.index[k]
	start := binarySearchStart(n, r.Start)
	end := binarySearchEnd(n, r.End)
	if start == -1 || end == -1 || start > end {
		return nil
	}

	var list []statsd.Metric
	for i := start; i <= end; i++ {
		list = append(list, n[i].Data)
	}

	return list
}

func (s *SkipList) Delete(ts int64) {
	update := make([]*Node, s.maxLevel)
	cur := s.header

	for i := s.level - 1; i >= 0; i-- {
		for cur.Forward[i] != nil && cur.Forward[i].Data.Timestamp < ts {
			cur = cur.Forward[i]
		}
		update[i] = cur
	}

	for i := 0; i < s.level; i++ {
		if update[i].Forward[i] != nil && update[i].Forward[i].Data.Timestamp == ts {
			update[i].Forward[i] = update[i].Forward[i].Forward[i]
		}
	}

	if s.level > 0 && s.header.Forward[s.level-1] == nil {
		s.level--
	}
}

type RangeQuery struct {
	Start int64
	End   int64
}

func (s *SkipList) RangeQuery(rq RangeQuery) []statsd.Metric {
	if rq.Start > rq.End {
		return nil
	}

	var points []statsd.Metric
	cur := s.header
	for i := s.level - 1; i >= 0; i-- {
		for cur.Forward[i] != nil && cur.Forward[i].Data.Timestamp < rq.Start {
			cur = cur.Forward[i]
		}
	}

	if cur.Data.Timestamp < rq.Start {
		cur = cur.Forward[0]
	}

	for cur != nil && cur.Data.Timestamp <= rq.End {
		points = append(points, cur.Data)
		cur = cur.Forward[0]
	}

	return points
}

func (s *SkipList) Count() int {
	s.l.RLock()
	v := s.count
	s.l.RUnlock()

	return v
}

func (s *SkipList) SearchKeyFull(key string) []statsd.Metric {
	cur := s.header
	var points []statsd.Metric

	for cur.Forward[0] != nil {
		if cur.Forward[0].Data.Key == key {
			points = append(points, cur.Forward[0].Data)
		}
		cur = cur.Forward[0]
	}

	return points
}

func (s *SkipList) randomLevel() int {
	level := 1
	for rand.Float32() < s.p && level < s.maxLevel {
		level++
	}

	return level
}

func (s *SkipList) updateMaxLevel() {
	prev := s.maxLevel
	m := math.Log2(float64(s.Count()))

	s.maxLevel = max(int(m), minLevel)
	if prev == s.maxLevel {
		return
	}

	for i := prev; i < s.maxLevel; i++ {
		s.header.Forward = append(s.header.Forward, nil)
	}
	//	log.Printf("UPDATED MAX LEVEL %d", s.maxLevel)
}

func (s *SkipList) addCount() {
	s.l.Lock()
	s.count++
	s.l.Unlock()
}

type Range interface {
	~struct {
		Start int64
		End   int64
	}
}

func binarySearchStart(nodes []*Node, start int64) int {
	low, high := 0, len(nodes)-1
	result := -1

	for low <= high {
		mid := low + (high-low)/2
		if nodes[mid].Data.Timestamp >= start {
			result = mid
			high = mid - 1
		} else {
			low = mid + 1
		}
	}

	return result
}

func binarySearchEnd(nodes []*Node, end int64) int {
	low, high := 0, len(nodes)-1
	result := -1

	for low <= high {
		mid := low + (high-low)/2
		if nodes[mid].Data.Timestamp <= end {
			result = mid
			low = mid + 1
		} else {
			high = mid - 1
		}
	}

	return result
}

func max(a, b int) int {
	if a > b {
		return a
	}

	return b
}
