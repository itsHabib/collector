package statsd

import "encoding/json"

const (
	MaxTags = 50
)

type Type string

const (
	CounterType Type = "c"
	GaugeType   Type = "g"
	TimingType  Type = "ms"
)

func ToStatsDType(s string) Type {
	switch s {
	case "c":
		return CounterType
	case "g":
		return GaugeType
	case "ms":
		return TimingType
	default:
		return ""
	}
}

type Metric struct {
	Name      string  `json:"name"`
	Value     float64 `json:"value"`
	Type      Type    `json:"type"`
	Timestamp int64   `json:"timestamp"`
	Tags      *Tags   `json:"tags"`
	Key       string  `json:"-"`
}

func (m *Metric) Copy() Metric {
	return Metric{
		Name:      m.Name,
		Value:     m.Value,
		Type:      m.Type,
		Timestamp: m.Timestamp,
		Tags:      NewType(m.Tags),
		Key:       m.Key,
	}
}

func (m *Metric) MarshalJSON() ([]byte, error) {
	type Alias Metric
	return json.Marshal(&struct {
		Tags map[string]string `json:"tags,omitempty"`
		*Alias
	}{
		Tags:  m.TagsMap(),
		Alias: (*Alias)(m),
	})
}

type Tag struct {
	Key   string
	Value string
}

type Tags struct {
	items []Tag
}

func (t *Tags) Count() int {
	return len(t.items)
}

func (t *Tags) String() string {
	if len(t.items) == 0 {
		return ""
	}

	var s string
	for i := range t.items {
		s += t.items[i].Key + ":" + t.items[i].Value
		if i < len(t.items)-1 {
			s += ","
		}
	}

	return s
}

func (t *Tags) Add(tag Tag) bool {
	if len(t.items) == MaxTags {
		return false
	}

	var (
		low  = 0
		high = len(t.items) - 1
	)
	for low <= high {
		mid := low + (high-low)/2
		if t.items[mid].Key <= tag.Key {
			low = mid + 1
		} else {
			high = mid - 1
		}
	}

	if low == len(t.items) {
		t.items = append(t.items, tag)
		return true
	}

	t.items = append(t.items, Tag{})
	for i := len(t.items) - 1; i > low; i-- {
		t.items[i] = t.items[i-1]
	}
	t.items[low] = tag

	return true
}

func (m *Metric) TagsMap() map[string]string {
	if m.Tags == nil {
		return nil
	}

	tm := make(map[string]string, len(m.Tags.items))
	for i := 0; i < len(m.Tags.items); i++ {
		tm[m.Tags.items[i].Key] = m.Tags.items[i].Value
	}

	return tm
}

func FormMetricKey(name string, tags Tags) string {
	if len(tags.items) == 0 {
		return name
	}

	return name + "|" + tags.String()
}

func FromMap(m map[string]string) Tags {
	if len(m) == 0 {
		return Tags{}
	}
	t := Tags{
		items: make([]Tag, 0, len(m)),
	}
	for k, v := range m {
		t.Add(Tag{
			Key:   k,
			Value: v,
		})
	}

	return t
}

func NewType[T any](t *T) *T {
	if t == nil {
		return nil
	}

	n := new(T)
	*n = *t

	return n
}
