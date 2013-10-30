package util

import (
	"fmt"
	"github.com/bitly/go-simplejson"
	"github.com/bmizerany/perks/quantile"
	"math"
	"sort"
	"strings"
	"sync"
	"time"
)

type PercentileResult struct {
	Count       int                  `json:"count"`
	Percentiles []map[string]float64 `json:"percentiles"`
}

func (pr *PercentileResult) String() string {
	var s []string
	for _, item := range pr.Percentiles {
		s = append(s, NanoSecondToHuman(item["value"]))
	}
	return strings.Join(s, ", ")
}

type Quantile struct {
	sync.Mutex
	streams        [2]quantile.Stream
	currentIndex   uint8
	lastMoveWindow time.Time
	currentStream  *quantile.Stream

	Percentiles    []float64
	MoveWindowTime time.Duration
}

func NewQuantile(WindowTime time.Duration, Percentiles []float64) *Quantile {
	q := Quantile{
		currentIndex:   0,
		lastMoveWindow: time.Now(),
		MoveWindowTime: WindowTime / 2,
		Percentiles:    Percentiles,
	}
	for i := 0; i < 2; i++ {
		q.streams[i] = *quantile.NewTargeted(Percentiles...)
	}
	q.currentStream = &q.streams[0]
	return &q
}

func (q *Quantile) PercentileResult() *PercentileResult {
	if q == nil {
		return &PercentileResult{}
	}
	queryHandler := q.QueryHandler()
	result := PercentileResult{
		Count:       queryHandler.Count(),
		Percentiles: make([]map[string]float64, len(q.Percentiles)),
	}
	for i, p := range q.Percentiles {
		value := queryHandler.Query(p)
		result.Percentiles[i] = map[string]float64{"quantile": p, "value": value}
	}
	return &result
}

func (q *Quantile) Insert(msgStartTime int64) {
	q.Lock()

	now := time.Now()
	for q.IsDataStale(now) {
		q.moveWindow()
	}

	q.currentStream.Insert(float64(now.UnixNano() - msgStartTime))
	q.Unlock()
}

func (q *Quantile) QueryHandler() *quantile.Stream {
	q.Lock()
	now := time.Now()
	for q.IsDataStale(now) {
		q.moveWindow()
	}

	merged := quantile.NewTargeted(q.Percentiles...)
	merged.Merge(q.streams[0].Samples())
	merged.Merge(q.streams[1].Samples())
	q.Unlock()
	return merged
}

func (q *Quantile) IsDataStale(now time.Time) bool {
	return now.After(q.lastMoveWindow.Add(q.MoveWindowTime))
}

func (q *Quantile) Merge(them *Quantile) {
	q.Lock()
	them.Lock()
	iUs := q.currentIndex
	iThem := them.currentIndex

	q.streams[iUs].Merge(them.streams[iThem].Samples())

	iUs ^= 0x1
	iThem ^= 0x1
	q.streams[iUs].Merge(them.streams[iThem].Samples())

	if q.lastMoveWindow.Before(them.lastMoveWindow) {
		q.lastMoveWindow = them.lastMoveWindow
	}
	q.Unlock()
	them.Unlock()
}

func (q *Quantile) moveWindow() {
	q.currentIndex ^= 0x1
	q.currentStream = &q.streams[q.currentIndex]
	q.lastMoveWindow = q.lastMoveWindow.Add(q.MoveWindowTime)
	q.currentStream.Reset()
}

type E2eProcessingLatencyAggregate struct {
	Count       int                  `json:"count"`
	Percentiles []map[string]float64 `json:"percentiles"`
	Topic       string               `json:"topic"`
	Channel     string               `json:"channel"`
	Addr        string               `json:"host"`
}

func (e *E2eProcessingLatencyAggregate) Target(key string) ([]string, string) {
	targets := make([]string, 0, len(e.Percentiles))
	var target string
	for _, percentile := range e.Percentiles {
		if e.Channel != "" {
			target = fmt.Sprintf(`%%stopic.%s.channel.%s.%s_%.0f`, e.Topic, e.Channel, key, percentile["quantile"]*100.0)
		} else {
			target = fmt.Sprintf(`%%stopic.%s.%s_%.0f`, e.Topic, key, percentile["quantile"]*100.0)
		}
		if e.Addr == "*" {
			target = fmt.Sprintf(`averageSeries(%s)`, target)
		}
		target = fmt.Sprintf(`scale(%s,0.000001)`, target)
		targets = append(targets, target)
	}
	return targets, ""
}

func (e *E2eProcessingLatencyAggregate) Host() string {
	return e.Addr
}

func E2eProcessingLatencyAggregateFromJson(j *simplejson.Json, topic, channel, host string) *E2eProcessingLatencyAggregate {
	count := j.Get("count").MustInt()

	rawPercentiles := j.Get("percentiles")
	numPercentiles := len(rawPercentiles.MustArray())
	percentiles := make([]map[string]float64, numPercentiles)

	for i := 0; i < numPercentiles; i++ {
		v := rawPercentiles.GetIndex(i)
		n := v.Get("value").MustFloat64()
		q := v.Get("quantile").MustFloat64()
		percentiles[i] = make(map[string]float64)
		percentiles[i]["min"] = n
		percentiles[i]["max"] = n
		percentiles[i]["average"] = n
		percentiles[i]["quantile"] = q
		percentiles[i]["count"] = float64(count)
	}

	return &E2eProcessingLatencyAggregate{
		Count:       count,
		Percentiles: percentiles,
		Topic:       topic,
		Channel:     channel,
		Addr:        host,
	}
}

func (e *E2eProcessingLatencyAggregate) Len() int { return len(e.Percentiles) }
func (e *E2eProcessingLatencyAggregate) Swap(i, j int) {
	e.Percentiles[i], e.Percentiles[j] = e.Percentiles[j], e.Percentiles[i]
}
func (e *E2eProcessingLatencyAggregate) Less(i, j int) bool {
	return e.Percentiles[i]["percentile"] > e.Percentiles[j]["percentile"]
}

func (A *E2eProcessingLatencyAggregate) Add(B *E2eProcessingLatencyAggregate, N int) *E2eProcessingLatencyAggregate {
	if A == nil {
		a := *B
		A = &a
	} else {
		ap := A.Percentiles
		bp := B.Percentiles
		A.Count += B.Count
		for _, value := range bp {
			indexA := -1
			for i, v := range ap {
				if value["quantile"] == v["quantile"] {
					indexA = i
					break
				}
			}
			if indexA == -1 {
				indexA = len(ap)
				A.Percentiles = append(ap, make(map[string]float64))
				ap = A.Percentiles
				ap[indexA]["quantile"] = value["quantile"]
			}
			ap[indexA]["max"] = math.Max(value["max"], ap[indexA]["max"])
			ap[indexA]["min"] = math.Min(value["max"], ap[indexA]["max"])

			ap[indexA]["count"] += value["count"]
			delta := value["average"] - ap[indexA]["average"]
			R := delta * value["count"] / ap[indexA]["count"]
			ap[indexA]["average"] = ap[indexA]["average"] + R
		}
	}
	sort.Sort(A)
	A.Addr = "*"
	return A
}
