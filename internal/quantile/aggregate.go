package quantile

import (
	"fmt"
	"math"
	"sort"

	"github.com/bitly/go-simplejson"
)

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

func E2eProcessingLatencyAggregateFromJSON(j *simplejson.Json, topic, channel, host string) *E2eProcessingLatencyAggregate {
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

// Add merges e2 into e by averaging the percentiles
func (e *E2eProcessingLatencyAggregate) Add(e2 *E2eProcessingLatencyAggregate) {
	e.Addr = "*"
	p := e.Percentiles
	e.Count += e2.Count
	for _, value := range e2.Percentiles {
		i := -1
		for j, v := range p {
			if value["quantile"] == v["quantile"] {
				i = j
				break
			}
		}
		if i == -1 {
			i = len(p)
			e.Percentiles = append(p, make(map[string]float64))
			p = e.Percentiles
			p[i]["quantile"] = value["quantile"]
		}
		p[i]["max"] = math.Max(value["max"], p[i]["max"])
		p[i]["min"] = math.Min(value["max"], p[i]["max"])

		p[i]["count"] += value["count"]
		delta := value["average"] - p[i]["average"]
		R := delta * value["count"] / p[i]["count"]
		p[i]["average"] = p[i]["average"] + R
	}
	sort.Sort(e)
}
