package timermetrics

import (
	"log"
	"math"
	"sort"
	"sync"
	"time"
)

type TimerMetrics struct {
	timings     durations
	prefix      string
	statusEvery int
	sync.Mutex
}

// start a new TimerMetrics to print out metrics every n times
func NewTimerMetrics(statusEvery int, prefix string) *TimerMetrics {
	s := &TimerMetrics{
		statusEvery: statusEvery,
		prefix:      prefix,
	}
	if statusEvery > 0 {
		s.timings = make(durations, 0, statusEvery)
	}
	return s
}

type durations []time.Duration

func (s durations) Len() int           { return len(s) }
func (s durations) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s durations) Less(i, j int) bool { return s[i] < s[j] }

func percentile(perc float64, arr []time.Duration, length int) time.Duration {
	if length == 0 {
		return 0
	}
	indexOfPerc := int(math.Ceil(((perc / 100.0) * float64(length)) + 0.5))
	if indexOfPerc >= length {
		indexOfPerc = length - 1
	}
	return arr[indexOfPerc]
}

func (m *TimerMetrics) Status(startTime time.Time) {
	if m.statusEvery > 0 {
		m.StatusDuration(time.Now().Sub(startTime))
	}
}

func (m *TimerMetrics) StatusDuration(duration time.Duration) {
	if m.statusEvery <= 0 {
		return
	}

	m.Lock()
	m.timings = append(m.timings, duration)

	l := len(m.timings)
	if l >= m.statusEvery {
		var total time.Duration
		for _, v := range m.timings {
			total += v
		}
		avgMs := (total.Seconds() * 1000) / float64(l)

		sort.Sort(m.timings)
		p95Ms := percentile(95.0, m.timings, l).Seconds() * 1000
		p99Ms := percentile(99.0, m.timings, l).Seconds() * 1000

		log.Printf("%s %d - 99th: %.02fms - 95th: %.02fms - avg: %.02fms", m.prefix, m.statusEvery, p99Ms, p95Ms, avgMs)
		m.timings = m.timings[:0]
	}
	m.Unlock()
}
