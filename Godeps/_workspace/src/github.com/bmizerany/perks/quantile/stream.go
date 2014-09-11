// Package quantile computes approximate quantiles over an unbounded data
// stream within low memory and CPU bounds.
//
// A small amount of accuracy is traded to achieve the above properties.
//
// Multiple streams can be merged before calling Query to generate a single set
// of results. This is meaningful when the streams represent the same type of
// data. See Merge and Samples.
//
// For more detailed information about the algorithm used, see:
//
// Effective Computation of Biased Quantiles over Data Streams
//
// http://www.cs.rutgers.edu/~muthu/bquant.pdf
package quantile

import (
	"math"
	"sort"
)

// Sample holds an observed value and meta information for compression. JSON
// tags have been added for convenience.
type Sample struct {
	Value float64 `json:",string"`
	Width float64 `json:",string"`
	Delta float64 `json:",string"`
}

// Samples represents a slice of samples. It implements sort.Interface.
type Samples []Sample

func (a Samples) Len() int           { return len(a) }
func (a Samples) Less(i, j int) bool { return a[i].Value < a[j].Value }
func (a Samples) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }

type invariant func(s *stream, r float64) float64

// NewBiased returns an initialized Stream for high-biased quantiles (e.g.
// 50th, 90th, 99th) not known a priori with finer error guarantees for the
// higher ranks of the data distribution.
// See http://www.cs.rutgers.edu/~muthu/bquant.pdf for time, space, and error properties.
func NewBiased() *Stream {
	ƒ := func(s *stream, r float64) float64 {
		return 2 * s.epsilon * r
	}
	return newStream(ƒ)
}

// NewTargeted returns an initialized Stream concerned with a particular set of
// quantile values that are supplied a priori. Knowing these a priori reduces
// space and computation time.
// See http://www.cs.rutgers.edu/~muthu/bquant.pdf for time, space, and error properties.
func NewTargeted(quantiles ...float64) *Stream {
	ƒ := func(s *stream, r float64) float64 {
		var m float64 = math.MaxFloat64
		var f float64
		for _, q := range quantiles {
			if q*s.n <= r {
				f = (2 * s.epsilon * r) / q
			} else {
				f = (2 * s.epsilon * (s.n - r)) / (1 - q)
			}
			if f < m {
				m = f
			}
		}
		return m
	}
	return newStream(ƒ)
}

// Stream computes quantiles for a stream of float64s. It is not thread-safe by
// design. Take care when using across multiple goroutines.
type Stream struct {
	*stream
	b      Samples
	sorted bool
}

func newStream(ƒ invariant) *Stream {
	const defaultEpsilon = 0.01
	x := &stream{epsilon: defaultEpsilon, ƒ: ƒ}
	return &Stream{x, make(Samples, 0, 500), true}
}

// Insert inserts v into the stream.
func (s *Stream) Insert(v float64) {
	s.insert(Sample{Value: v, Width: 1})
}

func (s *Stream) insert(sample Sample) {
	s.b = append(s.b, sample)
	s.sorted = false
	if len(s.b) == cap(s.b) {
		s.flush()
		s.compress()
	}
}

// Query returns the computed qth percentiles value. If s was created with
// NewTargeted, and q is not in the set of quantiles provided a priori, Query
// will return an unspecified result.
func (s *Stream) Query(q float64) float64 {
	if !s.flushed() {
		// Fast path when there hasn't been enough data for a flush;
		// this also yields better accuracy for small sets of data.
		l := len(s.b)
		if l == 0 {
			return 0
		}
		i := int(float64(l) * q)
		if i > 0 {
			i -= 1
		}
		s.maybeSort()
		return s.b[i].Value
	}
	s.flush()
	return s.stream.query(q)
}

// Merge merges samples into the underlying streams samples. This is handy when
// merging multiple streams from separate threads, database shards, etc.
func (s *Stream) Merge(samples Samples) {
	sort.Sort(samples)
	s.stream.merge(samples)
}

// Reset reinitializes and clears the list reusing the samples buffer memory.
func (s *Stream) Reset() {
	s.stream.reset()
	s.b = s.b[:0]
}

// Samples returns stream samples held by s.
func (s *Stream) Samples() Samples {
	if !s.flushed() {
		return s.b
	}
	s.flush()
	s.compress()
	return s.stream.samples()
}

// Count returns the total number of samples observed in the stream
// since initialization.
func (s *Stream) Count() int {
	return len(s.b) + s.stream.count()
}

func (s *Stream) flush() {
	s.maybeSort()
	s.stream.merge(s.b)
	s.b = s.b[:0]
}

func (s *Stream) maybeSort() {
	if !s.sorted {
		s.sorted = true
		sort.Sort(s.b)
	}
}

func (s *Stream) flushed() bool {
	return len(s.stream.l) > 0
}

type stream struct {
	epsilon float64
	n       float64
	l       []*Sample
	ƒ       invariant
}

// SetEpsilon sets the error epsilon for the Stream. The default epsilon is
// 0.01 and is usually satisfactory. If needed, this must be called before all
// Inserts.
// To learn more, see: http://www.cs.rutgers.edu/~muthu/bquant.pdf
func (s *stream) SetEpsilon(epsilon float64) {
	s.epsilon = epsilon
}

func (s *stream) reset() {
	s.l = s.l[:0]
	s.n = 0
}

func (s *stream) insert(v float64) {
	s.merge(Samples{{v, 1, 0}})
}

func (s *stream) merge(samples Samples) {
	var r float64
	i := 0
	for _, sample := range samples {
		for ; i < len(s.l); i++ {
			c := s.l[i]
			if c.Value > sample.Value {
				// Insert at position i.
				s.l = append(s.l, nil)
				copy(s.l[i+1:], s.l[i:])
				s.l[i] = &Sample{sample.Value, sample.Width, math.Floor(s.ƒ(s, r)) - 1}
				i++
				goto inserted
			}
			r += c.Width
		}
		s.l = append(s.l, &Sample{sample.Value, sample.Width, 0})
		i++
	inserted:
		s.n += sample.Width
	}
}

func (s *stream) count() int {
	return int(s.n)
}

func (s *stream) query(q float64) float64 {
	t := math.Ceil(q * s.n)
	t += math.Ceil(s.ƒ(s, t) / 2)
	p := s.l[0]
	r := float64(0)
	for _, c := range s.l[1:] {
		if r+c.Width+c.Delta > t {
			return p.Value
		}
		r += p.Width
		p = c
	}
	return p.Value
}

func (s *stream) compress() {
	if len(s.l) < 2 {
		return
	}
	x := s.l[len(s.l)-1]
	r := s.n - 1 - x.Width

	for i := len(s.l) - 2; i >= 0; i-- {
		c := s.l[i]
		if c.Width+x.Width+x.Delta <= s.ƒ(s, r) {
			x.Width += c.Width
			// Remove element at i.
			copy(s.l[i:], s.l[i+1:])
			s.l[len(s.l)-1] = nil
			s.l = s.l[:len(s.l)-1]
		} else {
			x = c
		}
		r -= c.Width
	}
}

func (s *stream) samples() Samples {
	samples := make(Samples, len(s.l))
	for i, c := range s.l {
		samples[i] = *c
	}
	return samples
}
