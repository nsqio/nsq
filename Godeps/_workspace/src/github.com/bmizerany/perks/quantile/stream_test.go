package quantile

import (
	"math"
	"math/rand"
	"sort"
	"testing"
)

func TestQuantRandQuery(t *testing.T) {
	s := NewTargeted(0.5, 0.90, 0.99)
	a := make([]float64, 0, 1e5)
	rand.Seed(42)
	for i := 0; i < cap(a); i++ {
		v := float64(rand.Int63())
		s.Insert(v)
		a = append(a, v)
	}
	t.Logf("len: %d", s.Count())
	sort.Float64s(a)
	w := getPerc(a, 0.50)
	if g := s.Query(0.50); math.Abs(w-g)/w > 0.03 {
		t.Errorf("perc50: want %v, got %v", w, g)
		t.Logf("e: %f", math.Abs(w-g)/w)
	}
	w = getPerc(a, 0.90)
	if g := s.Query(0.90); math.Abs(w-g)/w > 0.03 {
		t.Errorf("perc90: want %v, got %v", w, g)
		t.Logf("e: %f", math.Abs(w-g)/w)
	}
	w = getPerc(a, 0.99)
	if g := s.Query(0.99); math.Abs(w-g)/w > 0.03 {
		t.Errorf("perc99: want %v, got %v", w, g)
		t.Logf("e: %f", math.Abs(w-g)/w)
	}
}

func TestQuantRandMergeQuery(t *testing.T) {
	ch := make(chan float64)
	done := make(chan *Stream)
	for i := 0; i < 2; i++ {
		go func() {
			s := NewTargeted(0.5, 0.90, 0.99)
			for v := range ch {
				s.Insert(v)
			}
			done <- s
		}()
	}

	rand.Seed(42)
	a := make([]float64, 0, 1e6)
	for i := 0; i < cap(a); i++ {
		v := float64(rand.Int63())
		a = append(a, v)
		ch <- v
	}
	close(ch)

	s := <-done
	o := <-done
	s.Merge(o.Samples())

	t.Logf("len: %d", s.Count())
	sort.Float64s(a)
	w := getPerc(a, 0.50)
	if g := s.Query(0.50); math.Abs(w-g)/w > 0.03 {
		t.Errorf("perc50: want %v, got %v", w, g)
		t.Logf("e: %f", math.Abs(w-g)/w)
	}
	w = getPerc(a, 0.90)
	if g := s.Query(0.90); math.Abs(w-g)/w > 0.03 {
		t.Errorf("perc90: want %v, got %v", w, g)
		t.Logf("e: %f", math.Abs(w-g)/w)
	}
	w = getPerc(a, 0.99)
	if g := s.Query(0.99); math.Abs(w-g)/w > 0.03 {
		t.Errorf("perc99: want %v, got %v", w, g)
		t.Logf("e: %f", math.Abs(w-g)/w)
	}
}

func TestUncompressed(t *testing.T) {
	tests := []float64{0.50, 0.90, 0.95, 0.99}
	q := NewTargeted(tests...)
	for i := 100; i > 0; i-- {
		q.Insert(float64(i))
	}
	if g := q.Count(); g != 100 {
		t.Errorf("want count 100, got %d", g)
	}
	// Before compression, Query should have 100% accuracy.
	for _, v := range tests {
		w := v * 100
		if g := q.Query(v); g != w {
			t.Errorf("want %f, got %f", w, g)
		}
	}
}

func TestUncompressedSamples(t *testing.T) {
	q := NewTargeted(0.99)
	for i := 1; i <= 100; i++ {
		q.Insert(float64(i))
	}
	if g := q.Samples().Len(); g != 100 {
		t.Errorf("want count 100, got %d", g)
	}
}

func TestUncompressedOne(t *testing.T) {
	q := NewTargeted(0.90)
	q.Insert(3.14)
	if g := q.Query(0.90); g != 3.14 {
		t.Error("want PI, got", g)
	}
}

func TestDefaults(t *testing.T) {
	if g := NewTargeted(0.99).Query(0.99); g != 0 {
		t.Errorf("want 0, got %f", g)
	}
}

func getPerc(x []float64, p float64) float64 {
	k := int(float64(len(x)) * p)
	return x[k]
}
