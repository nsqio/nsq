package quantile

import (
	"testing"
)

func BenchmarkInsertTargeted(b *testing.B) {
	s := NewTargeted(0.01, 0.5, 0.9, 0.99)
	b.ResetTimer()
	for i := float64(0); i < float64(b.N); i++ {
		s.Insert(i)
	}
}

func BenchmarkInsertTargetedSmallEpsilon(b *testing.B) {
	s := NewTargeted(0.01, 0.5, 0.9, 0.99)
	s.SetEpsilon(0.0001)
	b.ResetTimer()
	for i := float64(0); i < float64(b.N); i++ {
		s.Insert(i)
	}
}

func BenchmarkInsertBiased(b *testing.B) {
	s := NewBiased()
	b.ResetTimer()
	for i := float64(0); i < float64(b.N); i++ {
		s.Insert(i)
	}
}

func BenchmarkInsertBiasedSmallEpsilon(b *testing.B) {
	s := NewBiased()
	s.SetEpsilon(0.0001)
	b.ResetTimer()
	for i := float64(0); i < float64(b.N); i++ {
		s.Insert(i)
	}
}

func BenchmarkQuery(b *testing.B) {
	s := NewTargeted(0.01, 0.5, 0.9, 0.99)
	for i := float64(0); i < 1e6; i++ {
		s.Insert(i)
	}
	b.ResetTimer()
	n := float64(b.N)
	for i := float64(0); i < n; i++ {
		s.Query(i / n)
	}
}

func BenchmarkQuerySmallEpsilon(b *testing.B) {
	s := NewTargeted(0.01, 0.5, 0.9, 0.99)
	s.SetEpsilon(0.0001)
	for i := float64(0); i < 1e6; i++ {
		s.Insert(i)
	}
	b.ResetTimer()
	n := float64(b.N)
	for i := float64(0); i < n; i++ {
		s.Query(i / n)
	}
}
