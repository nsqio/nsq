package pqueue

import (
	"container/heap"
	"github.com/bmizerany/assert"
	"math/rand"
	"sort"
	"testing"
)

type Reverse struct {
	sort.Interface
}

func (r Reverse) Less(i, j int) bool {
	return r.Interface.Less(j, i)
}

func TestPriorityQueue(t *testing.T) {
	c := 100
	pq := New(c)

	for i := 0; i < c+1; i++ {
		heap.Push(&pq, &Item{Value: i, Priority: int64(i)})
	}
	assert.Equal(t, pq.Len(), c+1)
	assert.Equal(t, cap(pq), c*2)

	for i := 0; i < c+1; i++ {
		item := heap.Pop(&pq)
		assert.Equal(t, item.(*Item).Value.(int), c-i)
	}
	assert.Equal(t, cap(pq), c/4)
}

func TestUnsortedInsert(t *testing.T) {
	c := 100
	pq := New(c)
	ints := make([]int, 0, c)

	for i := 0; i < c; i++ {
		v := rand.Int()
		ints = append(ints, v)
		heap.Push(&pq, &Item{Value: i, Priority: int64(v)})
	}
	assert.Equal(t, pq.Len(), c)
	assert.Equal(t, cap(pq), c)

	sort.Sort(Reverse{sort.IntSlice(ints)})

	firstItem, _ := pq.PeekAndShift(int64(ints[c-1]))
	assert.Equal(t, firstItem.Priority, int64(ints[0]))
	for i := 1; i < c; i++ {
		item := heap.Pop(&pq)
		assert.Equal(t, item.(*Item).Priority, int64(ints[i]))
	}
}

func TestRemove(t *testing.T) {
	c := 100
	pq := New(c)

	for i := 0; i < c; i++ {
		v := rand.Int()
		heap.Push(&pq, &Item{Value: "test", Priority: int64(v)})
	}

	for i := 0; i < 10; i++ {
		heap.Remove(&pq, rand.Intn((c-1)-i))
	}

	lastPriority := heap.Pop(&pq).(*Item).Priority
	for i := 0; i < (c - 10 - 1); i++ {
		item := heap.Pop(&pq)
		assert.Equal(t, lastPriority > item.(*Item).Priority, true)
		lastPriority = item.(*Item).Priority
	}
}
