package pqueue

import (
	"container/heap"
	"github.com/bmizerany/assert"
	"testing"
)

func TestDiskQueue(t *testing.T) {
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
