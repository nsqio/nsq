package nsqd

import (
	"math/rand"
	"sort"
	"testing"

	"github.com/bmizerany/assert"
)

func TestPriorityQueue(t *testing.T) {
	c := 100
	pq := newInFlightPqueue(c)

	for i := 0; i < c+1; i++ {
		pq.Push(&Message{clientID: int64(i), pri: int64(i)})
	}
	assert.Equal(t, len(pq), c+1)
	assert.Equal(t, cap(pq), c*2)

	for i := 0; i < c+1; i++ {
		msg := pq.Pop()
		assert.Equal(t, msg.clientID, int64(i))
	}
	assert.Equal(t, cap(pq), c/4)
}

func TestUnsortedInsert(t *testing.T) {
	c := 100
	pq := newInFlightPqueue(c)
	ints := make([]int, 0, c)

	for i := 0; i < c; i++ {
		v := rand.Int()
		ints = append(ints, v)
		pq.Push(&Message{pri: int64(v)})
	}
	assert.Equal(t, len(pq), c)
	assert.Equal(t, cap(pq), c)

	sort.Sort(sort.IntSlice(ints))

	for i := 0; i < c; i++ {
		msg, _ := pq.PeekAndShift(int64(ints[len(ints)-1]))
		assert.Equal(t, msg.pri, int64(ints[i]))
	}
}

func TestRemove(t *testing.T) {
	c := 100
	pq := newInFlightPqueue(c)

	for i := 0; i < c; i++ {
		v := rand.Int()
		pq.Push(&Message{pri: int64(v)})
	}

	for i := 0; i < 10; i++ {
		pq.Remove(rand.Intn((c - 1) - i))
	}

	lastPriority := pq.Pop().pri
	for i := 0; i < (c - 10 - 1); i++ {
		msg := pq.Pop()
		assert.Equal(t, lastPriority < msg.pri, true)
		lastPriority = msg.pri
	}
}
