package nsqd

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"
)

func TestPriorityQueue(t *testing.T) {
	c := 100
	pq := newInFlightPqueue(c)

	for i := 0; i < c+1; i++ {
		pq.Push(&Message{clientID: int64(i), pri: int64(i)})
	}
	equal(t, len(pq), c+1)
	equal(t, cap(pq), c*2)

	for i := 0; i < c+1; i++ {
		msg := pq.Pop()
		equal(t, msg.clientID, int64(i))
	}
	equal(t, cap(pq), c/4)
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
	equal(t, len(pq), c)
	equal(t, cap(pq), c)

	sort.Sort(sort.IntSlice(ints))

	for i := 0; i < c; i++ {
		msg, _ := pq.PeekAndShift(int64(ints[len(ints)-1]))
		equal(t, msg.pri, int64(ints[i]))
	}
}

func TestRemove(t *testing.T) {
	c := 100
	pq := newInFlightPqueue(c)

	msgs := make(map[MessageID]*Message)
	for i := 0; i < c; i++ {
		m := &Message{pri: int64(rand.Intn(100000000))}
		copy(m.ID[:], fmt.Sprintf("%016d", m.pri))
		msgs[m.ID] = m
		pq.Push(m)
	}

	for i := 0; i < 10; i++ {
		idx := rand.Intn((c - 1) - i)
		var fm *Message
		for _, m := range msgs {
			if m.index == idx {
				fm = m
				break
			}
		}
		rm := pq.Remove(idx)
		equal(t, fmt.Sprintf("%s", fm.ID), fmt.Sprintf("%s", rm.ID))
	}

	lastPriority := pq.Pop().pri
	for i := 0; i < (c - 10 - 1); i++ {
		msg := pq.Pop()
		equal(t, lastPriority <= msg.pri, true)
		lastPriority = msg.pri
	}
}
