package nsqd

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"github.com/nsqio/nsq/internal/test"
)

func TestPriorityQueue(t *testing.T) {
	c := 100
	pq := newInFlightPqueue(c)

	for i := 0; i < c+1; i++ {
		pq.Push(&Message{clientID: int64(i), pri: int64(i)})
	}
	test.Equal(t, c+1, len(pq))
	test.Equal(t, c*2, cap(pq))

	for i := 0; i < c+1; i++ {
		msg := pq.Pop()
		test.Equal(t, int64(i), msg.clientID)
	}
	test.Equal(t, c/4, cap(pq))
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
	test.Equal(t, c, len(pq))
	test.Equal(t, c, cap(pq))

	sort.Sort(sort.IntSlice(ints))

	for i := 0; i < c; i++ {
		msg, _ := pq.PeekAndShift(int64(ints[len(ints)-1]))
		test.Equal(t, int64(ints[i]), msg.pri)
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
		test.Equal(t, fmt.Sprintf("%s", fm.ID), fmt.Sprintf("%s", rm.ID))
	}

	lastPriority := pq.Pop().pri
	for i := 0; i < (c - 10 - 1); i++ {
		msg := pq.Pop()
		test.Equal(t, true, lastPriority <= msg.pri)
		lastPriority = msg.pri
	}
}
