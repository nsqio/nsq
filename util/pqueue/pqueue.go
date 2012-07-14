package pqueue

type Item struct {
	Value    interface{}
	Priority int64
	Index    int
}

type PriorityQueue struct {
	Items []*Item
}

func New(capacity int) PriorityQueue {
	return PriorityQueue{
		Items: make([]*Item, 0, capacity),
	}
}

func (pq PriorityQueue) Len() int {
	return len(pq.Items)
}

func (pq PriorityQueue) Less(i, j int) bool {
	return pq.Items[i].Priority > pq.Items[j].Priority
}

func (pq PriorityQueue) Swap(i, j int) {
	pq.Items[i], pq.Items[j] = pq.Items[j], pq.Items[i]
	pq.Items[i].Index = i
	pq.Items[j].Index = j
}

func (pq *PriorityQueue) Push(x interface{}) {
	n := len(pq.Items)
	c := cap(pq.Items)
	if n+1 > c {
		newItems := make([]*Item, n, c*2)
		copy(newItems, pq.Items)
		pq.Items = newItems
	}
	pq.Items = pq.Items[0 : n+1]
	item := x.(*Item)
	item.Index = n
	pq.Items[n] = item
}

func (pq *PriorityQueue) Pop() interface{} {
	n := len(pq.Items)
	item := pq.Items[n-1]
	item.Index = -1
	pq.Items = pq.Items[0 : n-1]
	return item
}

func (pq *PriorityQueue) Peek() interface{} {
	return pq.Items[len(pq.Items)-1]
}
