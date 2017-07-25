package nsqd

import (
	"fmt"
	"github.com/Workiva/go-datastructures/augmentedtree"
)

type QueueInterval interface {
	Start() int64
	End() int64
	EndCnt() uint64
	augmentedtree.Interval
}

type MsgQueueInterval struct {
	Start  int64
	End    int64
	EndCnt uint64
}

type queueInterval struct {
	start  int64
	end    int64
	endCnt uint64
}

func (self *queueInterval) Start() int64 {
	return self.start
}
func (self *queueInterval) End() int64 {
	return self.end
}
func (self *queueInterval) EndCnt() uint64 {
	return self.endCnt
}

// the augmentedtree use the low and the id to determin if the interval is the duplicate
// so here we use the end as the id of segment
func (self *queueInterval) ID() uint64 {
	return uint64(self.end)
}

func (self *queueInterval) LowAtDimension(dim uint64) int64 {
	return self.start
}

func (self *queueInterval) HighAtDimension(dim uint64) int64 {
	return self.end
}

func (self *queueInterval) OverlapsAtDimension(inter augmentedtree.Interval, dim uint64) bool {
	if inter.HighAtDimension(dim) < self.start {
		return false
	}
	if inter.LowAtDimension(dim) > self.end {
		return false
	}
	return true
}

type IntervalTree struct {
	tr augmentedtree.Tree
}

func NewIntervalTree() *IntervalTree {
	return &IntervalTree{
		tr: augmentedtree.New(1),
	}
}

// return the merged interval, if no overlap just return the original
func (self *IntervalTree) AddOrMerge(inter QueueInterval) QueueInterval {
	overlaps := self.tr.Query(inter)
	if len(overlaps) == 1 && overlaps[0].LowAtDimension(1) <= inter.LowAtDimension(1) &&
		overlaps[0].HighAtDimension(1) >= inter.HighAtDimension(1) {
		return overlaps[0].(QueueInterval)
	} else if len(overlaps) == 0 {
		self.tr.Add(inter)
		return inter
	} else {
		qi := &queueInterval{}
		qi.start = inter.Start()
		qi.end = inter.End()
		qi.endCnt = inter.EndCnt()
		for _, v := range overlaps {
			if v.LowAtDimension(0) < qi.LowAtDimension(0) {
				qi.start = v.LowAtDimension(0)
			}
			if v.HighAtDimension(0) > qi.HighAtDimension(0) {
				qi.end = v.HighAtDimension(0)
				qi.endCnt = v.(QueueInterval).EndCnt()
			}
			self.tr.Delete(v)
		}
		self.tr.Add(qi)
		return qi
	}
}

func (self *IntervalTree) Len() int {
	return int(self.tr.Len())
}

func (self *IntervalTree) ToIntervalList() []MsgQueueInterval {
	il := make([]MsgQueueInterval, 0, self.Len())
	self.tr.Traverse(func(inter augmentedtree.Interval) {
		qi, ok := inter.(QueueInterval)
		if ok {
			var mqi MsgQueueInterval
			mqi.Start = qi.Start()
			mqi.End = qi.End()
			mqi.EndCnt = qi.EndCnt()
			il = append(il, mqi)
		}
	})

	return il
}

func (self *IntervalTree) ToString() string {
	dataStr := ""
	self.tr.Traverse(func(inter augmentedtree.Interval) {
		dataStr += fmt.Sprintf("interval %v, ", inter)
	})
	return dataStr
}

func (self *IntervalTree) Delete(inter QueueInterval) {
	overlaps := self.tr.Query(inter)
	for _, v := range overlaps {
		if v.LowAtDimension(1) >= inter.LowAtDimension(1) &&
			v.HighAtDimension(1) <= inter.HighAtDimension(1) {
			self.tr.Delete(v)
		}
	}
}

func (self *IntervalTree) DeleteLower(low int64) int {
	qi := &queueInterval{
		start:  0,
		end:    low,
		endCnt: 0,
	}
	overlaps := self.tr.Query(qi)
	cnt := 0

	for _, v := range overlaps {
		if v.HighAtDimension(1) <= low {
			self.tr.Delete(v)
			cnt++
		}
	}
	return cnt
}

func (self *IntervalTree) IsOverlaps(inter QueueInterval, excludeBoard bool) bool {
	overlaps := self.tr.Query(inter)
	for _, v := range overlaps {
		if excludeBoard {
			if v.LowAtDimension(1) >= inter.HighAtDimension(1) {
				continue
			}
			if v.HighAtDimension(1) <= inter.LowAtDimension(1) {
				continue
			}
		}
		return true
	}
	return false
}

func (self *IntervalTree) Query(inter QueueInterval, excludeBoard bool) []QueueInterval {
	overlaps := self.tr.Query(inter)
	rets := make([]QueueInterval, 0, len(overlaps))
	for _, v := range overlaps {
		if excludeBoard {
			if v.LowAtDimension(1) >= inter.HighAtDimension(1) {
				continue
			}
			if v.HighAtDimension(1) <= inter.LowAtDimension(1) {
				continue
			}
		}
		rets = append(rets, v.(QueueInterval))
	}
	return rets
}

func (self *IntervalTree) IsLowestAt(low int64) QueueInterval {
	qi := &queueInterval{
		start:  0,
		end:    low,
		endCnt: 0,
	}
	overlaps := self.tr.Query(qi)
	if len(overlaps) == 0 {
		return nil
	} else if len(overlaps) == 1 {
		if overlaps[0].LowAtDimension(1) == low {
			return overlaps[0].(QueueInterval)
		}
		return nil
	}
	return nil
}
