package nsqlookupd

import (
	"testing"
	"time"
)

func TestRegistrationDB(t *testing.T) {
	sec30 := 30 * time.Second
	beginningOfTime := time.Unix(1348797047, 0)
	pi1 := &PeerInfo{beginningOfTime.UnixNano(), "1", "remote_addr:1", "host", "b_addr", 1, 2, "v1"}
	pi2 := &PeerInfo{beginningOfTime.UnixNano(), "2", "remote_addr:2", "host", "b_addr", 2, 3, "v1"}
	pi3 := &PeerInfo{beginningOfTime.UnixNano(), "3", "remote_addr:3", "host", "b_addr", 3, 4, "v1"}
	p1 := &Producer{pi1, false, beginningOfTime}
	p2 := &Producer{pi2, false, beginningOfTime}
	p3 := &Producer{pi3, false, beginningOfTime}
	p4 := &Producer{pi1, false, beginningOfTime}

	db := NewRegistrationDB()

	// add producers
	db.AddProducer(Registration{"c", "a", "", "0"}, p1)
	db.AddProducer(Registration{"c", "a", "", "0"}, p2)
	db.AddProducer(Registration{"c", "a", "b", "0"}, p2)
	db.AddProducer(Registration{"d", "a", "", "0"}, p3)
	db.AddProducer(Registration{"t", "a", "", "0"}, p4)

	// find producers
	r := db.FindRegistrations("c", "*", "", "0").Keys()
	equal(t, len(r), 1)
	equal(t, r[0], "a")

	p := db.FindProducers("t", "*", "", "0")
	equal(t, len(p), 1)
	p = db.FindProducers("c", "*", "", "0")
	equal(t, len(p), 2)
	p = db.FindProducers("c", "a", "", "0")
	equal(t, len(p), 2)
	p = db.FindProducers("c", "*", "b", "0")
	equal(t, len(p), 1)
	equal(t, p[0].peerInfo.Id, p2.peerInfo.Id)

	// filter by active
	equal(t, len(p.FilterByActive(sec30, sec30)), 0)
	p2.peerInfo.lastUpdate = time.Now().UnixNano()
	equal(t, len(p.FilterByActive(sec30, sec30)), 1)
	p = db.FindProducers("c", "*", "", "0")
	equal(t, len(p.FilterByActive(sec30, sec30)), 1)

	// tombstoning
	fewSecAgo := time.Now().Add(-5 * time.Second).UnixNano()
	p1.peerInfo.lastUpdate = fewSecAgo
	p2.peerInfo.lastUpdate = fewSecAgo
	equal(t, len(p.FilterByActive(sec30, sec30)), 2)
	p1.Tombstone()
	equal(t, len(p.FilterByActive(sec30, sec30)), 1)
	time.Sleep(10 * time.Millisecond)
	equal(t, len(p.FilterByActive(sec30, 5*time.Millisecond)), 2)
	// make sure we can still retrieve p1 from another registration see #148
	equal(t, len(db.FindProducers("t", "*", "", "0").FilterByActive(sec30, sec30)), 1)

	// keys and subkeys
	k := db.FindRegistrations("c", "b", "", "0").Keys()
	equal(t, len(k), 0)
	k = db.FindRegistrations("c", "a", "", "0").Keys()
	equal(t, len(k), 1)
	equal(t, k[0], "a")
	k = db.FindRegistrations("c", "*", "b", "0").SubKeys()
	equal(t, len(k), 1)
	equal(t, k[0], "b")

	// removing producers
	db.RemoveProducer(Registration{"c", "a", "", "0"}, p1.peerInfo.Id)
	p = db.FindProducers("c", "*", "*", "0")
	equal(t, len(p), 1)

	db.RemoveProducer(Registration{"c", "a", "", "0"}, p2.peerInfo.Id)
	db.RemoveProducer(Registration{"c", "a", "b", "0"}, p2.peerInfo.Id)
	p = db.FindProducers("c", "*", "*", "0")
	equal(t, len(p), 0)

	// do some key removals
	regs := db.FindRegistrations("c", "*", "*", "0")
	k = regs.Keys()
	equal(t, len(regs), 2)
	equal(t, len(k), 1)
	db.RemoveRegistration(Registration{"c", "a", "", "0"})
	db.RemoveRegistration(Registration{"c", "a", "b", "0"})
	k = db.FindRegistrations("c", "*", "*", "0").Keys()
	equal(t, len(k), 0)
}
