package nsqlookupd

import (
	"testing"
	"time"

	"github.com/nsqio/nsq/internal/test"
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
	db.AddProducer(Registration{"c", "a", ""}, p1)
	db.AddProducer(Registration{"c", "a", ""}, p2)
	db.AddProducer(Registration{"c", "a", "b"}, p2)
	db.AddProducer(Registration{"d", "a", ""}, p3)
	db.AddProducer(Registration{"t", "a", ""}, p4)

	// find producers
	r := db.FindRegistrations("c", "*", "").Keys()
	test.Equal(t, 1, len(r))
	test.Equal(t, "a", r[0])

	p := db.FindProducers("t", "*", "")
	t.Logf("%s", p)
	test.Equal(t, 1, len(p))
	p = db.FindProducers("c", "*", "")
	t.Logf("%s", p)
	test.Equal(t, 2, len(p))
	p = db.FindProducers("c", "a", "")
	t.Logf("%s", p)
	test.Equal(t, 2, len(p))
	p = db.FindProducers("c", "*", "b")
	t.Logf("%s", p)
	test.Equal(t, 1, len(p))
	test.Equal(t, p2.peerInfo.id, p[0].peerInfo.id)

	// filter by active
	test.Equal(t, 0, len(p.FilterByActive(sec30, sec30)))
	p2.peerInfo.lastUpdate = time.Now().UnixNano()
	test.Equal(t, 1, len(p.FilterByActive(sec30, sec30)))
	p = db.FindProducers("c", "*", "")
	t.Logf("%s", p)
	test.Equal(t, 1, len(p.FilterByActive(sec30, sec30)))

	// tombstoning
	fewSecAgo := time.Now().Add(-5 * time.Second).UnixNano()
	p1.peerInfo.lastUpdate = fewSecAgo
	p2.peerInfo.lastUpdate = fewSecAgo
	test.Equal(t, 2, len(p.FilterByActive(sec30, sec30)))
	p1.Tombstone()
	test.Equal(t, 1, len(p.FilterByActive(sec30, sec30)))
	time.Sleep(10 * time.Millisecond)
	test.Equal(t, 2, len(p.FilterByActive(sec30, 5*time.Millisecond)))
	// make sure we can still retrieve p1 from another registration see #148
	test.Equal(t, 1, len(db.FindProducers("t", "*", "").FilterByActive(sec30, sec30)))

	// keys and subkeys
	k := db.FindRegistrations("c", "b", "").Keys()
	test.Equal(t, 0, len(k))
	k = db.FindRegistrations("c", "a", "").Keys()
	test.Equal(t, 1, len(k))
	test.Equal(t, "a", k[0])
	k = db.FindRegistrations("c", "*", "b").SubKeys()
	test.Equal(t, 1, len(k))
	test.Equal(t, "b", k[0])

	// removing producers
	db.RemoveProducer(Registration{"c", "a", ""}, p1.peerInfo.id)
	p = db.FindProducers("c", "*", "*")
	t.Logf("%s", p)
	test.Equal(t, 1, len(p))

	db.RemoveProducer(Registration{"c", "a", ""}, p2.peerInfo.id)
	db.RemoveProducer(Registration{"c", "a", "b"}, p2.peerInfo.id)
	p = db.FindProducers("c", "*", "*")
	t.Logf("%s", p)
	test.Equal(t, 0, len(p))

	// do some key removals
	k = db.FindRegistrations("c", "*", "*").Keys()
	test.Equal(t, 2, len(k))
	db.RemoveRegistration(Registration{"c", "a", ""})
	db.RemoveRegistration(Registration{"c", "a", "b"})
	k = db.FindRegistrations("c", "*", "*").Keys()
	test.Equal(t, 0, len(k))
}
