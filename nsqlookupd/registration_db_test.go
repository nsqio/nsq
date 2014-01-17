package nsqlookupd

import (
	"github.com/bmizerany/assert"
	"io/ioutil"
	"log"
	"os"
	"testing"
	"time"
)

func TestRegistrationDB(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	defer log.SetOutput(os.Stdout)

	sec30 := 30 * time.Second
	beginningOfTime := time.Unix(1348797047, 0)
	pi1 := &PeerInfo{"1", "remote_addr:1", "host", "b_addr", 1, 2, "v1", beginningOfTime}
	pi2 := &PeerInfo{"2", "remote_addr:2", "host", "b_addr", 2, 3, "v1", beginningOfTime}
	pi3 := &PeerInfo{"3", "remote_addr:3", "host", "b_addr", 3, 4, "v1", beginningOfTime}
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
	assert.Equal(t, len(r), 1)
	assert.Equal(t, r[0], "a")

	p := db.FindProducers("t", "*", "")
	assert.Equal(t, len(p), 1)
	p = db.FindProducers("c", "*", "")
	assert.Equal(t, len(p), 2)
	p = db.FindProducers("c", "a", "")
	assert.Equal(t, len(p), 2)
	p = db.FindProducers("c", "*", "b")
	assert.Equal(t, len(p), 1)
	assert.Equal(t, p[0].peerInfo.id, p2.peerInfo.id)

	// filter by active
	assert.Equal(t, len(p.FilterByActive(sec30, sec30)), 0)
	p2.peerInfo.lastUpdate = time.Now()
	assert.Equal(t, len(p.FilterByActive(sec30, sec30)), 1)
	p = db.FindProducers("c", "*", "")
	assert.Equal(t, len(p.FilterByActive(sec30, sec30)), 1)

	// tombstoning
	fewSecAgo := time.Now().Add(-5 * time.Second)
	p1.peerInfo.lastUpdate = fewSecAgo
	p2.peerInfo.lastUpdate = fewSecAgo
	assert.Equal(t, len(p.FilterByActive(sec30, sec30)), 2)
	p1.Tombstone()
	assert.Equal(t, len(p.FilterByActive(sec30, sec30)), 1)
	time.Sleep(10 * time.Millisecond)
	assert.Equal(t, len(p.FilterByActive(sec30, 5*time.Millisecond)), 2)
	// make sure we can still retrieve p1 from another registration see #148
	assert.Equal(t, len(db.FindProducers("t", "*", "").FilterByActive(sec30, sec30)), 1)

	// keys and subkeys
	k := db.FindRegistrations("c", "b", "").Keys()
	assert.Equal(t, len(k), 0)
	k = db.FindRegistrations("c", "a", "").Keys()
	assert.Equal(t, len(k), 1)
	assert.Equal(t, k[0], "a")
	k = db.FindRegistrations("c", "*", "b").SubKeys()
	assert.Equal(t, len(k), 1)
	assert.Equal(t, k[0], "b")

	// removing producers
	db.RemoveProducer(Registration{"c", "a", ""}, p1.peerInfo.id)
	p = db.FindProducers("c", "*", "*")
	assert.Equal(t, len(p), 1)

	db.RemoveProducer(Registration{"c", "a", ""}, p2.peerInfo.id)
	db.RemoveProducer(Registration{"c", "a", "b"}, p2.peerInfo.id)
	p = db.FindProducers("c", "*", "*")
	assert.Equal(t, len(p), 0)

	// do some key removals
	k = db.FindRegistrations("c", "*", "*").Keys()
	assert.Equal(t, len(k), 2)
	db.RemoveRegistration(Registration{"c", "a", ""})
	db.RemoveRegistration(Registration{"c", "a", "b"})
	k = db.FindRegistrations("c", "*", "*").Keys()
	assert.Equal(t, len(k), 0)
}
