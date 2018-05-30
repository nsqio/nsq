package nsqlookupd

import (
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/nsqio/nsq/internal/test"
)

type FakeRegistrationDB struct {
	sync.RWMutex
	registrationMap map[Registration]Producers
}

func NewFakeRegistrationDB() *FakeRegistrationDB {
	return &FakeRegistrationDB{
		registrationMap: make(map[Registration]Producers),
	}
}

// add a registration key
func (r *FakeRegistrationDB) AddRegistration(k Registration) {
	r.Lock()
	defer r.Unlock()
	_, ok := r.registrationMap[k]
	if !ok {
		r.registrationMap[k] = Producers{}
	}
}

// add a producer to a registration
func (r *FakeRegistrationDB) AddProducer(k Registration, p *Producer) bool {
	r.Lock()
	defer r.Unlock()
	producers := r.registrationMap[k]
	found := false
	for _, producer := range producers {
		if producer.peerInfo.id == p.peerInfo.id {
			found = true
			break
		}
	}
	if found == false {
		r.registrationMap[k] = append(producers, p)
	}
	return !found
}

func (r *FakeRegistrationDB) LookupRegistrations(id string) Registrations {
	r.RLock()
	defer r.RUnlock()
	results := Registrations{}
	for k, producers := range r.registrationMap {
		for _, p := range producers {
			if p.peerInfo.id == id {
				results = append(results, k)
				break
			}
		}
	}
	return results
}

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

func benchmarkLookupRegistrations(b *testing.B, registrationNumber int, producerNumber int) {
	regDB := NewRegistrationDB()
	for i := 0; i < registrationNumber; i++ {
		regKey := strconv.Itoa(i)
		reg := Registration{
			Category: regKey,
			Key:      regKey,
			SubKey:   regKey,
		}
		regDB.AddRegistration(reg)
		for j := 0; j < producerNumber; j++ {
			pKey := strconv.Itoa(j)
			p := Producer{
				peerInfo: &PeerInfo{
					id: pKey,
				},
			}
			regDB.AddProducer(reg, &p)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		i := rand.Intn(producerNumber)
		pKey := strconv.Itoa(i)
		regDB.LookupRegistrations(pKey)
	}
}

func BenchmarkLookupRegistrations64Registration64Producer(b *testing.B) {
	benchmarkLookupRegistrations(b, 64, 64)
}

func BenchmarkLookupRegistrations128Registration128Producer(b *testing.B) {
	benchmarkLookupRegistrations(b, 128, 128)
}

func BenchmarkLookupRegistrations256Registration256Producer(b *testing.B) {
	benchmarkLookupRegistrations(b, 256, 256)
}

func BenchmarkLookupRegistrations512Registration512Producer(b *testing.B) {
	benchmarkLookupRegistrations(b, 512, 512)
}

func BenchmarkLookupRegistrations1024Registration1024Producer(b *testing.B) {
	benchmarkLookupRegistrations(b, 1024, 1024)
}

func BenchmarkLookupRegistrations2048Registration2048Producer(b *testing.B) {
	benchmarkLookupRegistrations(b, 2048, 2048)
}

func benchmarkFakeLookupRegistrations(b *testing.B, registrationNumber int, producerNumber int) {
	regDB := NewFakeRegistrationDB()
	for i := 0; i < registrationNumber; i++ {
		regKey := strconv.Itoa(i)
		reg := Registration{
			Category: regKey,
			Key:      regKey,
			SubKey:   regKey,
		}
		regDB.AddRegistration(reg)
		for j := 0; j < producerNumber; j++ {
			pKey := strconv.Itoa(j)
			p := Producer{
				peerInfo: &PeerInfo{
					id: pKey,
				},
			}
			regDB.AddProducer(reg, &p)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		i := rand.Intn(producerNumber)
		pKey := strconv.Itoa(i)
		regDB.LookupRegistrations(pKey)
	}
}

func BenchmarkFakeLookupRegistrations64Registration64Producer(b *testing.B) {
	benchmarkFakeLookupRegistrations(b, 64, 64)
}

func BenchmarkFakeLookupRegistrations128Registration128Producer(b *testing.B) {
	benchmarkFakeLookupRegistrations(b, 128, 128)
}

func BenchmarkFakeLookupRegistrations256Registration256Producer(b *testing.B) {
	benchmarkFakeLookupRegistrations(b, 256, 256)
}

func BenchmarkFakeLookupRegistrations512Registration512Producer(b *testing.B) {
	benchmarkFakeLookupRegistrations(b, 512, 512)
}

func BenchmarkFakeLookupRegistrations1024Registration1024Producer(b *testing.B) {
	benchmarkFakeLookupRegistrations(b, 1024, 1024)
}

func BenchmarkFakeLookupRegistrations2048Registration2048Producer(b *testing.B) {
	benchmarkFakeLookupRegistrations(b, 2048, 2048)
}
