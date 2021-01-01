package nsqlookupd

import (
	"math/rand"
	"strconv"
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
	p1 := &Producer{pi1, false, beginningOfTime, false}
	p2 := &Producer{pi2, false, beginningOfTime, true}
	p3 := &Producer{pi3, false, beginningOfTime, false}
	p4 := &Producer{pi1, false, beginningOfTime, false}

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
	test.Equal(t, true, p.IsPaused())
	p = db.FindProducers("c", "*", "b")
	t.Logf("%s", p)
	test.Equal(t, 1, len(p))
	test.Equal(t, p2.peerInfo.id, p[0].peerInfo.id)
	test.Equal(t, true, p.IsPaused())

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

func fillRegDB(registrations int, producers int) *RegistrationDB {
	regDB := NewRegistrationDB()
	for i := 0; i < registrations; i++ {
		regT := Registration{"topic", "t" + strconv.Itoa(i), ""}
		regCa := Registration{"channel", "t" + strconv.Itoa(i), "ca" + strconv.Itoa(i)}
		regCb := Registration{"channel", "t" + strconv.Itoa(i), "cb" + strconv.Itoa(i)}
		for j := 0; j < producers; j++ {
			p := Producer{
				peerInfo: &PeerInfo{
					id: "p" + strconv.Itoa(j),
				},
			}
			regDB.AddProducer(regT, &p)
			regDB.AddProducer(regCa, &p)
			regDB.AddProducer(regCb, &p)
		}
	}
	return regDB
}

func fillRegDBV2(topics int, channels int, producers int) *RegistrationDB {
	regDB := NewRegistrationDB()
	for i := 0; i < topics; i++ {
		regT := Registration{"topic", "t" + strconv.Itoa(i), ""}
		for j := 0; j < producers; j++ {
			p := Producer{
				peerInfo: &PeerInfo{
					id: "p" + strconv.Itoa(j),
				},
			}
			regDB.AddProducer(regT, &p)
		}
		for k := 0; k < channels; k++ {
			regCa := Registration{"channel", "t" + strconv.Itoa(i), "ca" + strconv.Itoa(k)}
			for j := 0; j < producers; j++ {
				p := Producer{
					peerInfo: &PeerInfo{
						id: "p" + strconv.Itoa(j),
					},
				}
				regDB.AddProducer(regCa, &p)
			}
		}
	}
	return regDB
}

func benchmarkLookupRegistrations(b *testing.B, registrations int, producers int) {
	regDB := fillRegDB(registrations, producers)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		j := strconv.Itoa(rand.Intn(producers))
		_ = regDB.LookupRegistrations("p" + j)
	}
}

func benchmarkRegister(b *testing.B, registrations int, producers int) {
	for i := 0; i < b.N; i++ {
		_ = fillRegDB(registrations, producers)
	}
}

func benchmarkDoLookup(b *testing.B, registrations int, producers int) {
	regDB := fillRegDB(registrations, producers)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		topic := "t" + strconv.Itoa(rand.Intn(registrations))
		_ = regDB.FindRegistrations("topic", topic, "")
		_ = regDB.FindRegistrations("channel", topic, "*").SubKeys()
		_ = regDB.FindProducers("topic", topic, "")
	}
}

func benchmarkLookupSingleTopic(b *testing.B, registrations int, producers int) {
	regDB := fillRegDB(registrations, producers)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		topic := "t" + strconv.Itoa(rand.Intn(registrations))
		_ = regDB.FindProducers("topic", topic, "")
	}
}

func benchmarkLookupTopics(b *testing.B, registrations int, producers int) {
	regDB := fillRegDB(registrations, producers)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		topics := regDB.FindRegistrations("topic", "*", "").Keys()
		for _, topic := range topics {
			_ = regDB.FindProducers("topic", topic, "")
		}
	}
}

func benchmarkLookupChannels(b *testing.B, topics int, channels int, producers int) {
	regDB := fillRegDBV2(topics, channels, producers)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		topic := "t" + strconv.Itoa(rand.Intn(topics))
		channels := regDB.FindRegistrations("channel", topic, "*").SubKeys()
		for _, channel := range channels {
			_ = regDB.FindProducers("channel", topic, channel)
		}
	}
}

func BenchmarkLookupRegistrations8x8(b *testing.B) {
	benchmarkLookupRegistrations(b, 8, 8)
}

func BenchmarkLookupRegistrations8x64(b *testing.B) {
	benchmarkLookupRegistrations(b, 8, 64)
}

func BenchmarkLookupRegistrations64x64(b *testing.B) {
	benchmarkLookupRegistrations(b, 64, 64)
}

func BenchmarkLookupRegistrations64x512(b *testing.B) {
	benchmarkLookupRegistrations(b, 64, 512)
}

func BenchmarkLookupRegistrations512x512(b *testing.B) {
	benchmarkLookupRegistrations(b, 512, 512)
}

func BenchmarkLookupRegistrations512x2048(b *testing.B) {
	benchmarkLookupRegistrations(b, 512, 2048)
}

func BenchmarkRegister8x8(b *testing.B) {
	benchmarkRegister(b, 8, 8)
}

func BenchmarkRegister8x64(b *testing.B) {
	benchmarkRegister(b, 8, 64)
}

func BenchmarkRegister64x64(b *testing.B) {
	benchmarkRegister(b, 64, 64)
}

func BenchmarkRegister64x512(b *testing.B) {
	benchmarkRegister(b, 64, 512)
}

func BenchmarkRegister512x512(b *testing.B) {
	benchmarkRegister(b, 512, 512)
}

func BenchmarkRegister512x2048(b *testing.B) {
	benchmarkRegister(b, 512, 2048)
}

func BenchmarkDoLookup8x8(b *testing.B) {
	benchmarkDoLookup(b, 8, 8)
}

func BenchmarkDoLookup8x64(b *testing.B) {
	benchmarkDoLookup(b, 8, 64)
}

func BenchmarkDoLookup64x64(b *testing.B) {
	benchmarkDoLookup(b, 64, 64)
}

func BenchmarkDoLookup64x512(b *testing.B) {
	benchmarkDoLookup(b, 64, 512)
}

func BenchmarkDoLookup512x512(b *testing.B) {
	benchmarkDoLookup(b, 512, 512)
}

func BenchmarkDoLookup512x2048(b *testing.B) {
	benchmarkDoLookup(b, 512, 2048)
}

func BenchmarkLookupSingleTopic8x8(b *testing.B) {
	benchmarkLookupSingleTopic(b, 8, 8)
}

func BenchmarkLookupSingleTopic8x64(b *testing.B) {
	benchmarkLookupSingleTopic(b, 8, 64)
}

func BenchmarkLookupSingleTopic64x64(b *testing.B) {
	benchmarkLookupSingleTopic(b, 64, 64)
}

func BenchmarkLookupSingleTopic64x512(b *testing.B) {
	benchmarkLookupSingleTopic(b, 64, 512)
}

func BenchmarkLookupSingleTopic512x512(b *testing.B) {
	benchmarkLookupSingleTopic(b, 512, 512)
}

func BenchmarkLookupSingleTopic512x2048(b *testing.B) {
	benchmarkLookupSingleTopic(b, 512, 2048)
}

func BenchmarkLookupTopics8x8(b *testing.B) {
	benchmarkLookupTopics(b, 8, 8)
}

func BenchmarkLookupTopics8x64(b *testing.B) {
	benchmarkLookupTopics(b, 8, 64)
}

func BenchmarkLookupTopics64x64(b *testing.B) {
	benchmarkLookupTopics(b, 64, 64)
}

func BenchmarkLookupTopics64x512(b *testing.B) {
	benchmarkLookupTopics(b, 64, 512)
}

func BenchmarkLookupTopics512x512(b *testing.B) {
	benchmarkLookupTopics(b, 512, 512)
}

func BenchmarkLookupTopics512x2048(b *testing.B) {
	benchmarkLookupTopics(b, 512, 2048)
}

func BenchmarkLookupChannels8x8x8(b *testing.B) {
	benchmarkLookupChannels(b, 8, 8, 8)
}

func BenchmarkLookupChannels8x64x64(b *testing.B) {
	benchmarkLookupChannels(b, 8, 64, 64)
}

func BenchmarkLookupChannels64x64x64(b *testing.B) {
	benchmarkLookupChannels(b, 64, 64, 64)
}

//func BenchmarkLookupChannels64x512x512(b *testing.B) {
//	benchmarkLookupChannels(b, 64, 512, 512)
//}

//func BenchmarkLookupChannels512x512x512(b *testing.B) {
//	benchmarkLookupChannels(b, 512, 512, 512)
//}

//
//func BenchmarkLookupChannels512x2048x2048(b *testing.B) {
//	benchmarkLookupChannels(b, 512, 2048, 2048)
//}
