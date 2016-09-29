package nsqlookupd

import (
	"testing"
	"time"
)

func TestRegistrationDB(t *testing.T) {
	sec30 := 30 * time.Second
	beginningOfTime := time.Unix(1348797047, 0)
	pi1 := &PeerInfo{beginningOfTime.UnixNano(), "1", "remote_addr:1", "host", "b_addr", 1, 2, "v1", "1"}
	pi2 := &PeerInfo{beginningOfTime.UnixNano(), "2", "remote_addr:2", "host", "b_addr", 2, 3, "v1", "2"}
	pi3 := &PeerInfo{beginningOfTime.UnixNano(), "3", "remote_addr:3", "host", "b_addr", 3, 4, "v1", "3"}
	pi5 := &PeerInfo{beginningOfTime.UnixNano(), "5", "remote_addr:5", "host", "b_addr", 5, 6, "v1", "5"}
	p1 := &Producer{pi1, false, beginningOfTime}
	p2 := &Producer{pi2, false, beginningOfTime}
	p3 := &Producer{pi3, false, beginningOfTime}
	p4 := &Producer{pi1, false, beginningOfTime}
	p5 := &Producer{pi5, false, beginningOfTime}

	db := NewRegistrationDB()

	// add client
	db.addPeerClient(pi1.Id, pi1)
	db.addPeerClient(pi2.Id, pi2)
	db.addPeerClient(pi3.Id, pi3)
	db.addPeerClient(p4.peerInfo.Id, pi1)
	peers := db.GetAllPeerClients()
	equal(t, len(peers), 3)
	// add producers
	db.AddTopicProducer("a", "0", p1)
	db.AddTopicProducer("a", "1", p1)
	db.AddTopicProducer("a", "0", p2)
	db.AddTopicProducer("b", "0", p3)
	db.AddTopicProducer("c", "0", p4)
	topics := db.FindTopics()
	equal(t, len(topics), 3)
	peerTopics := db.FindPeerTopics(pi1.Id)
	equal(t, len(peerTopics), 2)
	producers := db.FindTopicProducers("a", "0")
	equal(t, len(producers), 2)
	producers = db.FindTopicProducers("a", "1")
	equal(t, len(producers), 1)
	equal(t, producers[0].ProducerNode.peerInfo.Id, p1.peerInfo.Id)
	producers = db.FindTopicProducers("b", "*")
	equal(t, len(producers), 1)
	producers = db.FindTopicProducers("c", "*")
	equal(t, len(producers), 1)
	producers = db.FindTopicProducers("a", "*")
	equal(t, len(producers), 3)
	// filter by active
	equal(t, len(producers.FilterByActive(sec30, true)), 0)
	p2.peerInfo.lastUpdate = time.Now().UnixNano()
	equal(t, len(producers.FilterByActive(sec30, true)), 1)

	fewSecAgo := time.Now().Add(-5 * time.Second).UnixNano()
	p1.peerInfo.lastUpdate = fewSecAgo
	p2.peerInfo.lastUpdate = fewSecAgo
	equal(t, len(producers.FilterByActive(sec30, true)), 3)
	// p1 has two partition producer keys
	p1.Tombstone()
	equal(t, len(producers.FilterByActive(sec30, true)), 1)
	p1.tombstoned = false

	k := db.FindChannelRegs("a", "0")
	equal(t, len(k), 0)
	// keys and subkeys
	added := db.AddChannelReg("a", ChannelReg{"0", p1.peerInfo.Id, "ch_0"})
	equal(t, added, true)
	db.AddChannelReg("a", ChannelReg{"1", p1.peerInfo.Id, "ch_0"})
	k = db.FindChannelRegs("a", "0")
	equal(t, len(k), 1)
	equal(t, k[0].Channel, "ch_0")
	equal(t, k[0].PartitionID, "0")
	k = db.FindChannelRegs("a", "*")
	equal(t, len(k), 2)
	equal(t, len(k.Channels()), 1)
	// add duplicate
	added = db.AddChannelReg("a", ChannelReg{"0", p1.peerInfo.Id, "ch_0"})
	equal(t, added, false)

	db.AddChannelReg("a", ChannelReg{"0", p2.peerInfo.Id, "ch_0"})
	k = db.FindChannelRegs("a", "*")
	equal(t, len(k), 3)
	equal(t, len(k.Channels()), 1)
	db.AddChannelReg("a", ChannelReg{"0", p1.peerInfo.Id, "ch_1"})
	db.AddChannelReg("a", ChannelReg{"0", p2.peerInfo.Id, "ch_1"})
	k = db.FindChannelRegs("a", "*")
	equal(t, len(k), 5)
	equal(t, len(k.Channels()), 2)

	removed := db.RemoveTopicProducer("a", "1", p1.peerInfo.Id)
	equal(t, removed, true)
	producers = db.FindTopicProducers("a", "*")
	equal(t, len(producers), 2)
	db.RemoveChannelReg("a", ChannelReg{"0", p1.peerInfo.Id, "ch_0"})
	k = db.FindChannelRegs("a", "*")
	equal(t, len(k), 4)

	producers = db.FindTopicProducers("b", "0")
	equal(t, len(producers), 1)
	db.RemoveAllByPeerId(p3.peerInfo.Id)
	producers = db.FindTopicProducers("b", "0")
	equal(t, len(producers), 0)
	equal(t, len(db.FindChannelRegs("b", "*")), 0)

	db.RemoveAllByPeerId(p5.peerInfo.Id)
	db.FindTopicProducers("a", "*")
	equal(t, len(k), 4)
}
