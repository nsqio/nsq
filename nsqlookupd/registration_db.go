package nsqlookupd

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type RegistrationDB struct {
	sync.RWMutex
	registrationChannelMap map[string]ChannelRegistrations
	registrationTopicMap   map[string]TopicRegistrations
	registrationNodeMap    map[string]*PeerInfo
}

type ChannelReg struct {
	PartitionID string
	PeerId      string
	Channel     string
}

type PeerInfo struct {
	lastUpdate       int64
	Id               string `json:"id"`
	RemoteAddress    string `json:"remote_address"`
	Hostname         string `json:"hostname"`
	BroadcastAddress string `json:"broadcast_address"`
	TCPPort          int    `json:"tcp_port"`
	HTTPPort         int    `json:"http_port"`
	Version          string `json:"version"`
	// the node id used in the cluster.
	DistributedID string `json:"distributed_id"`
}

func (self *PeerInfo) IsOldPeer() bool {
	if self.DistributedID == "" {
		// this is old nsqd node not in the HA cluster !!
		// check the version
		if !strings.Contains(self.Version, "HA") {
			return true
		}
	}
	return false
}

type Producer struct {
	peerInfo     *PeerInfo
	tombstoned   bool
	tombstonedAt time.Time
}

type Producers []*Producer
type PeerInfoList []*PeerInfo

func (p *Producer) String() string {
	return fmt.Sprintf("%s [%d, %d]", p.peerInfo.BroadcastAddress, p.peerInfo.TCPPort, p.peerInfo.HTTPPort)
}

func (p *Producer) UndoTombstone() {
	p.tombstoned = false
}

func (p *Producer) Tombstone() {
	p.tombstoned = true
	p.tombstonedAt = time.Now()
}

func (p *Producer) IsTombstoned() bool {
	return p.tombstoned
}

type TopicProducerReg struct {
	PartitionID  string
	ProducerNode *Producer
}

type ChannelRegistrations []ChannelReg
type TopicRegistrations []TopicProducerReg

func NewRegistrationDB() *RegistrationDB {
	return &RegistrationDB{
		registrationChannelMap: make(map[string]ChannelRegistrations),
		registrationTopicMap:   make(map[string]TopicRegistrations),
		registrationNodeMap:    make(map[string]*PeerInfo),
	}
}

// add a registration key
func (r *RegistrationDB) AddChannelReg(topic string, k ChannelReg) bool {
	r.Lock()
	defer r.Unlock()
	channels, ok := r.registrationChannelMap[topic]
	if !ok {
		channels = make(ChannelRegistrations, 0)
		r.registrationChannelMap[topic] = channels
	}
	for _, ch := range channels {
		if ch == k {
			return false
		}
	}
	channels = append(channels, k)
	r.registrationChannelMap[topic] = channels
	return true
}

func (r *RegistrationDB) FindChannelRegs(topic string, pid string) ChannelRegistrations {
	results := ChannelRegistrations{}
	r.RLock()
	defer r.RUnlock()
	channels, ok := r.registrationChannelMap[topic]
	if !ok {
		return results
	}
	for _, ch := range channels {
		if ch.IsMatch(pid, "*", "*") {
			results = append(results, ch)
		}
	}
	return results
}

func (r *RegistrationDB) RemoveChannelReg(topic string, k ChannelReg) bool {
	r.Lock()
	defer r.Unlock()
	channels, ok := r.registrationChannelMap[topic]
	if !ok {
		return false
	}
	cleaned := ChannelRegistrations{}
	removed := false
	for _, ch := range channels {
		if ch.IsMatch(k.PartitionID, k.PeerId, k.Channel) {
			removed = true
		} else {
			cleaned = append(cleaned, ch)
		}
	}
	if removed {
		if len(cleaned) == 0 {
			delete(r.registrationChannelMap, topic)
		} else {
			r.registrationChannelMap[topic] = cleaned
		}
	}
	return removed
}

func (r *RegistrationDB) addPeerClient(id string, p *PeerInfo) bool {
	r.Lock()
	defer r.Unlock()
	_, ok := r.registrationNodeMap[id]
	if ok {
		return false
	}
	r.registrationNodeMap[id] = p
	return true
}

func (r *RegistrationDB) SearchPeerClientByID(id string) *PeerInfo {
	r.RLock()
	defer r.RUnlock()
	p, _ := r.registrationNodeMap[id]
	return p
}

func (r *RegistrationDB) SearchPeerClientByClusterID(id string) *PeerInfo {
	r.RLock()
	defer r.RUnlock()
	for _, n := range r.registrationNodeMap {
		if n.DistributedID == id {
			return n
		}
	}
	return nil
}

func (r *RegistrationDB) GetAllPeerClients() PeerInfoList {
	r.RLock()
	defer r.RUnlock()
	retList := make(PeerInfoList, 0, len(r.registrationNodeMap))
	for _, n := range r.registrationNodeMap {
		retList = append(retList, n)
	}
	return retList
}

func (r *RegistrationDB) AddTopicProducer(topic string, pidStr string, p *Producer) bool {
	if pidStr == "" {
		return false
	}
	pid, err := strconv.Atoi(pidStr)
	if err != nil || pid < 0 {
		if pid != OLD_VERSION_PID {
			return false
		}
	}

	r.Lock()
	defer r.Unlock()
	producers, ok := r.registrationTopicMap[topic]
	if !ok {
		producers = TopicRegistrations{}
		r.registrationTopicMap[topic] = producers
	}
	exist := false
	for _, producerReg := range producers {
		if producerReg.PartitionID == pidStr &&
			producerReg.ProducerNode.peerInfo.Id == p.peerInfo.Id {
			exist = true
			break
		}
	}
	if !exist {
		r.registrationTopicMap[topic] = append(producers, TopicProducerReg{pidStr, p})
	}
	return !exist
}

// remove all topic producers and channels related with peer id
func (r *RegistrationDB) RemoveAllByPeerId(id string) {
	r.Lock()
	defer r.Unlock()
	for topic, producers := range r.registrationTopicMap {
		removed := false
		cleaned := TopicRegistrations{}
		for _, producer := range producers {
			if producer.ProducerNode.peerInfo.Id != id {
				cleaned = append(cleaned, producer)
			} else {
				removed = true
			}
		}
		if removed {
			// Note: this leaves keys in the DB even if they have empty lists
			r.registrationTopicMap[topic] = cleaned
		}
	}
	for topic, chRegs := range r.registrationChannelMap {
		removed := false
		cleaned := ChannelRegistrations{}
		for _, reg := range chRegs {
			if reg.PeerId == id {
				removed = true
			} else {
				cleaned = append(cleaned, reg)
			}
		}
		if removed {
			r.registrationChannelMap[topic] = cleaned
		}
	}
	delete(r.registrationNodeMap, id)
}

func (r *RegistrationDB) RemoveTopicProducer(topic string, pid string, id string) bool {
	r.Lock()
	defer r.Unlock()
	producers, ok := r.registrationTopicMap[topic]
	if !ok {
		return false
	}
	removed := false
	for idx, producerReg := range producers {
		if producerReg.PartitionID == pid &&
			producerReg.ProducerNode.peerInfo.Id == id {
			removed = true
			producers[idx] = producers[len(producers)-1]
			producers = producers[:len(producers)-1]
			break
		}
	}
	if removed {
		if len(producers) == 0 {
			delete(r.registrationTopicMap, topic)
		} else {
			r.registrationTopicMap[topic] = producers
		}
	}
	return removed
}

func (r *RegistrationDB) needFilter(key string, subKey string) bool {
	return key == "*" || subKey == "*"
}

func (r *RegistrationDB) FindTopics() []string {
	r.RLock()
	defer r.RUnlock()
	results := make([]string, 0, len(r.registrationTopicMap))
	for k, _ := range r.registrationTopicMap {
		results = append(results, k)
	}
	return results
}

func (r *RegistrationDB) FindPeerTopics(id string) map[string]TopicRegistrations {
	r.RLock()
	defer r.RUnlock()
	results := make(map[string]TopicRegistrations)
	for k, regs := range r.registrationTopicMap {
		ret, _ := results[k]
		for _, reg := range regs {
			if reg.ProducerNode.peerInfo != nil && reg.ProducerNode.peerInfo.Id == id {
				ret = append(ret, reg)
			}
		}
		if len(ret) > 0 {
			results[k] = ret
		}
	}
	return results
}

func (r *RegistrationDB) FindTopicProducers(topic string, pid string) TopicRegistrations {
	r.RLock()
	defer r.RUnlock()

	results := TopicRegistrations{}
	producers, ok := r.registrationTopicMap[topic]
	if !ok {
		return results
	}
	for _, producerReg := range producers {
		if pid != "*" && pid != producerReg.PartitionID {
			continue
		}
		results = append(results, producerReg)
	}
	return results
}

// Note: if you want the topics or channels related to some node, you can just
// directly query the node.
// The lookup should handle the query related to more than one node.

func (k ChannelReg) IsMatch(pid string, peerId string, ch string) bool {
	if pid != "*" && k.PartitionID != pid {
		return false
	}
	if peerId != "*" && k.PeerId != peerId {
		return false
	}
	if ch != "*" && k.Channel != ch {
		return false
	}
	return true
}

func (rr ChannelRegistrations) Channels() []string {
	keys := make([]string, 0, len(rr))
	dupCheck := make(map[string]struct{}, len(keys)*2)
	for _, k := range rr {
		if _, ok := dupCheck[k.Channel]; ok {
			continue
		}
		keys = append(keys, k.Channel)
		dupCheck[k.Channel] = struct{}{}
	}
	return keys
}

func (pp TopicRegistrations) FilterByActive(inactivityTimeout time.Duration, filterTomb bool) TopicRegistrations {
	now := time.Now()
	results := TopicRegistrations{}
	for _, p := range pp {
		cur := time.Unix(0, atomic.LoadInt64(&p.ProducerNode.peerInfo.lastUpdate))
		if now.Sub(cur) > inactivityTimeout {
			continue
		}
		if filterTomb && p.ProducerNode.IsTombstoned() {
			continue
		}
		results = append(results, p)
	}
	return results
}

func (pp PeerInfoList) FilterByActive(inactivityTimeout time.Duration) PeerInfoList {
	now := time.Now()
	results := PeerInfoList{}
	for _, p := range pp {
		cur := time.Unix(0, atomic.LoadInt64(&p.lastUpdate))
		if now.Sub(cur) > inactivityTimeout {
			continue
		}
		results = append(results, p)
	}
	return results
}

func (pp Producers) FilterByActive(inactivityTimeout time.Duration, filterTomb bool) Producers {
	now := time.Now()
	results := Producers{}
	for _, p := range pp {
		cur := time.Unix(0, atomic.LoadInt64(&p.peerInfo.lastUpdate))
		if now.Sub(cur) > inactivityTimeout {
			continue
		}
		if filterTomb && p.IsTombstoned() {
			continue
		}
		results = append(results, p)
	}
	return results
}

func (pp Producers) PeerInfo() []*PeerInfo {
	results := []*PeerInfo{}
	for _, p := range pp {
		results = append(results, p.peerInfo)
	}
	return results
}
