package nsqlookupd

import (
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

type RegistrationDB struct {
	sync.RWMutex
	registrationMap map[Registration]Producers
}

type Registration struct {
	Category    string
	Key         string
	SubKey      string
	PartitionID string
}

type Registrations []Registration

type PeerInfo struct {
	lastUpdate       int64
	Id               string `json:"id"`
	RemoteAddress    string `json:"remote_address"`
	Hostname         string `json:"hostname"`
	BroadcastAddress string `json:"broadcast_address"`
	TCPPort          int    `json:"tcp_port"`
	HTTPPort         int    `json:"http_port"`
	Version          string `json:"version"`
}

type Producer struct {
	peerInfo     *PeerInfo
	tombstoned   bool
	tombstonedAt time.Time
}

type Producers []*Producer

func (p *Producer) String() string {
	return fmt.Sprintf("%s [%d, %d]", p.peerInfo.BroadcastAddress, p.peerInfo.TCPPort, p.peerInfo.HTTPPort)
}

func (p *Producer) Tombstone() {
	p.tombstoned = true
	p.tombstonedAt = time.Now()
}

func (p *Producer) IsTombstoned(lifetime time.Duration) bool {
	return p.tombstoned && time.Now().Sub(p.tombstonedAt) < lifetime
}

func NewRegistrationDB() *RegistrationDB {
	return &RegistrationDB{
		registrationMap: make(map[Registration]Producers),
	}
}

// add a registration key
func (r *RegistrationDB) AddRegistration(k Registration) {
	r.Lock()
	defer r.Unlock()
	_, ok := r.registrationMap[k]
	if !ok {
		r.registrationMap[k] = Producers{}
	}
}

func (r *RegistrationDB) AddProducerClient(k Registration, p *Producer) bool {
	r.Lock()
	defer r.Unlock()
	producers := r.registrationMap[k]
	found := false
	for _, producer := range producers {
		if producer.peerInfo.Id == p.peerInfo.Id {
			found = true
		}
	}
	if found == false {
		r.registrationMap[k] = append(producers, p)
	}
	return !found
}

// add a producer to a registration
func (r *RegistrationDB) AddProducer(k Registration, p *Producer) bool {
	if k.PartitionID == "" {
		return false
	}
	pid, err := strconv.Atoi(k.PartitionID)
	if err != nil || pid < 0 {
		return false
	}
	return r.AddProducerClient(k, p)
}

// remove a producer from a registration
func (r *RegistrationDB) RemoveProducer(k Registration, id string) (bool, int) {
	r.Lock()
	defer r.Unlock()
	producers, ok := r.registrationMap[k]
	if !ok {
		return false, 0
	}
	removed := false
	cleaned := Producers{}
	for _, producer := range producers {
		if producer.peerInfo.Id != id {
			cleaned = append(cleaned, producer)
		} else {
			removed = true
		}
	}
	// Note: this leaves keys in the DB even if they have empty lists
	r.registrationMap[k] = cleaned
	return removed, len(cleaned)
}

// remove a Registration and all it's producers
func (r *RegistrationDB) RemoveRegistration(k Registration) {
	r.Lock()
	defer r.Unlock()
	delete(r.registrationMap, k)
}

func (r *RegistrationDB) needFilter(key string, subkey string, pid string) bool {
	return key == "*" || subkey == "*" || pid == "*"
}

func (r *RegistrationDB) FindRegistrations(category string, key string, subkey string, pid string) Registrations {
	r.RLock()
	defer r.RUnlock()
	if !r.needFilter(key, subkey, pid) {
		k := Registration{category, key, subkey, pid}
		if _, ok := r.registrationMap[k]; ok {
			return Registrations{k}
		}
		return Registrations{}
	}
	results := Registrations{}
	for k := range r.registrationMap {
		if !k.IsMatch(category, key, subkey, pid) {
			continue
		}
		results = append(results, k)
	}
	return results
}

func (r *RegistrationDB) FindProducers(category string, key string, subkey string, pid string) Producers {
	r.RLock()
	defer r.RUnlock()
	if !r.needFilter(key, subkey, pid) {
		k := Registration{category, key, subkey, pid}
		return r.registrationMap[k]
	}

	results := Producers{}
	for k, producers := range r.registrationMap {
		if !k.IsMatch(category, key, subkey, pid) {
			continue
		}
		for _, producer := range producers {
			found := false
			for _, p := range results {
				if producer.peerInfo.Id == p.peerInfo.Id {
					found = true
				}
			}
			if found == false {
				results = append(results, producer)
			}
		}
	}
	return results
}

func (r *RegistrationDB) LookupRegistrations(id string) Registrations {
	r.RLock()
	defer r.RUnlock()
	results := Registrations{}
	for k, producers := range r.registrationMap {
		for _, p := range producers {
			if p.peerInfo.Id == id {
				results = append(results, k)
				break
			}
		}
	}
	return results
}

func (k Registration) IsMatch(category string, key string, subkey string, pid string) bool {
	if category != k.Category {
		return false
	}
	if key != "*" && k.Key != key {
		return false
	}
	if subkey != "*" && k.SubKey != subkey {
		return false
	}
	if pid != "*" && k.PartitionID != pid {
		return false
	}
	return true
}

func (rr Registrations) Filter(category string, key string, subkey string, pid string) Registrations {
	output := Registrations{}
	for _, k := range rr {
		if k.IsMatch(category, key, subkey, pid) {
			output = append(output, k)
		}
	}
	return output
}

func (rr Registrations) Keys() []string {
	keys := make([]string, 0, len(rr))
	dupCheck := make(map[string]struct{}, len(keys)*2)
	for _, k := range rr {
		if _, ok := dupCheck[k.Key]; ok {
			continue
		}
		keys = append(keys, k.Key)
		dupCheck[k.Key] = struct{}{}
	}
	return keys
}

func (rr Registrations) SubKeys() []string {
	subkeys := make([]string, 0, len(rr))
	dupCheck := make(map[string]struct{}, len(subkeys)*2)
	for _, k := range rr {
		if _, ok := dupCheck[k.SubKey]; ok {
			continue
		}
		subkeys = append(subkeys, k.SubKey)
		dupCheck[k.SubKey] = struct{}{}
	}
	return subkeys
}

func (rr Registrations) PartitionIDStrs() []string {
	pids := make([]string, len(rr))
	for i, k := range rr {
		pids[i] = k.PartitionID
	}
	return pids
}

func (rr Registrations) PartitionIDs() []int {
	pids := make([]int, len(rr))
	for i, k := range rr {
		pids[i], _ = strconv.Atoi(k.PartitionID)
	}
	return pids
}

func (pp Producers) FilterByActive(inactivityTimeout time.Duration, tombstoneLifetime time.Duration) Producers {
	now := time.Now()
	results := Producers{}
	for _, p := range pp {
		cur := time.Unix(0, atomic.LoadInt64(&p.peerInfo.lastUpdate))
		if now.Sub(cur) > inactivityTimeout || p.IsTombstoned(tombstoneLifetime) {
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
