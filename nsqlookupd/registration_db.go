package nsqlookupd

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type RegistrationDB struct {
	sync.RWMutex
	registrationMap *sync.Map
}

type Registration struct {
	Category string
	Key      string
	SubKey   string
}
type Registrations []Registration

type PeerInfo struct {
	lastUpdate       int64
	id               string
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
type ProducerMap map[string]*Producer

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
		registrationMap: &sync.Map{},
	}
}

// add a registration key
func (r *RegistrationDB) AddRegistration(k Registration) {
	r.registrationMap.LoadOrStore(k, make(ProducerMap))
}

// add a producer to a registration
func (r *RegistrationDB) AddProducer(k Registration, p *Producer) bool {
	r.Lock()
	defer r.Unlock()
	val, _ := r.registrationMap.LoadOrStore(k, make(ProducerMap))
	producers := val.(ProducerMap)
	_, found := producers[p.peerInfo.id]
	if found == false {
		producers[p.peerInfo.id] = p
	}

	return !found
}

// remove a producer from a registration
func (r *RegistrationDB) RemoveProducer(k Registration, id string) (bool, int) {
	r.Lock()
	defer r.Unlock()
	value, ok := r.registrationMap.Load(k)
	if !ok {
		return false, 0
	}
	producers := value.(ProducerMap)
	removed := false
	if _, exists := producers[id]; exists {
		removed = true
	}

	// Note: this leaves keys in the DB even if they have empty lists
	delete(producers, id)

	return removed, len(producers)
}

// remove a Registration and all it's producers
func (r *RegistrationDB) RemoveRegistration(k Registration) {
	r.registrationMap.Delete(k)
}

func (r *RegistrationDB) needFilter(key string, subkey string) bool {
	return key == "*" || subkey == "*"
}

func (r *RegistrationDB) FindRegistrations(category string, key string, subkey string) Registrations {
	if !r.needFilter(key, subkey) {
		k := Registration{category, key, subkey}
		if _, ok := r.registrationMap.Load(k); ok {
			return Registrations{k}
		}
		return Registrations{}
	}
	results := Registrations{}
	r.registrationMap.Range(func(k, _ interface{}) bool {
		if k.(Registration).IsMatch(category, key, subkey) {
			results = append(results, k.(Registration))
		}
		return true
	})

	return results
}

func (r *RegistrationDB) FindProducers(category string, key string, subkey string) Producers {
	if !r.needFilter(key, subkey) {
		k := Registration{category, key, subkey}
		val, _ := r.registrationMap.Load(k)

		r.RLock()
		defer r.RUnlock()
		return ProducerMap2Slice(val.(ProducerMap))
	}

	r.RLock()
	results := make(map[string]struct{})
	var retProducers Producers
	r.registrationMap.Range(func(k, v interface{}) bool {
		if k.(Registration).IsMatch(category, key, subkey) {
			producers := v.(ProducerMap)
			for _, producer := range producers {
				_, found := results[producer.peerInfo.id]
				if found == false {
					results[producer.peerInfo.id] = struct{}{}
					retProducers = append(retProducers, producer)
				}
			}
		}
		return true
	})

	r.RUnlock()

	return retProducers
}

func (r *RegistrationDB) LookupRegistrations(id string) Registrations {
	r.RLock()

	results := Registrations{}
	r.registrationMap.Range(func(k, v interface{}) bool {
		producers := v.(ProducerMap)
		if _, exists := producers[id]; exists {
			results = append(results, k.(Registration))
		}

		return true
	})

	r.RUnlock()

	return results
}

func (k Registration) IsMatch(category string, key string, subkey string) bool {
	if category != k.Category {
		return false
	}
	if key != "*" && k.Key != key {
		return false
	}
	if subkey != "*" && k.SubKey != subkey {
		return false
	}
	return true
}

func (rr Registrations) Filter(category string, key string, subkey string) Registrations {
	output := Registrations{}
	for _, k := range rr {
		if k.IsMatch(category, key, subkey) {
			output = append(output, k)
		}
	}
	return output
}

func (rr Registrations) Keys() []string {
	keys := make([]string, len(rr))
	for i, k := range rr {
		keys[i] = k.Key
	}
	return keys
}

func (rr Registrations) SubKeys() []string {
	subkeys := make([]string, len(rr))
	for i, k := range rr {
		subkeys[i] = k.SubKey
	}
	return subkeys
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

func ProducerMap2Slice(pm ProducerMap) Producers {
	var producers Producers
	for _, producer := range pm {
		producers = append(producers, producer)
	}

	return producers
}
