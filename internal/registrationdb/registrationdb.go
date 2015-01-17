// Package registrationdb tracks a set of producers for a various registration.
// Registrations fall under a category, and may be distinguished with keys and
// subkeys.
//
// Examples of usage include NSQ topic, channel, and client registrations. These
// registrations are added under separate categories, so a producer will be
// registered for each of these things independently. This allows keeping
// information about things like topic existence and client existince separate
// while not requiring additional memory to store producers multiple times.
package registrationdb

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// RegistrationDB stores Producers keyed by Registrations
type RegistrationDB struct {
	mtx  sync.RWMutex
	data map[Registration]ProducerMap
}

// Registration is key for RegistrationDB identified by a category, key, and
// subkey
type Registration struct {
	Category string
	Key      string
	SubKey   string
}

// String returns a human-readable string
func (r Registration) String() string {
	return fmt.Sprintf("category:%s key:%s subkey:%s",
		r.Category, r.Key, r.SubKey)
}

// Registrations is a list of Registration
type Registrations []Registration

// PeerInfo contains the metadata for a Peer
type PeerInfo struct {
	LastUpdate       int64  `json:"-"`
	ID               string `json:"-"`
	RemoteAddress    string `json:"remote_address"`
	Hostname         string `json:"hostname"`
	BroadcastAddress string `json:"broadcast_address"`
	TCPPort          int    `json:"tcp_port"`
	HTTPPort         int    `json:"http_port"`
	Version          string `json:"version"`
}

// Producer is a unique, per Registration, Peer
type Producer struct {
	*PeerInfo
	tombstoned   bool
	tombstonedAt time.Time
}

// Producers is a list of Producer
type Producers []*Producer
type ProducerMap map[string]*Producer

// Producer returns a human-readable string
func (p Producer) String() string {
	return fmt.Sprintf("%s [%d, %d]",
		p.BroadcastAddress, p.TCPPort, p.HTTPPort)
}

// Tombstone marks this producer as tombstoned
func (p *Producer) Tombstone() {
	p.tombstoned = true
	p.tombstonedAt = time.Now()
}

// IsTombstoned returns a boolean indicating the tombstone status of this Producer
// for the given lifetime
func (p *Producer) IsTombstoned(lifetime time.Duration) bool {
	return p.tombstoned && time.Now().Sub(p.tombstonedAt) < lifetime
}

// New returns a new instance of RegistrationDB
func New() *RegistrationDB {
	return &RegistrationDB{
		data: make(map[Registration]ProducerMap),
	}
}

// Debug returns a map containing the metadata for all registrations and producers
func (r *RegistrationDB) Debug() map[string][]map[string]interface{} {
	r.mtx.RLock()
	defer r.mtx.RUnlock()

	data := make(map[string][]map[string]interface{})
	for r, producers := range r.data {
		key := r.Category + ":" + r.Key + ":" + r.SubKey
		data[key] = make([]map[string]interface{}, 0)
		for _, p := range producers {
			m := make(map[string]interface{})
			m["id"] = p.ID
			m["hostname"] = p.Hostname
			m["broadcast_address"] = p.BroadcastAddress
			m["tcp_port"] = p.TCPPort
			m["http_port"] = p.HTTPPort
			m["version"] = p.Version
			m["last_update"] = atomic.LoadInt64(&p.LastUpdate)
			m["tombstoned"] = p.tombstoned
			m["tombstoned_at"] = p.tombstonedAt.UnixNano()
			data[key] = append(data[key], m)
		}
	}

	return data
}

// AddRegistration creates an empty list of producers under a given registration
// if it does not exist
func (r *RegistrationDB) AddRegistration(k Registration) {
	r.mtx.Lock()
	defer r.mtx.Unlock()
	_, ok := r.data[k]
	if !ok {
		r.data[k] = make(map[string]*Producer)
	}
}

// AddProducer adds a producer to a registration set
func (r *RegistrationDB) AddProducer(k Registration, p *Producer) bool {
	r.mtx.Lock()
	defer r.mtx.Unlock()
	_, ok := r.data[k]
	if !ok {
		r.data[k] = make(map[string]*Producer)
	}
	producers := r.data[k]
	_, found := producers[p.ID]
	if found == false {
		producers[p.ID] = p
	}
	return !found
}

// RemoveProducer removes a producer from a registration set
func (r *RegistrationDB) RemoveProducer(k Registration, id string) (bool, int) {
	r.mtx.Lock()
	defer r.mtx.Unlock()
	producers, ok := r.data[k]
	if !ok {
		return false, 0
	}
	removed := false
	if _, exists := producers[id]; exists {
		removed = true
	}

	// Note: this leaves keys in the DB even if they have empty lists
	delete(producers, id)
	return removed, len(producers)
}

// RemoveRegistration removes all producers for a registration
func (r *RegistrationDB) RemoveRegistration(k Registration) {
	r.mtx.Lock()
	defer r.mtx.Unlock()
	delete(r.data, k)
}

func (r *RegistrationDB) needFilter(key string, subkey string) bool {
	return key == "*" || subkey == "*"
}

// FindRegistrations finds all the registrations (sets of producers) that match
// the given category, key, and subkey.
//
// The key and subkey may be given as wildcards (*).
func (r *RegistrationDB) FindRegistrations(category string, key string, subkey string) Registrations {
	r.mtx.RLock()
	defer r.mtx.RUnlock()
	if !r.needFilter(key, subkey) {
		k := Registration{category, key, subkey}
		if _, ok := r.data[k]; ok {
			return Registrations{k}
		}
		return Registrations{}
	}
	results := Registrations{}
	for k := range r.data {
		if !k.IsMatch(category, key, subkey) {
			continue
		}
		results = append(results, k)
	}
	return results
}

// FindProducers finds all the producers that are registered under the given
// category, key, and subkey.
//
// The key and subkey may be given as wildcards (*).
func (r *RegistrationDB) FindProducers(category string, key string, subkey string) Producers {
	r.mtx.RLock()
	defer r.mtx.RUnlock()
	if !r.needFilter(key, subkey) {
		k := Registration{category, key, subkey}
		return ProducerMap2Slice(r.data[k])
	}

	results := make(map[string]struct{})
	var retProducers Producers
	for k, producers := range r.data {
		if !k.IsMatch(category, key, subkey) {
			continue
		}
		for _, producer := range producers {
			_, found := results[producer.ID]
			if found == false {
				results[producer.ID] = struct{}{}
				retProducers = append(retProducers, producer)
			}
		}
	}
	return retProducers
}

// LookupRegistrations finds the list of registrations (sets of producers) that
// contain the producer with the ID given
func (r *RegistrationDB) LookupRegistrations(id string) Registrations {
	r.mtx.RLock()
	defer r.mtx.RUnlock()
	results := Registrations{}
	for k, producers := range r.data {
		if _, exists := producers[id]; exists {
			results = append(results, k)
		}
	}
	return results
}

// TouchProducer finds the the producer with ID id under the registration k and
// updates its LastUpdate to the current time.
//
// If the producer was found, the function returns true.
func (r *RegistrationDB) TouchProducer(k Registration, id string) bool {
	r.mtx.RLock()
	defer r.mtx.RUnlock()
	now := time.Now()
	producers, ok := r.data[k]
	if !ok {
		return false
	}
	for _, p := range producers {
		if p.ID == id {
			atomic.StoreInt64(&p.LastUpdate, now.UnixNano())
			return true
		}
	}
	return false
}

// IsMatch matches a category, key, and subkey with a registration.
//
// The key and subkey may be given as wildcards (*).
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

// Filter returns a new list of registrations that match the given category,
// key, and subkey.
func (rr Registrations) Filter(category string, key string, subkey string) Registrations {
	output := Registrations{}
	for _, k := range rr {
		if k.IsMatch(category, key, subkey) {
			output = append(output, k)
		}
	}
	return output
}

// Keys returns the keys of rr as a slice of strings.
func (rr Registrations) Keys() []string {
	keys := make([]string, len(rr))
	for i, k := range rr {
		keys[i] = k.Key
	}
	return keys
}

// SubKeys returns the subkeys of rr as a slice of strings.
func (rr Registrations) SubKeys() []string {
	subkeys := make([]string, len(rr))
	for i, k := range rr {
		subkeys[i] = k.SubKey
	}
	return subkeys
}

// FilterByActive returns a new list of producers that include elements of pp
// which have been active since inactivityTimeout and have not been tombstoned
// since tombstoneLifetime.
func (pp Producers) FilterByActive(inactivityTimeout time.Duration, tombstoneLifetime time.Duration) Producers {
	now := time.Now()
	results := Producers{}
	for _, p := range pp {
		cur := time.Unix(0, atomic.LoadInt64(&p.LastUpdate))
		if now.Sub(cur) > inactivityTimeout || p.IsTombstoned(tombstoneLifetime) {
			continue
		}
		results = append(results, p)
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
