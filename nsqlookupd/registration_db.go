package main

import (
	"fmt"
	"sync"
	"time"
)

type RegistrationDB struct {
	sync.RWMutex
	registrationMap map[Registration]Producers
}

type Registration struct {
	Category string
	Key      string
	SubKey   string
}
type Registrations []*Registration

type Producer struct {
	producerId string
	Address    string    `json:"address"`
	TcpPort    int       `json:"tcp_port"`
	HttpPort   int       `json:"http_port"`
	Version    string    `json:"version"`
	LastUpdate time.Time `json:"-"`
}

type Producers []*Producer

func (p *Producer) String() string {
	return fmt.Sprintf("%s [%d, %d]", p.Address, p.TcpPort, p.HttpPort)
}

func NewRegistrationDB() *RegistrationDB {
	return &RegistrationDB{
		registrationMap: make(map[Registration]Producers),
	}
}

// add a producer to a registration
func (r *RegistrationDB) Add(k Registration, p *Producer) {
	r.Lock()
	defer r.Unlock()
	producers := r.registrationMap[k]
	found := false
	for _, producer := range producers {
		if producer.producerId == p.producerId {
			found = true
		}
	}
	if found == false {
		r.registrationMap[k] = append(producers, p)
	}
}

// remove a producer from a registration
func (r *RegistrationDB) Remove(k Registration, p *Producer) {
	r.Lock()
	defer r.Unlock()
	producers, ok := r.registrationMap[k]
	if !ok {
		return
	}
	cleaned := make(Producers, 0)
	for _, producer := range producers {
		if producer != p { // this is a pointer comparison
			cleaned = append(cleaned, producer)
		}
	}
	// Note: this leaves keys in the DB even if they have empty lists
	r.registrationMap[k] = cleaned
}

// remove a Registration and all it's producers
func (r *RegistrationDB) RemoveRegistration(k Registration) {
	r.Lock()
	defer r.Unlock()
	delete(r.registrationMap, k)
}

func (r *RegistrationDB) FindRegistrations(category string, key string, subkey string) Registrations {
	r.RLock()
	defer r.RUnlock()
	results := make(Registrations, 0)
	for k, _ := range r.registrationMap {
		if !k.IsMatch(category, key, subkey) {
			continue
		}
		// strangely, we can't just return &k because k here is a copy and a local variable
		results = append(results, &Registration{k.Category, k.Key, k.SubKey})
	}
	return results
}

func (r *RegistrationDB) FindProducers(category string, key string, subkey string) Producers {
	r.RLock()
	defer r.RUnlock()
	results := make(Producers, 0)
	for k, producers := range r.registrationMap {
		if !k.IsMatch(category, key, subkey) {
			continue
		}
		for _, producer := range producers {
			found := false
			for _, p := range results {
				if producer.producerId == p.producerId {
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

func (r *RegistrationDB) LookupRegistrations(p *Producer) Registrations {
	r.RLock()
	defer r.RUnlock()
	results := make(Registrations, 0)
	for k, producers := range r.registrationMap {
		for _, producer := range producers {
			if producer.producerId == p.producerId {
				// strangely, we can't just return &k because k here is a copy and a local variable
				results = append(results, &Registration{k.Category, k.Key, k.SubKey})
				break
			}
		}
	}
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
	output := make(Registrations, 0)
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

func (pp Producers) CurrentProducers() Producers {
	now := time.Now()
	results := make(Producers, 0)
	// TODO: make this a command line parameter
	maxAge := time.Duration(300) * time.Second
	for _, p := range pp {
		if now.Sub(p.LastUpdate) > maxAge {
			continue
		}
		results = append(results, p)
	}
	return results
}
