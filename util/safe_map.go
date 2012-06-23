package util

import (
	"log"
	"sync"
)

type SafeMap struct {
	sync.RWMutex
	data map[string]interface{}
}

func NewSafeMap() *SafeMap {
	sm := SafeMap{
		data: make(map[string]interface{}),
	}
	return &sm
}

func (sm *SafeMap) Get(key string) (interface{}, bool) {
	sm.RLock()
	defer sm.RUnlock()

	val, ok := sm.data[key]
	if !ok {
		return nil, false
	}

	return val, true
}

func (sm *SafeMap) Set(key string, updateFunc func(data interface{}, params []interface{}) (interface{}, error), params ...interface{}) error {
	sm.Lock()
	defer sm.Unlock()

	newData, err := updateFunc(sm.data[key], params)
	if err != nil {
		return err
	}
	sm.data[key] = newData
	log.Printf("DATA: %#v", sm.data[key])

	return nil
}

func (sm *SafeMap) Keys() []string {
	sm.RLock()
	defer sm.RUnlock()

	keys := make([]string, 0, len(sm.data))
	for key, _ := range sm.data {
		keys = append(keys, key)
	}

	return keys
}

func (sm *SafeMap) Iter(iterFunc func(key string, data interface{}) error) error {
	sm.RLock()
	defer sm.RUnlock()

	for key, val := range sm.data {
		err := iterFunc(key, val)
		if err != nil {
			return err
		}
	}

	return nil
}
