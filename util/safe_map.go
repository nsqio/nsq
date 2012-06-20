package util

import (
	"errors"
	"log"
	"sync"
)

type SafeMap struct {
	data  map[string]interface{}
	mutex sync.RWMutex
}

func NewSafeMap() *SafeMap {
	sm := SafeMap{
		data: make(map[string]interface{}),
	}
	return &sm
}

func (sm *SafeMap) Get(key string) (interface{}, error) {
	sm.mutex.RLock()
	defer sm.mutex.RUnlock()

	val, ok := sm.data[key]
	if !ok {
		return nil, errors.New("E_NOT_FOUND")
	}

	return val, nil
}

func (sm *SafeMap) Set(key string, updateFunc func(data interface{}, params []interface{}) (interface{}, error), params ...interface{}) error {
	sm.mutex.Lock()
	defer sm.mutex.Unlock()

	newData, err := updateFunc(sm.data[key], params)
	if err != nil {
		return err
	}
	sm.data[key] = newData
	log.Printf("DATA: %#v", sm.data[key])

	return nil
}

func (sm *SafeMap) Keys() []string {
	sm.mutex.RLock()
	defer sm.mutex.RUnlock()

	keys := make([]string, 0, len(sm.data))
	for key, _ := range sm.data {
		keys = append(keys, key)
	}

	return keys
}

func (sm *SafeMap) Iter(iterFunc func(key string, data interface{}) error) error {
	sm.mutex.RLock()
	defer sm.mutex.RUnlock()

	for key, val := range sm.data {
		err := iterFunc(key, val)
		if err != nil {
			return err
		}
	}

	return nil
}
