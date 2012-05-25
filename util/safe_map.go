package util

import (
	"errors"
	"log"
)

type SetOp struct {
	key string
	updateFunc func(data interface{}, params []interface{}) interface{}
	params []interface{}
}

type SafeMap struct {
	data    map[string]interface{}
	setChan chan ChanReq
	getChan chan ChanReq
}

func NewSafeMap() *SafeMap {
	sm := SafeMap{make(map[string]interface{}),
		make(chan ChanReq),
		make(chan ChanReq)}
	go sm.Router()
	return &sm
}

func (sm *SafeMap) Get(key string) (interface{}, error) {
	retChan := make(chan interface{})
	getReq := ChanReq{key, retChan}
	sm.getChan <- getReq
	ret := (<-retChan).(ChanRet)
	return ret.Variable, ret.Err
}

func (sm *SafeMap) Set(key string, updateFunc func(data interface{}, params []interface{}) interface{}, params ...interface{}) error {
	retChan := make(chan interface{})
	setReq := ChanReq{SetOp{key, updateFunc, params}, retChan}
	sm.setChan <- setReq
	ret := (<-retChan).(ChanRet)
	return ret.Err
}

func (sm *SafeMap) Router() {
	for {
		select {
		case setReq := <-sm.setChan:
			ret := ChanRet{}
			setOp := setReq.Variable.(SetOp)
			key := setOp.key
			updateFunc := setOp.updateFunc
			params := setOp.params
			sm.data[key] = updateFunc(sm.data[key], params)
			log.Printf("DATA: %#v", sm.data[key])
			ret.Err = nil
			ret.Variable = nil
			setReq.RetChan <- ret
		case getReq := <-sm.getChan:
			ret := ChanRet{}
			key := getReq.Variable.(string)
			// TODO: should this copy?
			val, ok := sm.data[key]
			if ok {
				ret.Err = nil
				ret.Variable = val
			} else {
				ret.Err = errors.New("E_NOT_FOUND")
				ret.Variable = nil
			}
			getReq.RetChan <- ret
		}
	}
}
