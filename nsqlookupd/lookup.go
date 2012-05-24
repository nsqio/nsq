package main

import (
	"../util"
	"errors"
)

type SetOp struct {
	key string
	val interface{}
}

type LookupDB struct {
	data map[string]interface{}
	setChan chan util.ChanReq
	getChan chan util.ChanReq
}

func NewLookupDB() *LookupDB {
	ldb := LookupDB{make(map[string]interface{}), 
		make(chan util.ChanReq), 
		make(chan util.ChanReq)}
	go ldb.Router()
	return &ldb
}

func (l *LookupDB) Get(key string) (interface{}, error) {
	retChan := make(chan interface{})
	getReq := util.ChanReq{key, retChan}
	l.getChan <- getReq
	ret := (<-retChan).(util.ChanRet)
	return ret.Variable, ret.Err
}

func (l *LookupDB) Set(key string, val interface{}) error {
	retChan := make(chan interface{})
	setReq := util.ChanReq{SetOp{key, val}, retChan}
	l.setChan <- setReq
	ret := (<-retChan).(util.ChanRet)
	return ret.Err
}

func (l *LookupDB) Router() {
	for {
		select {
		case setReq := <-l.setChan:
			ret := util.ChanRet{}
			setOp := setReq.Variable.(SetOp)
			key := setOp.key
			val := setOp.val
			l.data[key] = val
			ret.Err = nil
			ret.Variable = nil
			setReq.RetChan <- ret
		case getReq := <-l.getChan:
			ret := util.ChanRet{}
			key := getReq.Variable.(string)
			val, ok := l.data[key]
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
