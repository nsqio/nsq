package main

import (
	"../util"
)

var UuidChan = make(chan []byte, 1000)

func UuidFactory() {
	for {
		UuidChan <- util.Uuid()
	}
}
