package main

import (
	"crypto/rand"
	"fmt"
	"io"
	"log"
)

var uuidChan = make(chan []byte, 1000)

func uuidFactory() {
	for {
		uuidChan <- Uuid()
	}
}

func Uuid() []byte {
	b := make([]byte, 16)
	_, err := io.ReadFull(rand.Reader, b)
	if err != nil {
		log.Fatal(err)
	}
	b[6] = (b[6] & 0x0F) | 0x40
	b[8] = (b[8] &^ 0x40) | 0x80
	return b
}

func UuidToStr(b []byte) string {
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[:4], b[4:6], b[6:8], b[8:10], b[10:])
}
