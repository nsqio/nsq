package nsq

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"log"
)

type Uuid []byte

func NewUuid() Uuid {
	b := make([]byte, 16)
	_, err := io.ReadFull(rand.Reader, b)
	if err != nil {
		log.Fatal(err)
	}
	b[6] = (b[6] & 0x0F) | 0x40
	b[8] = (b[8] &^ 0x40) | 0x80
	return b
}

func (u Uuid) Hex() []byte {
	b := make([]byte, 32)
	hex.Encode(b, u)
	return b
}

func (u Uuid) Hex4() []byte {
	return []byte(fmt.Sprintf("%x-%x-%x-%x-%x", u[:4], u[4:6], u[6:8], u[8:10], u[10:]))
}
