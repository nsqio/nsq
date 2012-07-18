package main

import (
	"../nsq"
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
)

type ProtocolV1 struct {
	nsq.Protocol
}

func init() {
	// BigEndian client byte sequence "  V1"
	var magicInt int32
	buf := bytes.NewBuffer([]byte(nsq.MagicV1))
	binary.Read(buf, binary.BigEndian, &magicInt)
	protocols[magicInt] = &ProtocolV1{}
}

func (p *ProtocolV1) IOLoop(conn net.Conn) error {
	var err error
	var line string

	client := NewClientV1(conn)
	client.State = nsq.StateInit

	err = nil
	reader := bufio.NewReader(client)
	for {
		line, err = reader.ReadString('\n')
		if err != nil {
			break
		}

		line = strings.TrimSpace(line)
		params := strings.Split(line, " ")

		if *verbose {
			log.Printf("PROTOCOL(V1) [%s]: %#v", client, params)
		}

		response, err := p.Exec(client, params)
		if err != nil {
			_, err = nsq.SendResponse(client, []byte(err.Error()))
			if err != nil {
				break
			}
			continue
		}

		if response != nil {
			_, err = nsq.SendResponse(client, response)
			if err != nil {
				break
			}
		}
	}

	return err
}

func (p *ProtocolV1) Exec(client *ClientV1, params []string) ([]byte, error) {
	switch params[0] {
	case "PUB":
		return p.PUB(client, params)
	}
	return nil, nsq.ClientErrInvalid
}

func (p *ProtocolV1) PUB(client *ClientV1, params []string) ([]byte, error) {
	var err error

	if len(params) < 3 {
		return nil, nsq.ClientErrInvalid
	}

	topicName := params[1]
	if len(topicName) > nsq.MaxTopicNameLength {
		return nil, nsq.ClientErrBadTopic
	}

	messageSize, err := strconv.Atoi(params[2])
	if err != nil {
		return nil, nsq.ClientErrBadMessage
	}

	messageBody := make([]byte, messageSize)
	_, err = io.ReadFull(client, messageBody)
	if err != nil {
		return nil, nsq.ClientErrBadMessage
	}

	topic := nsqd.GetTopic(topicName)
	msg := nsq.NewMessage(<-nsqd.idChan, messageBody)
	err = topic.PutMessage(msg)
	if err != nil {
		return nil, nsq.ClientErrPutFailed
	}

	return []byte("OK"), nil
}
