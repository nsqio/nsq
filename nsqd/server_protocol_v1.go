package main

import (
	"../nsq"
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
	"log"
	"strconv"
	"strings"
)

type ServerProtocolV1 struct {
	nsq.Protocol
}

func init() {
	// BigEndian client byte sequence "  V1"
	var magicInt int32
	buf := bytes.NewBuffer([]byte(nsq.ProtocolV1Magic))
	binary.Read(buf, binary.BigEndian, &magicInt)
	Protocols[magicInt] = &ServerProtocolV1{}
}

func (p *ServerProtocolV1) IOLoop(client *nsq.ServerClient) error {
	var err error
	var line string

	client.SetState("state", nsq.ClientStateV1Init)

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

		response, err := nsq.ProtocolExecute(p, client, params...)
		if err != nil {
			_, err = client.Write([]byte(err.Error()))
			if err != nil {
				break
			}
			continue
		}

		if response != nil {
			_, err = client.Write(response)
			if err != nil {
				break
			}
		}
	}

	return err
}

func (p *ServerProtocolV1) PUB(client *nsq.ServerClient, params []string) ([]byte, error) {
	var err error

	if len(params) < 3 {
		return nil, nsq.ClientErrV1Invalid
	}

	topicName := params[1]
	if len(topicName) > nsq.MaxNameLength {
		return nil, nsq.ClientErrV1BadTopic
	}

	messageSize, err := strconv.Atoi(params[2])
	if err != nil {
		return nil, err
	}

	messageBody := make([]byte, messageSize)
	_, err = io.ReadFull(client, messageBody)
	if err != nil {
		return nil, err
	}

	topic := nsqd.GetTopic(topicName)
	msg := nsq.NewMessage(<-nsqd.idChan, messageBody)
	topic.PutMessage(msg)

	return []byte("OK"), nil
}
