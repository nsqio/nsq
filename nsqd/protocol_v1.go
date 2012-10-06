package main

import (
	"../nsq"
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
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

		response, err := p.Exec(client, reader, params)
		if err != nil {
			log.Printf("ERROR: CLIENT(%s) - %s", client, err.(*nsq.ClientErr).Description())
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

func (p *ProtocolV1) Exec(client *ClientV1, reader *bufio.Reader, params []string) ([]byte, error) {
	switch params[0] {
	case "PUB":
		return p.PUB(client, reader, params)
	}
	return nil, nsq.NewClientErr("E_INVALID", fmt.Sprintf("invalid command %s", params[0]))
}

func (p *ProtocolV1) PUB(client *ClientV1, reader *bufio.Reader, params []string) ([]byte, error) {
	var err error

	if len(params) < 2 {
		return nil, nsq.NewClientErr("E_MISSING_PARAMS", "insufficient number of parameters")
	}

	topicName := params[1]
	if !nsq.IsValidTopicName(topicName) {
		return nil, nsq.NewClientErr("E_BAD_TOPIC", fmt.Sprintf("topic name '%s' is not valid", topicName))
	}

	var bodyLen int32
	err = binary.Read(reader, binary.BigEndian, &bodyLen)
	if err != nil {
		return nil, nsq.NewClientErr("E_BAD_BODY", err.Error())
	}

	messageBody := make([]byte, bodyLen)
	_, err = io.ReadFull(client, messageBody)
	if err != nil {
		return nil, nsq.NewClientErr("E_BAD_BODY", err.Error())
	}

	topic := nsqd.GetTopic(topicName)
	msg := nsq.NewMessage(<-nsqd.idChan, messageBody)
	err = topic.PutMessage(msg)
	if err != nil {
		return nil, nsq.NewClientErr("E_PUT_FAILED", err.Error())
	}

	return []byte("OK"), nil
}
