package main

import (
	"../nsq"
	"bufio"
	"bytes"
	"encoding/binary"
	"net"
	"strconv"
	"strings"
)

type LookupProtocolV1 struct {
	nsq.Protocol
}

func init() {
	// BigEndian client byte sequence "  V1"
	var magicInt int32
	buf := bytes.NewBuffer([]byte(nsq.MagicV1))
	binary.Read(buf, binary.BigEndian, &magicInt)
	protocols[magicInt] = &LookupProtocolV1{}
}

func (p *LookupProtocolV1) IOLoop(conn net.Conn) error {
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

func (p *LookupProtocolV1) Exec(client *ClientV1, params []string) ([]byte, error) {
	switch params[0] {
	case "ANNOUNCE":
		return p.ANNOUNCE(client, params)
	case "PING":
		return p.PING(client, params)
	}
	return nil, nsq.ClientErrInvalid
}

func (p *LookupProtocolV1) ANNOUNCE(client *ClientV1, params []string) ([]byte, error) {
	var err error

	if len(params) < 4 {
		return nil, nsq.ClientErrInvalid
	}

	topicName := params[1]
	channelName := params[2]
	port, err := strconv.Atoi(params[3])
	if err != nil {
		return nil, err
	}

	host, _, err := net.SplitHostPort(client.RemoteAddr().String())
	if err != nil {
		return nil, err
	}

	err = sm.Set("topic."+topicName, UpdateTopic, topicName, channelName, host, port)
	if err != nil {
		return nil, err
	}

	return []byte("OK"), nil
}

func (p *LookupProtocolV1) PING(client *ClientV1, params []string) ([]byte, error) {
	return []byte("OK"), nil
}
