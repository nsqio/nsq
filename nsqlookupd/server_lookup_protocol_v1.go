package main

import (
	"../nsq"
	"../util"
	"bufio"
	"bytes"
	"encoding/binary"
	"log"
	"net"
	"strconv"
	"strings"
)

type ServerLookupProtocolV1 struct {
	nsq.Protocol
}

func init() {
	// BigEndian client byte sequence "  V1"
	var magicInt int32
	buf := bytes.NewBuffer([]byte(nsq.LookupProtocolV1Magic))
	binary.Read(buf, binary.BigEndian, &magicInt)
	Protocols[magicInt] = &ServerLookupProtocolV1{}
}

func (p *ServerLookupProtocolV1) IOLoop(client *nsq.ServerClient) error {
	var err error
	var line string

	client.SetState("state", nsq.LookupClientStateV1Init)
	client.SetState("safe_map", sm)

	err = nil
	reader := bufio.NewReader(client)
	for {
		line, err = reader.ReadString('\n')
		if err != nil {
			break
		}

		line = strings.TrimSpace(line)
		params := strings.Split(line, " ")

		log.Printf("PROTOCOL(V1) [%s]: %#v", client, params)

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

func (p *ServerLookupProtocolV1) ANNOUNCE(client *nsq.ServerClient, params []string) ([]byte, error) {
	var err error

	if len(params) < 4 {
		return nil, nsq.LookupClientErrV1Invalid
	}

	topicName := params[1]
	channelName := params[2]
	port, err := strconv.Atoi(params[3])
	if err != nil {
		return nil, err
	}

	host, _, err := net.SplitHostPort(client.String())
	if err != nil {
		return nil, err
	}

	smInterface, _ := client.GetState("safe_map")
	sm := smInterface.(*util.SafeMap)
	err = sm.Set("topic."+topicName, UpdateTopic, topicName, channelName, host, port)
	if err != nil {
		return nil, err
	}

	return []byte("OK"), nil
}

func (p *ServerLookupProtocolV1) PING(client *nsq.ServerClient, params []string) ([]byte, error) {
	return []byte("OK"), nil
}
