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
	"time"
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

		response, err := p.Exec(client, reader, params)
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

func (p *LookupProtocolV1) Exec(client *ClientV1, reader *bufio.Reader, params []string) ([]byte, error) {
	switch params[0] {
	case "ANNOUNCE":
		return p.ANNOUNCE(client, reader, params)
	case "PING":
		return p.PING(client, params)
	}
	log.Printf("ERROR: invalid method %s from client %s", client.RemoteAddr(), params[0])
	return nil, nsq.ClientErrInvalid
}

func (p *LookupProtocolV1) ANNOUNCE(client *ClientV1, reader *bufio.Reader, params []string) ([]byte, error) {
	var err error

	if len(params) < 4 {
		return nil, nsq.ClientErrInvalid
	}
	topicName := params[1]
	channelName := params[2]

	var bodyLen int32
	err = binary.Read(reader, binary.BigEndian, &bodyLen)
	if err != nil {
		return nil, err
	}

	body := make([]byte, bodyLen)
	_, err = io.ReadFull(reader, body)
	if err != nil {
		return nil, err
	}

	// TODO: move this client information into a separate message so the ANNOUNCE can return to just
	// be about topic and channel
	if client.Producer == nil {
		tcpPort, err := strconv.Atoi(params[3])
		if err != nil {
			return nil, err
		}
		httpPort := tcpPort + 1
		if len(params) > 4 {
			httpPort, err = strconv.Atoi(params[4])
			if err != nil {
				return nil, err
			}
		}

		// build an identifier to use to track this client. We use remote Address + TCP port number
		host, _, err := net.SplitHostPort(client.RemoteAddr().String())
		if err != nil {
			return nil, err
		}
		producerId := net.JoinHostPort(host, strconv.Itoa(tcpPort))

		var ipAddresses []string
		// client sends multiple source IP address as the message body
		for _, ip := range bytes.Split(body, []byte("\n")) {
			ipAddresses = append(ipAddresses, string(ip))
		}

		client.Producer = &Producer{
			ProducerId:  producerId,
			TCPPort:     tcpPort,
			HTTPPort:    httpPort,
			IpAddresses: ipAddresses,
			LastUpdate:  time.Now(),
		}
		log.Printf("CLIENT(%s) -> CLIENT(%s) registered TCP:%d HTTP:%d addresses:%s", client.RemoteAddr(), producerId, tcpPort, httpPort, ipAddresses)
	}

	log.Printf("CLIENT(%s) announcing Topic:%s Channel:%s", client.Producer.ProducerId, topicName, channelName)
	err = lookupdb.Update(topicName, channelName, client.Producer)
	if err != nil {
		return nil, err
	}

	return []byte("OK"), nil
}

func (p *LookupProtocolV1) PING(client *ClientV1, params []string) ([]byte, error) {
	if client.Producer != nil {
		// we could get a PING before an ANNOUNCE on the same client connection
		now := time.Now()
		log.Printf("CLIENT(%s) pinged (last ping %s)", client.Producer.ProducerId, now.Sub(client.Producer.LastUpdate))
		client.Producer.LastUpdate = now
	}
	return []byte("OK"), nil
}
