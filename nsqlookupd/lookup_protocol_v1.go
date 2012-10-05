package main

import (
	"../nsq"
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net"
	"os"
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
			log.Printf("ERROR: CLIENT(%s) %s for %v", client.RemoteAddr(), err.Error(), params)
			_, err = nsq.SendResponse(client, []byte(nsq.ClientErrInvalid.Error()))
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

	log.Printf("CLIENT(%s) closing", client.RemoteAddr())
	if client.Producer != nil {
		lookupd.DB.Remove(Registration{"client", "", ""}, client.Producer)
		registrations := lookupd.DB.LookupRegistrations(client.Producer)
		for _, r := range registrations {
			lookupd.DB.Remove(*r, client.Producer)
		}
	}
	return err
}

func (p *LookupProtocolV1) Exec(client *ClientV1, reader *bufio.Reader, params []string) ([]byte, error) {
	switch params[0] {
	case "PING":
		return p.PING(client, params)
	case "IDENTIFY":
		return p.IDENTIFY(client, reader, params[1:])
	case "REGISTER":
		return p.REGISTER(client, reader, params[1:])
	case "UNREGISTER":
		return p.UNREGISTER(client, reader, params[1:])
	case "ANNOUNCE":
		return p.ANNOUNCE_OLD(client, reader, params[1:])
	}
	log.Printf("ERROR: invalid method %s from client %s", client.RemoteAddr(), params[0])
	return nil, nsq.ClientErrInvalid
}

func getTopicChan(params []string) (string, string, error) {
	if len(params) == 0 {
		return "", "", errors.New("Wrong number of parameters")
	}
	topicName := params[0]
	var channelName string
	if len(params) == 2 {
		channelName = params[1]
	}
	if !nsq.IsValidTopicName(topicName) {
		return "", "", errors.New("INVALID_TOPIC")
	}
	if channelName != "" && !nsq.IsValidChannelName(channelName) {
		return "", "", errors.New("INVALID_CHANNEL")
	}
	return topicName, channelName, nil
}

func (p *LookupProtocolV1) REGISTER(client *ClientV1, reader *bufio.Reader, params []string) ([]byte, error) {
	if client.Producer == nil {
		return nil, nsq.ClientErrInvalid
	}
	topic, channel, err := getTopicChan(params)
	if err != nil {
		return nil, err
	}
	if channel != "" {
		key := Registration{"channel", topic, channel}
		lookupd.DB.Add(key, client.Producer)
	}
	key := Registration{"topic", topic, ""}
	lookupd.DB.Add(key, client.Producer)
	return []byte("OK"), nil
}

func (p *LookupProtocolV1) UNREGISTER(client *ClientV1, reader *bufio.Reader, params []string) ([]byte, error) {
	if client.Producer == nil {
		return nil, nsq.ClientErrInvalid
	}
	topic, channel, err := getTopicChan(params)
	if err != nil {
		return nil, err
	}
	if channel != "" {
		key := Registration{"channel", topic, channel}
		lookupd.DB.Remove(key, client.Producer)
	}
	return []byte("OK"), nil
}

func (p *LookupProtocolV1) ANNOUNCE_OLD(client *ClientV1, reader *bufio.Reader, params []string) ([]byte, error) {
	if len(params) < 3 {
		return nil, nsq.ClientErrInvalid
	}
	topic, channel, err := getTopicChan(params)
	if err != nil {
		return nil, err
	}

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

	if client.Producer == nil {
		tcpPort, err := strconv.Atoi(params[2])
		if err != nil {
			return nil, err
		}
		httpPort := tcpPort + 1
		if len(params) > 3 {
			httpPort, err = strconv.Atoi(params[4])
			if err != nil {
				return nil, err
			}
		}

		var ipAddresses []string
		// client sends multiple source IP address as the message body
		for _, ip := range bytes.Split(body, []byte("\n")) {
			ipAddresses = append(ipAddresses, string(ip))
		}

		client.Producer = &Producer{
			producerId: client.RemoteAddr().String(),
			TcpPort:    tcpPort,
			HttpPort:   httpPort,
			Address:    ipAddresses[len(ipAddresses)-1],
			LastUpdate: time.Now(),
		}
		log.Printf("CLIENT(%s) registered TCP:%d HTTP:%d address:%s", client.RemoteAddr(), tcpPort, httpPort, client.Producer.Address)
	}

	log.Printf("CLIENT(%s) announcing Topic:%s Channel:%s", client.Producer.producerId, topic, channel)

	var key Registration

	if len(channel) != 0 {
		key = Registration{"channel", topic, channel}
		lookupd.DB.Add(key, client.Producer)
	}

	if channel != "." {
		key = Registration{"channel", topic, channel}
		lookupd.DB.Add(key, client.Producer)
	}

	key = Registration{"topic", topic, ""}
	lookupd.DB.Add(key, client.Producer)

	return []byte("OK"), nil
}

func (p *LookupProtocolV1) IDENTIFY(client *ClientV1, reader *bufio.Reader, params []string) ([]byte, error) {
	var err error

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

	// body is a json structure with producer information
	producer := Producer{producerId: client.RemoteAddr().String()}
	err = json.Unmarshal(body, &producer)
	if err != nil {
		return nil, err
	}

	// require all fields
	if producer.Address == "" || producer.TcpPort == 0 || producer.HttpPort == 0 || producer.Version == "" {
		log.Printf("ERROR: missing fields in IDENTIFY from %s", client.RemoteAddr())
		return nil, errors.New("MISSING_FIELDS")
	}
	producer.LastUpdate = time.Now()

	client.Producer = &producer
	lookupd.DB.Add(Registration{"client", "", ""}, client.Producer)
	log.Printf("CLIENT(%s) registered TCP:%d HTTP:%d address:%s",
		client.RemoteAddr(),
		producer.TcpPort,
		producer.HttpPort,
		producer.Address)

	// build a response
	data := make(map[string]interface{})
	data["tcp_port"] = lookupd.tcpAddr.Port
	data["http_port"] = lookupd.httpAddr.Port
	data["version"] = VERSION
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatalf("ERROR: unable to get hostname %s", err.Error())
	}
	data["address"] = hostname
	response, err := json.Marshal(data)
	if err != nil {
		log.Printf("error marshaling %v", data)
		return []byte("OK"), nil
	}
	return response, nil
}

func (p *LookupProtocolV1) PING(client *ClientV1, params []string) ([]byte, error) {
	if client.Producer != nil {
		// we could get a PING before an ANNOUNCE on the same client connection
		now := time.Now()
		log.Printf("CLIENT(%s) pinged (last ping %s)", client.Producer.producerId, now.Sub(client.Producer.LastUpdate))
		client.Producer.LastUpdate = now
	}
	return []byte("OK"), nil
}
