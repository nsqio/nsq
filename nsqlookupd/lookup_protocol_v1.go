package main

import (
	"../nsq"
	"../util"
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
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

	log.Printf("CLIENT(%s): closing", client)
	if client.Producer != nil {
		lookupd.DB.RemoveProducer(Registration{"client", "", ""}, client.Producer)
		registrations := lookupd.DB.LookupRegistrations(client.Producer)
		for _, r := range registrations {
			lookupd.DB.RemoveProducer(*r, client.Producer)
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
	}
	return nil, nsq.NewClientErr("E_INVALID", fmt.Sprintf("invalid command %s", params[0]))
}

func getTopicChan(params []string) (string, string, error) {
	if len(params) == 0 {
		return "", "", nsq.NewClientErr("E_MISSING_PARAMS", "insufficient number of params")
	}

	topicName := params[0]
	var channelName string
	if len(params) >= 2 {
		channelName = params[1]
	}

	if !nsq.IsValidTopicName(topicName) {
		return "", "", nsq.NewClientErr("E_BAD_TOPIC", fmt.Sprintf("topic name '%s' is not valid", topicName))
	}

	if channelName != "" && !nsq.IsValidChannelName(channelName) {
		return "", "", nsq.NewClientErr("E_BAD_CHANNEL", fmt.Sprintf("channel name '%s' is not valid", channelName))
	}

	return topicName, channelName, nil
}

func (p *LookupProtocolV1) REGISTER(client *ClientV1, reader *bufio.Reader, params []string) ([]byte, error) {
	if client.Producer == nil {
		return nil, nsq.NewClientErr("E_INVALID", "client must IDENTIFY")
	}

	topic, channel, err := getTopicChan(params)
	if err != nil {
		return nil, err
	}

	if channel != "" {
		log.Printf("DB: client(%s) added registration for channel:%s in topic:%s", client, channel, topic)
		key := Registration{"channel", topic, channel}
		lookupd.DB.AddProducer(key, client.Producer)
	}
	log.Printf("DB: client(%s) added registration for topic:%s", client, topic)
	key := Registration{"topic", topic, ""}
	lookupd.DB.AddProducer(key, client.Producer)

	return []byte("OK"), nil
}

func (p *LookupProtocolV1) UNREGISTER(client *ClientV1, reader *bufio.Reader, params []string) ([]byte, error) {
	if client.Producer == nil {
		return nil, nsq.NewClientErr("E_INVALID", "client must IDENTIFY")
	}

	topic, channel, err := getTopicChan(params)
	if err != nil {
		return nil, err
	}

	if channel != "" {
		log.Printf("DB: client(%s) removed registration for channel:%s in topic:%s", client, channel, topic)
		key := Registration{"channel", topic, channel}
		producers := lookupd.DB.RemoveProducer(key, client.Producer)
		// for ephemeral channels, remove the channel as well if it has no producers
		if producers == 0 && strings.HasSuffix(channel, "#ephemeral") {
			lookupd.DB.RemoveRegistration(key)
		}
	}

	return []byte("OK"), nil
}

func (p *LookupProtocolV1) IDENTIFY(client *ClientV1, reader *bufio.Reader, params []string) ([]byte, error) {
	var err error

	var bodyLen int32
	err = binary.Read(reader, binary.BigEndian, &bodyLen)
	if err != nil {
		return nil, nsq.NewClientErr("E_BAD_BODY", err.Error())
	}

	body := make([]byte, bodyLen)
	_, err = io.ReadFull(reader, body)
	if err != nil {
		return nil, nsq.NewClientErr("E_BAD_BODY", err.Error())
	}

	// body is a json structure with producer information
	producer := Producer{producerId: client.RemoteAddr().String()}
	err = json.Unmarshal(body, &producer)
	if err != nil {
		return nil, nsq.NewClientErr("E_BAD_BODY", err.Error())
	}

	// require all fields
	if producer.Address == "" || producer.TcpPort == 0 || producer.HttpPort == 0 || producer.Version == "" {
		return nil, nsq.NewClientErr("E_BAD_BODY", "missing fields in IDENTIFY")
	}
	producer.LastUpdate = time.Now()

	client.Producer = &producer
	lookupd.DB.AddProducer(Registration{"client", "", ""}, client.Producer)
	log.Printf("CLIENT(%s) registered TCP:%d HTTP:%d address:%s",
		client.RemoteAddr(),
		producer.TcpPort,
		producer.HttpPort,
		producer.Address)

	// build a response
	data := make(map[string]interface{})
	data["tcp_port"] = lookupd.tcpAddr.Port
	data["http_port"] = lookupd.httpAddr.Port
	data["version"] = util.BINARY_VERSION
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatalf("ERROR: unable to get hostname %s", err.Error())
	}
	data["address"] = hostname
	response, err := json.Marshal(data)
	if err != nil {
		log.Printf("ERROR: marshaling %v", data)
		return []byte("OK"), nil
	}
	return response, nil
}

func (p *LookupProtocolV1) PING(client *ClientV1, params []string) ([]byte, error) {
	if client.Producer != nil {
		// we could get a PING before other commands on the same client connection
		now := time.Now()
		log.Printf("CLIENT(%s): pinged (last ping %s)", client.Producer.producerId, now.Sub(client.Producer.LastUpdate))
		client.Producer.LastUpdate = now
	}
	return []byte("OK"), nil
}
