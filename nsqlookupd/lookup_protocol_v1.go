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
		registrations := lookupd.DB.LookupRegistrations(client.Producer)
		for _, r := range registrations {
			if removed, _ := lookupd.DB.RemoveProducer(*r, client.Producer); removed {
				log.Printf("DB: client(%s) UNREGISTER category:%s key:%s subkey:%s",
					client, r.Category, r.Key, r.SubKey)
			}
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
		key := Registration{"channel", topic, channel}
		if lookupd.DB.AddProducer(key, client.Producer) {
			log.Printf("DB: client(%s) REGISTER category:%s key:%s subkey:%s",
				client, "channel", topic, channel)
		}
	}
	key := Registration{"topic", topic, ""}
	if lookupd.DB.AddProducer(key, client.Producer) {
		log.Printf("DB: client(%s) REGISTER category:%s key:%s subkey:%s",
			client, "topic", topic, "")
	}

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
		key := Registration{"channel", topic, channel}
		removed, left := lookupd.DB.RemoveProducer(key, client.Producer)
		if removed {
			log.Printf("DB: client(%s) UNREGISTER category:%s key:%s subkey:%s",
				client, "channel", topic, channel)
		}
		// for ephemeral channels, remove the channel as well if it has no producers
		if left == 0 && strings.HasSuffix(channel, "#ephemeral") {
			lookupd.DB.RemoveRegistration(key)
		}
	} else {
		// no channel was specified so this is a topic unregistration
		// remove all of the channel registrations...
		// normally this shouldn't happen which is why we print a warning message
		// if anything is actually removed
		registrations := lookupd.DB.FindRegistrations("channel", topic, "*")
		for _, r := range registrations {
			if removed, _ := lookupd.DB.RemoveProducer(*r, client.Producer); removed {
				log.Printf("WARNING: client(%s) unexpected UNREGISTER category:%s key:%s subkey:%s",
					client, "channel", topic, r.SubKey)
			}
		}
	}
	key := Registration{"topic", topic, ""}
	if removed, _ := lookupd.DB.RemoveProducer(key, client.Producer); removed {
		log.Printf("DB: client(%s) UNREGISTER category:%s key:%s subkey:%s",
			client, "topic", topic, "")
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

	log.Printf("CLIENT(%s): IDENTIFY Address:%s TCP:%d HTTP:%d Version:%s",
		client, producer.Address, producer.TcpPort, producer.HttpPort, producer.Version)

	client.Producer = &producer
	if lookupd.DB.AddProducer(Registration{"client", "", ""}, client.Producer) {
		log.Printf("DB: client(%s) REGISTER category:%s key:%s subkey:%s", client, "client", "", "")
	}

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
