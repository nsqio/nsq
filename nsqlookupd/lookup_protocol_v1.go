package main

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/bitly/go-nsq"
	"github.com/bitly/nsq/util"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"time"
)

type LookupProtocolV1 struct {
	context *Context
}

func (p *LookupProtocolV1) IOLoop(conn net.Conn) error {
	var err error
	var line string

	client := NewClientV1(conn)
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
			context := ""
			if parentErr := err.(util.ChildErr).Parent(); parentErr != nil {
				context = " - " + parentErr.Error()
			}
			log.Printf("ERROR: [%s] - %s%s", client, err.Error(), context)

			_, err = util.SendResponse(client, []byte(err.Error()))
			if err != nil {
				break
			}

			// errors of type FatalClientErr should forceably close the connection
			if _, ok := err.(*util.FatalClientErr); ok {
				break
			}
			continue
		}

		if response != nil {
			_, err = util.SendResponse(client, response)
			if err != nil {
				break
			}
		}
	}

	log.Printf("CLIENT(%s): closing", client)
	if client.peerInfo != nil {
		registrations := p.context.nsqlookupd.DB.LookupRegistrations(client.peerInfo.id)
		for _, r := range registrations {
			if removed, _ := p.context.nsqlookupd.DB.RemoveProducer(r, client.peerInfo.id); removed {
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
	return nil, util.NewFatalClientErr(nil, "E_INVALID", fmt.Sprintf("invalid command %s", params[0]))
}

func getTopicChan(command string, params []string) (string, string, error) {
	if len(params) == 0 {
		return "", "", util.NewFatalClientErr(nil, "E_INVALID", fmt.Sprintf("%s insufficient number of params", command))
	}

	topicName := params[0]
	var channelName string
	if len(params) >= 2 {
		channelName = params[1]
	}

	if !nsq.IsValidTopicName(topicName) {
		return "", "", util.NewFatalClientErr(nil, "E_BAD_TOPIC", fmt.Sprintf("%s topic name '%s' is not valid", command, topicName))
	}

	if channelName != "" && !nsq.IsValidChannelName(channelName) {
		return "", "", util.NewFatalClientErr(nil, "E_BAD_CHANNEL", fmt.Sprintf("%s channel name '%s' is not valid", command, channelName))
	}

	return topicName, channelName, nil
}

func (p *LookupProtocolV1) REGISTER(client *ClientV1, reader *bufio.Reader, params []string) ([]byte, error) {
	if client.peerInfo == nil {
		return nil, util.NewFatalClientErr(nil, "E_INVALID", "client must IDENTIFY")
	}

	topic, channel, err := getTopicChan("REGISTER", params)
	if err != nil {
		return nil, err
	}

	if channel != "" {
		key := Registration{"channel", topic, channel}
		if p.context.nsqlookupd.DB.AddProducer(key, &Producer{peerInfo: client.peerInfo}) {
			log.Printf("DB: client(%s) REGISTER category:%s key:%s subkey:%s",
				client, "channel", topic, channel)
		}
	}
	key := Registration{"topic", topic, ""}
	if p.context.nsqlookupd.DB.AddProducer(key, &Producer{peerInfo: client.peerInfo}) {
		log.Printf("DB: client(%s) REGISTER category:%s key:%s subkey:%s",
			client, "topic", topic, "")
	}

	return []byte("OK"), nil
}

func (p *LookupProtocolV1) UNREGISTER(client *ClientV1, reader *bufio.Reader, params []string) ([]byte, error) {
	if client.peerInfo == nil {
		return nil, util.NewFatalClientErr(nil, "E_INVALID", "client must IDENTIFY")
	}

	topic, channel, err := getTopicChan("UNREGISTER", params)
	if err != nil {
		return nil, err
	}

	if channel != "" {
		key := Registration{"channel", topic, channel}
		removed, left := p.context.nsqlookupd.DB.RemoveProducer(key, client.peerInfo.id)
		if removed {
			log.Printf("DB: client(%s) UNREGISTER category:%s key:%s subkey:%s",
				client, "channel", topic, channel)
		}
		// for ephemeral channels, remove the channel as well if it has no producers
		if left == 0 && strings.HasSuffix(channel, "#ephemeral") {
			p.context.nsqlookupd.DB.RemoveRegistration(key)
		}
	} else {
		// no channel was specified so this is a topic unregistration
		// remove all of the channel registrations...
		// normally this shouldn't happen which is why we print a warning message
		// if anything is actually removed
		registrations := p.context.nsqlookupd.DB.FindRegistrations("channel", topic, "*")
		for _, r := range registrations {
			if removed, _ := p.context.nsqlookupd.DB.RemoveProducer(r, client.peerInfo.id); removed {
				log.Printf("WARNING: client(%s) unexpected UNREGISTER category:%s key:%s subkey:%s",
					client, "channel", topic, r.SubKey)
			}
		}

		key := Registration{"topic", topic, ""}
		if removed, _ := p.context.nsqlookupd.DB.RemoveProducer(key, client.peerInfo.id); removed {
			log.Printf("DB: client(%s) UNREGISTER category:%s key:%s subkey:%s",
				client, "topic", topic, "")
		}
	}

	return []byte("OK"), nil
}

func (p *LookupProtocolV1) IDENTIFY(client *ClientV1, reader *bufio.Reader, params []string) ([]byte, error) {
	var err error

	if client.peerInfo != nil {
		return nil, util.NewFatalClientErr(err, "E_INVALID", "cannot IDENTIFY again")
	}

	var bodyLen int32
	err = binary.Read(reader, binary.BigEndian, &bodyLen)
	if err != nil {
		return nil, util.NewFatalClientErr(err, "E_BAD_BODY", "IDENTIFY failed to read body size")
	}

	body := make([]byte, bodyLen)
	_, err = io.ReadFull(reader, body)
	if err != nil {
		return nil, util.NewFatalClientErr(err, "E_BAD_BODY", "IDENTIFY failed to read body")
	}

	// body is a json structure with producer information
	peerInfo := PeerInfo{id: client.RemoteAddr().String()}
	err = json.Unmarshal(body, &peerInfo)
	if err != nil {
		return nil, util.NewFatalClientErr(err, "E_BAD_BODY", "IDENTIFY failed to decode JSON body")
	}

	peerInfo.RemoteAddress = client.RemoteAddr().String()
	//TODO: remove this check for 1.0
	if peerInfo.BroadcastAddress == "" {
		peerInfo.BroadcastAddress = peerInfo.Address
	}

	// require all fields
	if peerInfo.BroadcastAddress == "" || peerInfo.TcpPort == 0 || peerInfo.HttpPort == 0 || peerInfo.Version == "" {
		return nil, util.NewFatalClientErr(nil, "E_BAD_BODY", "IDENTIFY missing fields")
	}

	peerInfo.lastUpdate = time.Now()

	log.Printf("CLIENT(%s): IDENTIFY Address:%s TCP:%d HTTP:%d Version:%s",
		client, peerInfo.BroadcastAddress, peerInfo.TcpPort, peerInfo.HttpPort, peerInfo.Version)

	client.peerInfo = &peerInfo
	if p.context.nsqlookupd.DB.AddProducer(Registration{"client", "", ""}, &Producer{peerInfo: client.peerInfo}) {
		log.Printf("DB: client(%s) REGISTER category:%s key:%s subkey:%s", client, "client", "", "")
	}

	// build a response
	data := make(map[string]interface{})
	data["tcp_port"] = p.context.nsqlookupd.tcpAddr.Port
	data["http_port"] = p.context.nsqlookupd.httpAddr.Port
	data["version"] = util.BINARY_VERSION
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatalf("ERROR: unable to get hostname %s", err.Error())
	}
	data["address"] = hostname //TODO: remove for 1.0
	data["broadcast_address"] = p.context.nsqlookupd.broadcastAddress
	data["hostname"] = hostname

	response, err := json.Marshal(data)
	if err != nil {
		log.Printf("ERROR: marshaling %v", data)
		return []byte("OK"), nil
	}
	return response, nil
}

func (p *LookupProtocolV1) PING(client *ClientV1, params []string) ([]byte, error) {
	if client.peerInfo != nil {
		// we could get a PING before other commands on the same client connection
		now := time.Now()
		log.Printf("CLIENT(%s): pinged (last ping %s)", client.peerInfo.id, now.Sub(client.peerInfo.lastUpdate))
		client.peerInfo.lastUpdate = now
	}
	return []byte("OK"), nil
}
