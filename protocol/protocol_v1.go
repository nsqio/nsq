package protocol

import (
	"../message"
	"../util"
	"bufio"
	"bytes"
	"log"
	"reflect"
	"strings"
)

const (
	ClientInit         = 0
	ClientWaitGet      = 1
	ClientWaitAck      = 2
	ClientWaitResponse = 3
)

var (
	ClientErrInvalid    = ClientError{"E_INVALID"}
	ClientErrBadTopic   = ClientError{"E_BAD_TOPIC"}
	ClientErrBadChannel = ClientError{"E_BAD_CHANNEL"}
	ClientErrBadMessage = ClientError{"E_BAD_MESSAGE"}
)

func init() {
	// BigEndian client byte sequence "  V1"
	Protocols[538990129] = &ProtocolV1{}
}

type ProtocolV1 struct {
	channel *message.Channel
}

func (p *ProtocolV1) IOLoop(client Client) error {
	var err error
	var line string
	var response []byte

	client.SetState(ClientInit)

	typ := reflect.TypeOf(p)
	args := make([]reflect.Value, 3)
	args[0] = reflect.ValueOf(p)
	args[1] = reflect.ValueOf(client)

	err = nil
	reader := bufio.NewReader(client.GetConnection())
	for {
		line, err = reader.ReadString('\n')
		if err != nil {
			break
		}

		line = strings.Replace(line, "\n", "", -1)
		line = strings.Replace(line, "\r", "", -1)
		params := strings.Split(line, " ")
		cmd := params[0]

		log.Printf("PROTOCOL: %#v", params)

		// don't let them h@x0r
		if cmd == "IOLoop" {
			err = client.WriteError(ClientErrInvalid)
			if err != nil {
				break
			}
			continue
		}

		// use reflection to call the appropriate method for this 
		// command on the protocol object
		if method, ok := typ.MethodByName(cmd); ok {
			args[2] = reflect.ValueOf(params)
			returnValues := method.Func.Call(args)
			err = nil
			if !returnValues[0].IsNil() {
				err = returnValues[0].Interface().(error)
			}
			response = nil
			if !returnValues[1].IsNil() {
				response = returnValues[1].Interface().([]byte)
			}

			if err != nil {
				err = client.WriteError(err)
				if err != nil {
					break
				}
				continue
			}

			if response != nil {
				err = client.Write(response)
				if err != nil {
					break
				}
			}
		} else {
			err = client.WriteError(ClientErrInvalid)
			if err != nil {
				break
			}
		}
	}

	if p.channel != nil {
		// p.channel.RemoveClient(c)
	}

	return err
}

func (p *ProtocolV1) SUB(client Client, params []string) (error, []byte) {
	if client.GetState() != ClientInit {
		return ClientErrInvalid, nil
	}

	if len(params) < 3 {
		return ClientErrInvalid, nil
	}

	topicName := params[1]
	if len(topicName) == 0 {
		return ClientErrBadTopic, nil
	}

	channelName := params[2]
	if len(channelName) == 0 {
		return ClientErrBadChannel, nil
	}

	client.SetState(ClientWaitGet)

	topic := message.GetTopic(topicName)
	p.channel = topic.GetChannel(channelName)
	// client.channel.AddClient(client)

	return nil, nil
}

func (p *ProtocolV1) GET(client Client, params []string) (error, []byte) {
	var err error

	if client.GetState() != ClientWaitGet {
		return ClientErrInvalid, nil
	}

	// this blocks until a message is ready
	msg := p.channel.GetMessage()
	if msg == nil {
		log.Printf("ERROR: msg == nil")
		return ClientErrBadMessage, nil
	}

	uuidStr := util.UuidToStr(msg.Uuid())

	log.Printf("PROTOCOL: writing msg(%s) to client(%s) - %s", uuidStr, client.String(), string(msg.Body()))

	buf := bytes.NewBuffer([]byte(uuidStr))
	_, err = buf.Write(msg.Body())
	if err != nil {
		return err, nil
	}

	client.SetState(ClientWaitAck)

	return nil, buf.Bytes()
}

func (p *ProtocolV1) ACK(client Client, params []string) (error, []byte) {
	if client.GetState() != ClientWaitAck {
		return ClientErrInvalid, nil
	}

	client.SetState(ClientWaitResponse)

	return nil, nil
}

func (p *ProtocolV1) FIN(client Client, params []string) (error, []byte) {
	if client.GetState() != ClientWaitResponse {
		return ClientErrInvalid, nil
	}

	if len(params) < 2 {
		return ClientErrInvalid, nil
	}

	uuidStr := params[1]
	err := p.channel.FinishMessage(uuidStr)
	if err != nil {
		return err, nil
	}

	client.SetState(ClientWaitGet)

	return nil, nil
}

func (p *ProtocolV1) REQ(client Client, params []string) (error, []byte) {
	if client.GetState() != ClientWaitResponse {
		return ClientErrInvalid, nil
	}

	if len(params) < 2 {
		return ClientErrInvalid, nil
	}

	uuidStr := params[1]
	err := p.channel.RequeueMessage(uuidStr)
	if err != nil {
		return err, nil
	}

	client.SetState(ClientWaitGet)

	return nil, nil
}
