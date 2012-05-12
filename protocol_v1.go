package main

import (
	"bufio"
	"bytes"
	"log"
	"reflect"
	"strings"
)

func init() {
	// BigEndian client byte sequence "  V1"
	protocols[538990129] = &ProtocolV1{}
}

type ProtocolV1 struct{}

func (p *ProtocolV1) IOLoop(client *Client) error {
	var err error
	var line string
	var response []byte

	typ := reflect.TypeOf(p)
	args := make([]reflect.Value, 3)
	args[0] = reflect.ValueOf(p)
	args[1] = reflect.ValueOf(client)

	err = nil
	reader := bufio.NewReader(client.conn)
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
			err = client.WriteError(clientErrInvalid)
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
			err = client.WriteError(clientErrInvalid)
			if err != nil {
				break
			}
		}
	}

	return err
}

func (p *ProtocolV1) SUB(client *Client, params []string) (error, []byte) {
	if client.state != clientInit {
		return clientErrInvalid, nil
	}

	if len(params) < 3 {
		return clientErrInvalid, nil
	}

	topicName := params[1]
	if len(topicName) == 0 {
		return clientErrBadTopic, nil
	}

	channelName := params[2]
	if len(channelName) == 0 {
		return clientErrBadChannel, nil
	}

	client.state = clientWaitGet

	topic := GetTopic(topicName)
	client.channel = topic.GetChannel(channelName)
	client.channel.AddClient(client)

	return nil, nil
}

func (p *ProtocolV1) GET(client *Client, params []string) (error, []byte) {
	var err error

	if client.state != clientWaitGet {
		return clientErrInvalid, nil
	}

	// this blocks until a message is ready
	msg := client.channel.GetMessage()
	if msg == nil {
		log.Printf("ERROR: msg == nil")
		return clientErrBadMessage, nil
	}

	uuidStr := UuidToStr(msg.Uuid())

	log.Printf("PROTOCOL: writing msg(%s) to client(%s) - %s", uuidStr, client.String(), string(msg.Body()))

	buf := bytes.NewBuffer([]byte(uuidStr))
	_, err = buf.Write(msg.Body())
	if err != nil {
		return err, nil
	}

	client.state = clientWaitAck

	return nil, buf.Bytes()
}

func (p *ProtocolV1) ACK(client *Client, params []string) (error, []byte) {
	if client.state != clientWaitAck {
		return clientErrInvalid, nil
	}

	client.state = clientWaitResponse

	return nil, nil
}

func (p *ProtocolV1) FIN(client *Client, params []string) (error, []byte) {
	if client.state != clientWaitResponse {
		return clientErrInvalid, nil
	}

	if len(params) < 2 {
		return clientErrInvalid, nil
	}

	uuidStr := params[1]
	err := client.channel.FinishMessage(uuidStr)
	if err != nil {
		return err, nil
	}

	client.state = clientWaitGet

	return nil, nil
}

func (p *ProtocolV1) REQ(client *Client, params []string) (error, []byte) {
	if client.state != clientWaitResponse {
		return clientErrInvalid, nil
	}

	if len(params) < 2 {
		return clientErrInvalid, nil
	}

	uuidStr := params[1]
	err := client.channel.RequeueMessage(uuidStr)
	if err != nil {
		return err, nil
	}

	client.state = clientWaitGet

	return nil, nil
}
