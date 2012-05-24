package main

import (
	"../nsq"
	"bufio"
	"bytes"
	"log"
	"reflect"
	"strings"
	"encoding/binary"
)

type ServerLookupProtocolV1 struct {
	nsq.LookupProtocolV1
}

func init() {
	// BigEndian client byte sequence "  V1"
	var magicInt int32
	buf := bytes.NewBuffer([]byte(nsq.LookupProtocolV1Magic))
	binary.Read(buf, binary.BigEndian, &magicInt)
	Protocols[magicInt] = &ServerLookupProtocolV1{}
}

func (p *ServerLookupProtocolV1) IOLoop(client nsq.StatefulReadWriter) error {
	var err error
	var line string

	client.SetState("state", nsq.LookupClientStateV1Init)
	client.SetState("lookup_db", ldb)

	err = nil
	reader := bufio.NewReader(client)
	for {
		line, err = reader.ReadString('\n')
		if err != nil {
			break
		}

		line = strings.Replace(line, "\n", "", -1)
		line = strings.Replace(line, "\r", "", -1)
		params := strings.Split(line, " ")

		log.Printf("PROTOCOL(V1): %#v", params)

		response, err := p.Execute(client, params...)
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

func (p *ServerLookupProtocolV1) Execute(client nsq.StatefulReadWriter, params ...string) ([]byte, error) {
	var err error
	var response []byte

	typ := reflect.TypeOf(p)
	args := make([]reflect.Value, 3)
	args[0] = reflect.ValueOf(p)
	args[1] = reflect.ValueOf(client)

	cmd := strings.ToUpper(params[0])

	// use reflection to call the appropriate method for this 
	// command on the protocol object
	if method, ok := typ.MethodByName(cmd); ok {
		args[2] = reflect.ValueOf(params)
		returnValues := method.Func.Call(args)
		response = nil
		if !returnValues[0].IsNil() {
			response = returnValues[0].Interface().([]byte)
		}
		err = nil
		if !returnValues[1].IsNil() {
			err = returnValues[1].Interface().(error)
		}

		return response, err
	}

	return nil, nsq.LookupClientErrV1Invalid
}

func (p *ServerLookupProtocolV1) GET(client nsq.StatefulReadWriter, params []string) ([]byte, error) {
	var err error

	if len(params) < 2 {
		return nil, nsq.LookupClientErrV1Invalid
	}

	key := params[1]
	if len(key) == 0 {
		return nil, nsq.LookupClientErrV1Invalid
	}

	ldbInterface, _ := client.GetState("lookup_db")
	ldb := ldbInterface.(*LookupDB)

	valInterface, err := ldb.Get(key)
	if err != nil {
		return nil, err
	}

	val := valInterface.([]byte)

	return val, nil
}

func (p *ServerLookupProtocolV1) SET(client nsq.StatefulReadWriter, params []string) ([]byte, error) {
	var err error

	if len(params) < 3 {
		return nil, nsq.LookupClientErrV1Invalid
	}

	key := params[1]
	if len(key) == 0 {
		return nil, nsq.LookupClientErrV1Invalid
	}

	val := params[2]
	if len(val) == 0 {
		return nil, nsq.LookupClientErrV1Invalid
	}
	
	ldbInterface, _ := client.GetState("lookup_db")
	ldb := ldbInterface.(*LookupDB)

	err = ldb.Set(key, []byte(val))
	if err != nil {
		return nil, err
	}

	return []byte("OK"), nil
}
