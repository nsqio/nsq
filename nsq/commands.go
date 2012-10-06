package nsq

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"
)

type Command struct {
	Name   []byte
	Params [][]byte
	Body   []byte
}

func (c *Command) String() string {
	if len(c.Params) > 0 {
		return fmt.Sprintf("%s %s", c.Name, string(bytes.Join(c.Params, []byte(" "))))
	}
	return string(c.Name)
}

// Announce creates a new Command to announce the existence of
// a given topic and/or channel.
// NOTE: if channel == "." then it is considered n/a
func Announce(topic string, channel string, port int, ips []string) *Command {
	var params = [][]byte{[]byte(topic), []byte(channel), []byte(strconv.Itoa(port))}
	return &Command{[]byte("ANNOUNCE"), params, []byte(strings.Join(ips, "\n"))}
}

// Identify is the first message sent to the Lookupd and provides information about the client
func Identify(version string, tcpPort int, httpPort int, address string) *Command {
	body, err := json.Marshal(struct {
		Version  string `json:"version"`
		TcpPort  int    `json:"tcp_port"`
		HttpPort int    `json:"http_port"`
		Address  string `json:"address"`
	}{
		version,
		tcpPort,
		httpPort,
		address,
	})
	if err != nil {
		log.Fatal("failed to create json %s", err.Error())
	}
	return &Command{[]byte("IDENTIFY"), [][]byte{}, body}
}

// REGISTER a topic/channel for this nsqd
func Register(topic string, channel string) *Command {
	params := [][]byte{[]byte(topic)}
	if len(channel) > 0 {
		params = append(params, []byte(channel))
	}
	return &Command{[]byte("REGISTER"), params, nil}
}

// UNREGISTER removes a topic/channel from this nsqd
func UnRegister(topic string, channel string) *Command {
	params := [][]byte{[]byte(topic)}
	if len(channel) > 0 {
		params = append(params, []byte(channel))
	}
	return &Command{[]byte("UNREGISTER"), params, nil}
}

// Ping creates a new Command to keep-alive the state of all the 
// announced topic/channels for a given client
func Ping() *Command {
	return &Command{[]byte("PING"), nil, nil}
}

// Publish creates a new Command to write a message to a given topic
func Publish(topic string, body []byte) *Command {
	var params = [][]byte{[]byte(topic)}
	return &Command{[]byte("PUB"), params, body}
}

// Subscribe creates a new Command to subscribe
// to the given topic/channel
func Subscribe(topic string, channel string, shortIdentifier string, longIdentifier string) *Command {
	var params = [][]byte{[]byte(topic), []byte(channel), []byte(shortIdentifier), []byte(longIdentifier)}
	return &Command{[]byte("SUB"), params, nil}
}

// Ready creates a new Command to specify
// the number of messages a client is willing to receive
func Ready(count int) *Command {
	var params = [][]byte{[]byte(strconv.Itoa(count))}
	return &Command{[]byte("RDY"), params, nil}
}

// Finish creates a new Command to indiciate that 
// a given message (by id) has been processed successfully
func Finish(id []byte) *Command {
	var params = [][]byte{id}
	return &Command{[]byte("FIN"), params, nil}
}

// Requeue creats a new Command to indicate that 
// a given message (by id) should be requeued after the given timeout (in ms)
// NOTE: a timeout of 0 indicates immediate requeue
func Requeue(id []byte, timeoutMs int) *Command {
	var params = [][]byte{id, []byte(strconv.Itoa(timeoutMs))}
	return &Command{[]byte("REQ"), params, nil}
}

// StartClose creates a new Command to indicate that the
// client would like to start a close cycle.  nsqd will no longer
// send messages to a client in this state and the client is expected
// to ACK after which it can finish pending messages and close the connection
func StartClose() *Command {
	return &Command{[]byte("CLS"), nil, nil}
}

func Nop() *Command {
	return &Command{[]byte("NOP"), nil, nil}
}
