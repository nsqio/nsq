package main

import (
	"errors"
	"net"
	"sync"
	"time"
)

type LookupDB struct {
	sync.RWMutex
	Topics map[string]*Topic
}

type Topic struct {
	TopicName string
	Producers []*Producer
	Channels  map[string]bool
}

type Producer struct {
	ProducerId  string // the address we receive a connection from + tcp port
	IpAddresses []string
	TCPPort     int
	HTTPPort    int
	LastUpdate  time.Time
}

func NewLookupDB() *LookupDB {
	return &LookupDB{Topics: make(map[string]*Topic)}
}

func (l *LookupDB) Update(topicName string, channelName string, producer *Producer) error {
	l.Lock()
	defer l.Unlock()

	topic, ok := l.Topics[topicName]
	if !ok {
		topic = &Topic{
			TopicName: topicName,
			Channels:  make(map[string]bool),
		}
		l.Topics[topicName] = topic
	}

	if channelName != "." {
		_, ok := topic.Channels[channelName]
		if !ok {
			topic.Channels[channelName] = true
		}
	}

	// update producer
	found := false
	for i, p := range topic.Producers {
		if p.ProducerId == producer.ProducerId {
			found = true
			topic.Producers[i] = producer
			break
		}
	}

	if found == false {
		topic.Producers = append(topic.Producers, producer)
	}

	return nil

}

func (p *Producer) identifyBestAddress(preferLocal bool) (string, error) {
	for i, address := range p.IpAddresses {
		if i == len(p.IpAddresses)-1 {
			// last entry is always hostname
			return address, nil
		}

		ip := net.ParseIP(address)
		if preferLocal && ip.IsLoopback() {
			return ip.String(), nil
		}
	}

	// should be impossible?
	return "", errors.New("no address available")
}
