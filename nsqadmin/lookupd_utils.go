package main

import (
	"../util"
	"errors"
	"fmt"
	"log"
	"net/url"
	"strings"
	"sync"
	"time"
)

func getLookupdTopics(lookupdHTTPAddrs []string) ([]string, error) {
	success := false
	allTopics := make([]string, 0)
	var lock sync.Mutex
	var wg sync.WaitGroup
	for _, addr := range lookupdHTTPAddrs {
		wg.Add(1)
		endpoint := fmt.Sprintf("http://%s/topics", addr)
		log.Printf("LOOKUPD: querying %s", endpoint)

		go func(endpoint string) {
			data, err := util.ApiRequest(endpoint)
			lock.Lock()
			defer lock.Unlock()
			defer wg.Done()
			if err != nil {
				log.Printf("ERROR: lookupd %s - %s", endpoint, err.Error())
				return
			}
			success = true
			// {"data":{"topics":["test"]}}
			// TODO: convert this to a StringArray() function in simplejson
			topics, _ := data.Get("topics").Array()
			allTopics = util.StringUnion(allTopics, topics)
		}(endpoint)
	}
	wg.Wait()
	if success == false {
		return nil, errors.New("unable to query any lookupd")
	}
	return allTopics, nil
}

func getLookupdProducers(lookupdHTTPAddrs []string) ([]*Producer, error) {
	success := false
	allProducers := make(map[string]*Producer, 0)
	output := make([]*Producer, 0)
	maxVersion := NewVersion("0.0.0")
	var lock sync.Mutex
	var wg sync.WaitGroup

	for _, addr := range lookupdHTTPAddrs {
		wg.Add(1)
		endpoint := fmt.Sprintf("http://%s/nodes", addr)
		log.Printf("LOOKUPD: querying %s", endpoint)
		go func(endpoint string) {
			data, err := util.ApiRequest(endpoint)
			lock.Lock()
			defer lock.Unlock()
			defer wg.Done()
			if err != nil {
				log.Printf("ERROR: lookupd %s - %s", endpoint, err.Error())
				return
			}
			success = true

			producers := data.Get("producers")
			producersArray, _ := producers.Array()
			for i, _ := range producersArray {
				producer := producers.GetIndex(i)
				address := producer.Get("address").MustString()
				httpPort := producer.Get("http_port").MustInt()
				tcpPort := producer.Get("tcp_port").MustInt()
				key := fmt.Sprintf("%s:%d:%d", address, httpPort, tcpPort)
				_, ok := allProducers[key]
				if !ok {
					topicList, _ := producer.Get("topics").Array()
					var topics []string
					for _, t := range topicList {
						topics = append(topics, t.(string))
					}
					version := producer.Get("version").MustString("unknown")
					versionObj := NewVersion(version)
					if !maxVersion.Less(versionObj) {
						maxVersion = versionObj
					}
					p := &Producer{
						Address:    address,
						TcpPort:    tcpPort,
						HttpPort:   httpPort,
						Version:    version,
						VersionObj: versionObj,
						Topics:     topics,
					}
					allProducers[key] = p
					output = append(output, p)
				}
			}
		}(endpoint)
	}
	wg.Wait()
	for _, producer := range allProducers {
		if maxVersion.Less(producer.VersionObj) {
			producer.OutOfDate = true
		}
	}
	if success == false {
		return nil, errors.New("unable to query any lookupd")
	}
	return output, nil
}

func getLookupdTopicProducers(topic string, lookupdHTTPAddrs []string) ([]string, error) {
	success := false
	allSources := make([]string, 0)
	var lock sync.Mutex
	var wg sync.WaitGroup

	for _, addr := range lookupdHTTPAddrs {
		wg.Add(1)

		endpoint := fmt.Sprintf("http://%s/lookup?topic=%s", addr, url.QueryEscape(topic))
		log.Printf("LOOKUPD: querying %s", endpoint)

		go func(endpoint string) {
			data, err := util.ApiRequest(endpoint)
			lock.Lock()
			defer lock.Unlock()
			defer wg.Done()
			if err != nil {
				log.Printf("ERROR: lookupd %s - %s", endpoint, err.Error())
				return
			}
			success = true
			producers, _ := data.Get("producers").Array()
			for _, producer := range producers {
				producer := producer.(map[string]interface{})
				address := producer["address"].(string)
				port := int(producer["http_port"].(float64))
				key := fmt.Sprintf("%s:%d", address, port)
				allSources = util.StringAdd(allSources, key)
			}
		}(endpoint)
	}
	wg.Wait()
	if success == false {
		return nil, errors.New("unable to query any lookupd")
	}
	return allSources, nil
}

func getNSQDTopics(nsqdHTTPAddrs []string) ([]string, error) {
	topics := make([]string, 0)
	var lock sync.Mutex
	var wg sync.WaitGroup
	success := false
	for _, addr := range nsqdHTTPAddrs {
		wg.Add(1)
		endpoint := fmt.Sprintf("http://%s/stats?format=json", addr)
		log.Printf("NSQD: querying %s", endpoint)

		go func(endpoint string) {
			data, err := util.ApiRequest(endpoint)
			lock.Lock()
			defer lock.Unlock()
			defer wg.Done()
			if err != nil {
				log.Printf("ERROR: lookupd %s - %s", endpoint, err.Error())
				return
			}
			success = true
			topicList, _ := data.Get("topics").Array()
			for _, topicInfo := range topicList {
				topicInfo := topicInfo.(map[string]interface{})
				topicName := topicInfo["topic_name"].(string)
				topics = util.StringAdd(topics, topicName)
			}
		}(endpoint)
	}
	wg.Wait()
	if success == false {
		return nil, errors.New("unable to query any nsqd")
	}
	return topics, nil
}

func getNsqdTopicProducers(topic string, nsqdHTTPAddrs []string) ([]string, error) {
	addresses := make([]string, 0)
	var lock sync.Mutex
	var wg sync.WaitGroup
	success := false
	for _, addr := range nsqdHTTPAddrs {
		wg.Add(1)
		endpoint := fmt.Sprintf("http://%s/stats?format=json", addr)
		log.Printf("NSQD: querying %s", endpoint)

		go func(endpoint string) {
			data, err := util.ApiRequest(endpoint)
			lock.Lock()
			defer lock.Unlock()
			defer wg.Done()
			if err != nil {
				log.Printf("ERROR: lookupd %s - %s", endpoint, err.Error())
				return
			}
			success = true
			topicList, _ := data.Get("topics").Array()
			for _, topicInfo := range topicList {
				topicInfo := topicInfo.(map[string]interface{})
				topicName := topicInfo["topic_name"].(string)
				if topicName == topic {
					addresses = append(addresses, addr)
					return
				}
			}
		}(endpoint)
	}
	wg.Wait()
	if success == false {
		return nil, errors.New("unable to query any nsqd")
	}
	return addresses, nil
}

func getNSQDStats(nsqdHTTPAddrs []string, selectedTopic string) ([]*TopicHostStats, map[string]*ChannelStats, error) {
	topicHostStats := make([]*TopicHostStats, 0)
	channelStats := make(map[string]*ChannelStats)
	success := false
	var lock sync.Mutex
	var wg sync.WaitGroup
	for _, addr := range nsqdHTTPAddrs {
		wg.Add(1)
		endpoint := fmt.Sprintf("http://%s/stats?format=json", addr)
		log.Printf("NSQD: querying %s", endpoint)

		go func(endpoint string, addr string) {
			data, err := util.ApiRequest(endpoint)
			lock.Lock()
			defer lock.Unlock()
			defer wg.Done()
			if err != nil {
				log.Printf("ERROR: lookupd %s - %s", endpoint, err.Error())
				return
			}
			success = true
			topics, _ := data.Get("topics").Array()
			for _, topicInfo := range topics {
				topicInfo := topicInfo.(map[string]interface{})
				topicName := topicInfo["topic_name"].(string)
				if topicName != selectedTopic {
					continue
				}
				depth := int64(topicInfo["depth"].(float64))
				backendDepth := int64(topicInfo["backend_depth"].(float64))
				h := &TopicHostStats{
					HostAddress:  addr,
					Depth:        depth,
					BackendDepth: backendDepth,
					MemoryDepth:  depth - backendDepth,
					MessageCount: int64(topicInfo["message_count"].(float64)),
					ChannelCount: len(topicInfo["channels"].([]interface{})),
				}
				topicHostStats = append(topicHostStats, h)

				channels := topicInfo["channels"].([]interface{})
				for _, c := range channels {
					c := c.(map[string]interface{})
					channelName := c["channel_name"].(string)
					channel, ok := channelStats[channelName]
					if !ok {
						channel = &ChannelStats{ChannelName: channelName, Topic: selectedTopic}
						channelStats[channelName] = channel
					}
					h := &ChannelStats{HostAddress: addr, ChannelName: channelName, Topic: selectedTopic}
					depth := int64(c["depth"].(float64))
					backendDepth := int64(c["backend_depth"].(float64))
					h.Depth = depth
					h.BackendDepth = backendDepth
					h.MemoryDepth = depth - backendDepth
					h.InFlightCount = int64(c["in_flight_count"].(float64))
					h.DeferredCount = int64(c["deferred_count"].(float64))
					h.MessageCount = int64(c["message_count"].(float64))
					h.RequeueCount = int64(c["requeue_count"].(float64))
					h.TimeoutCount = int64(c["timeout_count"].(float64))
					clients := c["clients"].([]interface{})
					// TODO: this is sort of wrong; client's should be de-duped
					// client A that connects to NSQD-a and NSQD-b should only be counted once. right?
					h.ClientCount = len(clients)
					channel.AddHostStats(h)

					// "clients": [
					//   {
					//     "version": "V2",
					//     "remote_address": "127.0.0.1:49700",
					//     "name": "jehiah-air",
					//     "state": 3,
					//     "ready_count": 1000,
					//     "in_flight_count": 0,
					//     "message_count": 0,
					//     "finish_count": 0,
					//     "requeue_count": 0,
					//     "connect_ts": 1347150965
					//   }
					// ]
					for _, client := range clients {
						client := client.(map[string]interface{})
						connected := time.Unix(int64(client["connect_ts"].(float64)), 0)
						connectedDuration := time.Now().Sub(connected).Seconds()
						clientInfo := ClientInfo{
							HostAddress:       addr,
							ClientVersion:     client["version"].(string),
							ClientIdentifier:  fmt.Sprintf("%s:%s", client["name"].(string), strings.Split(client["remote_address"].(string), ":")[1]),
							ConnectedDuration: time.Duration(int64(connectedDuration)) * time.Second, // truncate to second
							InFlightCount:     int(client["in_flight_count"].(float64)),
							ReadyCount:        int(client["ready_count"].(float64)),
							FinishCount:       int64(client["finish_count"].(float64)),
							RequeueCount:      int64(client["requeue_count"].(float64)),
							MessageCount:      int64(client["message_count"].(float64)),
						}
						channel.Clients = append(channel.Clients, clientInfo)
					}
				}
			}
		}(endpoint, addr)
	}
	wg.Wait()
	if success == false {
		return nil, nil, errors.New("unable to query any lookupd")
	}
	return topicHostStats, channelStats, nil

}
