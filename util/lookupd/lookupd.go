package lookupd

import (
	"errors"
	"fmt"
	"github.com/bitly/nsq/nsq"
	"github.com/bitly/nsq/util"
	"github.com/bitly/nsq/util/semver"
	"log"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"
)

// GetLookupdTopics returns a []string containing a union of all the topics
// from all the given lookupd
func GetLookupdTopics(lookupdHTTPAddrs []string) ([]string, error) {
	success := false
	allTopics := make([]string, 0)
	var lock sync.Mutex
	var wg sync.WaitGroup
	for _, addr := range lookupdHTTPAddrs {
		wg.Add(1)
		endpoint := fmt.Sprintf("http://%s/topics", addr)
		log.Printf("LOOKUPD: querying %s", endpoint)

		go func(endpoint string) {
			data, err := nsq.ApiRequest(endpoint)
			lock.Lock()
			defer lock.Unlock()
			defer wg.Done()
			if err != nil {
				log.Printf("ERROR: lookupd %s - %s", endpoint, err.Error())
				return
			}
			success = true
			// {"data":{"topics":["test"]}}
			topics, _ := data.Get("topics").Array()
			allTopics = util.StringUnion(allTopics, topics)
		}(endpoint)
	}
	wg.Wait()
	sort.Strings(allTopics)
	if success == false {
		return nil, errors.New("unable to query any lookupd")
	}
	return allTopics, nil
}

// GetLookupdTopicChannels returns a []string containing a union of the channels
// from all the given lookupd for the given topic
func GetLookupdTopicChannels(topic string, lookupdHTTPAddrs []string) ([]string, error) {
	success := false
	allChannels := make([]string, 0)
	var lock sync.Mutex
	var wg sync.WaitGroup
	for _, addr := range lookupdHTTPAddrs {
		wg.Add(1)
		endpoint := fmt.Sprintf("http://%s/channels?topic=%s", addr, url.QueryEscape(topic))
		log.Printf("LOOKUPD: querying %s", endpoint)
		go func(endpoint string) {
			data, err := nsq.ApiRequest(endpoint)
			lock.Lock()
			defer lock.Unlock()
			defer wg.Done()
			if err != nil {
				log.Printf("ERROR: lookupd %s - %s", endpoint, err.Error())
				return
			}
			success = true
			// {"data":{"channels":["test"]}}
			channels, _ := data.Get("channels").Array()
			allChannels = util.StringUnion(allChannels, channels)
		}(endpoint)
	}
	wg.Wait()
	sort.Strings(allChannels)
	if success == false {
		return nil, errors.New("unable to query any lookupd")
	}
	return allChannels, nil
}

// GetLookupdProducers returns a slice of pointers to Producer structs
// containing metadata for each node connected to given lookupds
func GetLookupdProducers(lookupdHTTPAddrs []string) ([]*Producer, error) {
	success := false
	allProducers := make(map[string]*Producer, 0)
	output := make([]*Producer, 0)
	maxVersion, _ := semver.Parse("0.0.0")
	var lock sync.Mutex
	var wg sync.WaitGroup

	for _, addr := range lookupdHTTPAddrs {
		wg.Add(1)
		endpoint := fmt.Sprintf("http://%s/nodes", addr)
		log.Printf("LOOKUPD: querying %s", endpoint)
		go func(addr string, endpoint string) {
			data, err := nsq.ApiRequest(endpoint)
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
			for i := range producersArray {
				producer := producers.GetIndex(i)
				remoteAddress := producer.Get("remote_address").MustString()
				if remoteAddress == "" {
					remoteAddress = "NA"
				}
				address := producer.Get("address").MustString() //TODO: remove for 1.0
				hostname := producer.Get("hostname").MustString()
				broadcastAddress := producer.Get("broadcast_address").MustString()
				if broadcastAddress == "" {
					broadcastAddress = address
				}
				httpPort := producer.Get("http_port").MustInt()
				tcpPort := producer.Get("tcp_port").MustInt()
				key := fmt.Sprintf("%s:%d:%d", broadcastAddress, httpPort, tcpPort)
				p, ok := allProducers[key]
				if !ok {
					var tombstones []bool
					var topics ProducerTopics

					topicList, _ := producer.Get("topics").Array()
					tombstoneList, err := producer.Get("tombstones").Array()
					if err != nil {
						// backwards compatibility with nsqlookupd < v0.2.22
						tombstones = make([]bool, len(topicList))
					} else {
						for _, t := range tombstoneList {
							tombstones = append(tombstones, t.(bool))
						}
					}

					for i, t := range topicList {
						topics = append(topics, ProducerTopic{
							Topic:      t.(string),
							Tombstoned: tombstones[i],
						})
					}

					sort.Sort(topics)

					version := producer.Get("version").MustString("unknown")
					versionObj, err := semver.Parse(version)
					if err != nil {
						versionObj = maxVersion
					}
					if maxVersion.Less(versionObj) {
						maxVersion = versionObj
					}

					p = &Producer{
						Address:          address, //TODO: remove for 1.0
						Hostname:         hostname,
						BroadcastAddress: broadcastAddress,
						TcpPort:          tcpPort,
						HttpPort:         httpPort,
						Version:          version,
						VersionObj:       versionObj,
						Topics:           topics,
					}
					allProducers[key] = p
					output = append(output, p)
				}
				p.RemoteAddresses = append(p.RemoteAddresses, fmt.Sprintf("%s/%s", addr, remoteAddress))
			}
		}(addr, endpoint)
	}
	wg.Wait()
	for _, producer := range allProducers {
		if producer.VersionObj.Less(maxVersion) {
			producer.OutOfDate = true
		}
	}
	sort.Sort(ProducersByHost{output})
	if success == false {
		return nil, errors.New("unable to query any lookupd")
	}
	return output, nil
}

// GetLookupdTopicProducers returns a []string of the broadcast_address:http_port of all the
// producers for a given topic by unioning the results returned from the given lookupd
func GetLookupdTopicProducers(topic string, lookupdHTTPAddrs []string) ([]string, error) {
	success := false
	allSources := make([]string, 0)
	var lock sync.Mutex
	var wg sync.WaitGroup

	for _, addr := range lookupdHTTPAddrs {
		wg.Add(1)

		endpoint := fmt.Sprintf("http://%s/lookup?topic=%s", addr, url.QueryEscape(topic))
		log.Printf("LOOKUPD: querying %s", endpoint)

		go func(endpoint string) {
			data, err := nsq.ApiRequest(endpoint)
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
			for i := range producersArray {
				producer := producers.GetIndex(i)
				address := producer.Get("address").MustString() //TODO: remove for 1.0
				broadcastAddress := producer.Get("broadcast_address").MustString()
				if broadcastAddress == "" {
					broadcastAddress = address
				}
				httpPort := producer.Get("http_port").MustInt()
				key := fmt.Sprintf("%s:%d", broadcastAddress, httpPort)
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

// GetNSQDTopics returns a []string containing all the topics
// produced by the given nsqd
func GetNSQDTopics(nsqdHTTPAddrs []string) ([]string, error) {
	topics := make([]string, 0)
	var lock sync.Mutex
	var wg sync.WaitGroup
	success := false
	for _, addr := range nsqdHTTPAddrs {
		wg.Add(1)
		endpoint := fmt.Sprintf("http://%s/stats?format=json", addr)
		log.Printf("NSQD: querying %s", endpoint)

		go func(endpoint string) {
			data, err := nsq.ApiRequest(endpoint)
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
	sort.Strings(topics)
	if success == false {
		return nil, errors.New("unable to query any nsqd")
	}
	return topics, nil
}

// GetNSQDTopicProducers returns a []string containing the addresses of all the nsqd
// that produce the given topic out of the given nsqd
func GetNSQDTopicProducers(topic string, nsqdHTTPAddrs []string) ([]string, error) {
	addresses := make([]string, 0)
	var lock sync.Mutex
	var wg sync.WaitGroup
	success := false
	for _, addr := range nsqdHTTPAddrs {
		wg.Add(1)
		endpoint := fmt.Sprintf("http://%s/stats?format=json", addr)
		log.Printf("NSQD: querying %s", endpoint)

		go func(endpoint string) {
			data, err := nsq.ApiRequest(endpoint)
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

// GetNSQDStats returns aggregate topic and channel stats from the given NSQD instances
//
// if selectedTopic is empty, this will return stats for *all* topic/channels
// and the ChannelStats dict will be keyed by topic + ':' + channel
func GetNSQDStats(nsqdHTTPAddrs []string, selectedTopic string) ([]*TopicStats, map[string]*ChannelStats, error) {
	topicStats := make([]*TopicStats, 0)
	channelStats := make(map[string]*ChannelStats)
	success := false
	var lock sync.Mutex
	var wg sync.WaitGroup
	for _, addr := range nsqdHTTPAddrs {
		wg.Add(1)
		endpoint := fmt.Sprintf("http://%s/stats?format=json", addr)
		log.Printf("NSQD: querying %s", endpoint)

		go func(endpoint string, addr string) {
			data, err := nsq.ApiRequest(endpoint)
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
				if selectedTopic != "" && topicName != selectedTopic {
					continue
				}
				depth := int64(topicInfo["depth"].(float64))
				backendDepth := int64(topicInfo["backend_depth"].(float64))
				h := &TopicStats{
					HostAddress:  addr,
					Depth:        depth,
					BackendDepth: backendDepth,
					MemoryDepth:  depth - backendDepth,
					MessageCount: int64(topicInfo["message_count"].(float64)),
					ChannelCount: len(topicInfo["channels"].([]interface{})),
					Topic:        topicName,
				}
				topicStats = append(topicStats, h)

				channels := topicInfo["channels"].([]interface{})
				for _, c := range channels {
					c := c.(map[string]interface{})
					channelName := c["channel_name"].(string)
					channelStatsKey := channelName
					if selectedTopic == "" {
						channelStatsKey = fmt.Sprintf("%s:%s", topicName, channelName)
					}
					channel, ok := channelStats[channelStatsKey]
					if !ok {
						channel = &ChannelStats{
							ChannelName: channelName,
							Topic:       topicName,
						}
						channelStats[channelStatsKey] = channel
					}
					h := &ChannelStats{HostAddress: addr, ChannelName: channelName, Topic: topicName}
					depth := int64(c["depth"].(float64))
					backendDepth := int64(c["backend_depth"].(float64))
					h.Depth = depth
					var paused bool
					pausedInterface, ok := c["paused"]
					if ok {
						paused = pausedInterface.(bool)
					}
					h.Paused = paused
					h.BackendDepth = backendDepth
					h.MemoryDepth = depth - backendDepth
					h.InFlightCount = int64(c["in_flight_count"].(float64))
					h.DeferredCount = int64(c["deferred_count"].(float64))
					h.MessageCount = int64(c["message_count"].(float64))
					h.RequeueCount = int64(c["requeue_count"].(float64))
					h.TimeoutCount = int64(c["timeout_count"].(float64))
					clients := c["clients"].([]interface{})
					// TODO: this is sort of wrong; clients should be de-duped
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
						clientInfo := &ClientInfo{
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
					sort.Sort(ClientsByHost{channel.Clients})
				}
			}
			sort.Sort(TopicStatsByHost{topicStats})
		}(endpoint, addr)
	}
	wg.Wait()
	if success == false {
		return nil, nil, errors.New("unable to query any nsqd")
	}
	return topicStats, channelStats, nil
}
