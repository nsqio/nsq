package main

import (
	"bitly/simplejson"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"strings"
	"time"
)

func makeReq(endpoint string) (*simplejson.Json, error) {
	httpclient := &http.Client{}
	req, err := http.NewRequest("GET", endpoint, nil)
	if err != nil {
		return nil, err
	}

	resp, err := httpclient.Do(req)
	if err != nil {
		return nil, err
	}

	body, err := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		return nil, err
	}

	data, err := simplejson.NewJson(body)
	if err != nil {
		return nil, err
	}

	statusCode, err := data.Get("status_code").Int()
	if err != nil {
		return nil, err
	}
	if statusCode != 200 {
		return nil, errors.New(fmt.Sprintf("response status_code == %d", statusCode))
	}
	return data.Get("data"), nil
}

func getLookupdTopics(lookupdAddresses []string) ([]string, error) {
	success := false
	allTopics := make([]string, 0)
	for _, addr := range lookupdAddresses {
		// endpoint := fmt.Sprintf("http://%s/lookup?topic=%s", addr, url.QueryEscape(q.TopicName))
		endpoint := fmt.Sprintf("http://%s/topics", addr)
		log.Printf("LOOKUPD: querying %s", endpoint)

		data, err := makeReq(endpoint)
		if err != nil {
			log.Printf("ERROR: lookupd %s - %s", endpoint, err.Error())
			continue
		}
		success = true
		// do something with the data
		// {"data":{"topics":["test"]}}
		topics, _ := data.Get("topics").Array()
		allTopics = stringUnion(allTopics, topics)
	}
	if success == false {
		return nil, errors.New("unable to query any lookupd")
	}
	return allTopics, nil
}

func getLookupdTopicProducers(topic string, lookupdAddresses []string) ([]string, error) {
	success := false
	allSources := make([]string, 0)
	for _, addr := range lookupdAddresses {
		endpoint := fmt.Sprintf("http://%s/lookup?topic=%s", addr, url.QueryEscape(topic))
		log.Printf("LOOKUPD: querying %s", endpoint)

		data, err := makeReq(endpoint)
		if err != nil {
			log.Printf("ERROR: lookupd %s - %s", endpoint, err.Error())
			continue
		}
		success = true
		// do something with the data
		// "data": {
		//   "channels": [],
		//   "producers": [
		//     {
		//       "address": "jehiah-air.local",
		//       "port": "4150",
		//       "tcp_port": 4150,
		//       "http_port": 4151
		//     }
		//   ],
		//   "timestamp": 1340152173
		// },

		producers, _ := data.Get("producers").Array()
		for _, producer := range producers {
			producer := producer.(map[string]interface{})
			address := producer["address"].(string)

			var port int
			portObj, ok := producer["http_port"]
			if !ok {
				// backwards compatible
				port = int(producer["port"].(float64)) + 1
			} else {
				port = int(portObj.(float64))
			}
			key := fmt.Sprintf("%s:%d", address, port)
			allSources = stringAdd(allSources, key)
		}
	}
	if success == false {
		return nil, errors.New("unable to query any lookupd")
	}
	return allSources, nil
}

func getNSQDStats(nsqdAddresses []string, selectedTopic string) ([]TopicHostStats, map[string]*ChannelStats, error) {
	topicHostStats := make([]TopicHostStats, 0)
	channelStats := make(map[string]*ChannelStats)
	success := false
	for _, addr := range nsqdAddresses {
		endpoint := fmt.Sprintf("http://%s/stats?format=json", addr)
		log.Printf("NSQD: querying %s", endpoint)

		data, err := makeReq(endpoint)
		if err != nil {
			log.Printf("ERROR: lookupd %s - %s", endpoint, err.Error())
			continue
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
			h := TopicHostStats{
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
	}
	if success == false {
		return nil, nil, errors.New("unable to query any lookupd")
	}
	return topicHostStats, channelStats, nil

}

func stringAdd(s []string, a string) []string {
	o := s
	found := false
	for _, existing := range s {
		if a == existing {
			found = true
			return s
		}
	}
	if found == false {
		o = append(o, a)
	}
	return o

}

func stringUnion(s []string, a []interface{}) []string {
	o := s
	for _, entry := range a {
		found := false
		for _, existing := range s {
			if entry.(string) == existing {
				found = true
				break
			}
		}
		if found == false {
			o = append(o, entry.(string))
		}
	}
	return o
}
