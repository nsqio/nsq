package main

import (
	"log"
	"reflect"
	"time"
)

func UpdateTopic(dataInterface interface{}, params []interface{}) (interface{}, error) {
	var data map[string]interface{}

	topicName := params[0].(string)
	channelName := params[1].(string)
	id := params[2].(string)
	address := params[3].(string)
	port := params[4].(int)

	if reflect.TypeOf(dataInterface) == nil {
		data = make(map[string]interface{})
		data["topic"] = topicName
		data["producers"] = make([]map[string]interface{}, 0)
		data["channels"] = make([]string, 0)
		data["timestamp"] = time.Now().Unix()
	} else {
		data = dataInterface.(map[string]interface{})
	}

	// add topic to list of producers, avoid duplicates
	producers := data["producers"].([]map[string]interface{})
	found := false
	var idx int
	for i, entry := range producers {
		if entry["id"].(string) == id {
			found = true
			idx = i
		}
	}

	if !found {
		producer := make(map[string]interface{})
		producer["id"] = id
		producer["port"] = port
		ips := make([]string, 1)
		ips[0] = address
		producer["ips"] = ips
		data["producers"] = append(producers, producer)
	} else {
		found = false
		ips := producers[idx]["ips"].([]string)
		for _, entry := range ips {
			if entry == address {
				found = true
			}
		}
		if !found {
			producers[idx]["ips"] = append(ips, address)
		}
	}

	// add channel to list of channels, avoid duplicates
	if channelName != "." {
		channels := data["channels"].([]string)
		found = false
		for _, entry := range channels {
			if entry == channelName {
				found = true
			}
		}

		if !found {
			data["channels"] = append(channels, channelName)
		}
	}

	log.Printf("LOOKUP: data %+v", data)

	return data, nil
}
