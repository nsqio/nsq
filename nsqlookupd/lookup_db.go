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
	address := params[2].(string)
	port := params[3].(int)

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
	for _, entry := range producers {
		if entry["address"].(string) == address && entry["port"].(int) == port {
			found = true
		}
	}

	if !found {
		producer := make(map[string]interface{})
		producer["address"] = address
		producer["port"] = port
		data["producers"] = append(producers, producer)
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

	log.Printf("LOOKUP: data %#v", data)

	return data, nil
}
