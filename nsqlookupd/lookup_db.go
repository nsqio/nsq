package main

import (
	"log"
	"reflect"
	"time"
)

func UpdateTopic(dataInterface interface{}, params []interface{}) (interface{}, error) {
	var data map[string]interface{}

	address := params[0].(string)
	port := params[1].(int)

	if reflect.TypeOf(dataInterface) == nil {
		data = make(map[string]interface{})
		data["producers"] = make([]map[string]interface{}, 0)
		data["channels"] = make([]string, 0)
		data["timestamp"] = time.Now().Unix()
	} else {
		data = dataInterface.(map[string]interface{})
	}

	// avoid duplicates
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
		producers = append(producers, producer)
		data["producers"] = producers
	}

	log.Printf("%#v", data)

	return data, nil
}
