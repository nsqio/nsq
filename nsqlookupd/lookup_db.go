package main

import (
	"reflect"
	"time"
)

func UpdateTopic(dataInterface interface{}, params []interface{}) interface{} {
	var data map[string]interface{}

	address := params[0].(string)
	port := params[1].(string)

	if reflect.TypeOf(dataInterface) == nil {
		data = make(map[string]interface{})
		data["producers"] = make([]map[string]string, 0)
		data["channels"] = make([]string, 0)
		data["timestamp"] = time.Now().Unix()
	} else {
		data = dataInterface.(map[string]interface{})
	}

	producer := make(map[string]string)
	producer["address"] = address
	producer["port"] = port
	producers := data["producers"].([]map[string]string)
	producers = append(producers, producer)
	data["producers"] = producers

	return data
}
