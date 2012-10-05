package util

import (
	"errors"
	"fmt"
	"log"
	"net/url"
	"sync"
)

func GetChannelsForTopic(topic string, lookupdAddresses []string) ([]string, error) {
	channels := make([]string, 0)
	var lock sync.Mutex
	var wg sync.WaitGroup
	success := false
	for _, addr := range lookupdAddresses {
		wg.Add(1)
		endpoint := fmt.Sprintf("http://%s/lookup?topic=%s", addr, url.QueryEscape(topic))
		log.Printf("LOOKUPD: querying %s", endpoint)
		go func(endpiont string) {
			data, err := ApiRequest(endpoint)
			lock.Lock()
			defer lock.Unlock()
			defer wg.Done()
			if err != nil {
				log.Printf("ERROR: lookupd %s - %s", endpoint, err.Error())
				return
			}
			success = true
			c, _ := data.Get("channels").Array()
			channels = StringUnion(channels, c)
		}(endpoint)
	}
	wg.Wait()
	if success == false {
		return nil, errors.New("unable to query any lookupd")
	}
	return channels, nil
}
