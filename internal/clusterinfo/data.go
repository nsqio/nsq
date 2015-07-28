package clusterinfo

import (
	"errors"
	"fmt"
	"net/url"
	"sort"
	"sync"

	"github.com/bitly/nsq/internal/http_api"
	"github.com/bitly/nsq/internal/stringy"
	"github.com/blang/semver"
)

type logger interface {
	Output(maxdepth int, s string) error
}

type ClusterInfo struct {
	log logger
}

func New(log logger) *ClusterInfo {
	return &ClusterInfo{log}
}

func (c *ClusterInfo) logf(f string, args ...interface{}) {
	if c.log == nil {
		return
	}
	c.log.Output(2, fmt.Sprintf(f, args...))
}

// GetVersion returns a semver.Version object by querying /info
func (c *ClusterInfo) GetVersion(addr string) (semver.Version, error) {
	endpoint := fmt.Sprintf("http://%s/info", addr)
	c.logf("version negotiation %s", endpoint)
	var resp struct {
		Version string `json:'version'`
	}
	err := http_api.NegotiateV1(endpoint, &resp)
	if err != nil {
		c.logf("ERROR: %s - %s", endpoint, err)
		return semver.Version{}, err
	}
	if resp.Version == "" {
		resp.Version = "unknown"
	}
	return semver.Parse(resp.Version)
}

// GetLookupdTopics returns a []string containing a union of all the topics
// from all the given lookupd
func (c *ClusterInfo) GetLookupdTopics(lookupdHTTPAddrs []string) ([]string, error) {
	success := false
	var allTopics []string
	var lock sync.Mutex
	var wg sync.WaitGroup

	type respType struct {
		Topics []string `json:"topics"`
	}

	for _, addr := range lookupdHTTPAddrs {
		wg.Add(1)
		endpoint := fmt.Sprintf("http://%s/topics", addr)
		c.logf("LOOKUPD: querying %s", endpoint)
		go func(endpoint string) {
			defer wg.Done()

			var resp respType
			err := http_api.NegotiateV1(endpoint, &resp)
			lock.Lock()
			defer lock.Unlock()
			if err != nil {
				c.logf("ERROR: lookupd %s - %s", endpoint, err.Error())
				return
			}
			success = true
			allTopics = append(allTopics, resp.Topics...)
		}(endpoint)
	}
	wg.Wait()
	allTopics = stringy.Uniq(allTopics)
	sort.Strings(allTopics)
	if success == false {
		return nil, errors.New("unable to query any lookupd")
	}
	return allTopics, nil
}

// GetLookupdTopicChannels returns a []string containing a union of the channels
// from all the given lookupd for the given topic
func (c *ClusterInfo) GetLookupdTopicChannels(topic string, lookupdHTTPAddrs []string) ([]string, error) {
	success := false
	var allChannels []string
	var lock sync.Mutex
	var wg sync.WaitGroup

	type respType struct {
		Channels []string `json:"channels"`
	}

	for _, addr := range lookupdHTTPAddrs {
		wg.Add(1)
		endpoint := fmt.Sprintf("http://%s/channels?topic=%s", addr, url.QueryEscape(topic))
		c.logf("LOOKUPD: querying %s", endpoint)
		go func(endpoint string) {
			defer wg.Done()

			var resp respType
			err := http_api.NegotiateV1(endpoint, &resp)
			lock.Lock()
			defer lock.Unlock()
			if err != nil {
				c.logf("ERROR: lookupd %s - %s", endpoint, err.Error())
				return
			}
			success = true
			allChannels = append(allChannels, resp.Channels...)
		}(endpoint)
	}
	wg.Wait()
	allChannels = stringy.Uniq(allChannels)
	sort.Strings(allChannels)
	if success == false {
		return nil, errors.New("unable to query any lookupd")
	}
	return allChannels, nil
}

// GetLookupdProducers returns a slice of pointers to Producer structs
// containing metadata for each node connected to given lookupds
func (c *ClusterInfo) GetLookupdProducers(lookupdHTTPAddrs []string) ([]*Producer, error) {
	success := false
	allProducers := make(map[string]*Producer)
	var output []*Producer
	maxVersion, _ := semver.Parse("0.0.0")
	var lock sync.Mutex
	var wg sync.WaitGroup

	type respType struct {
		Producers []*Producer `json:"producers"`
	}

	for _, addr := range lookupdHTTPAddrs {
		wg.Add(1)
		endpoint := fmt.Sprintf("http://%s/nodes", addr)
		c.logf("LOOKUPD: querying %s", endpoint)

		go func(addr string, endpoint string) {
			defer wg.Done()

			var resp respType
			err := http_api.NegotiateV1(endpoint, &resp)
			lock.Lock()
			defer lock.Unlock()
			if err != nil {
				c.logf("ERROR: lookupd %s - %s", endpoint, err.Error())
				return
			}
			success = true
			for _, producer := range resp.Producers {
				key := producer.TCPAddress()
				p, ok := allProducers[key]
				if !ok {
					if maxVersion.LT(producer.VersionObj) {
						maxVersion = producer.VersionObj
					}
					sort.Sort(producer.Topics)
					p = producer
					allProducers[key] = p
					output = append(output, p)
				}
				p.RemoteAddresses = append(p.RemoteAddresses, fmt.Sprintf("%s/%s", addr, producer.Address()))
			}
		}(addr, endpoint)
	}
	wg.Wait()
	for _, producer := range allProducers {
		if producer.VersionObj.LT(maxVersion) {
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
func (c *ClusterInfo) GetLookupdTopicProducers(topic string, lookupdHTTPAddrs []string) ([]string, error) {
	success := false
	var allSources []string
	var lock sync.Mutex
	var wg sync.WaitGroup

	type respType struct {
		Producers []Producer `json:"producers"`
	}

	for _, addr := range lookupdHTTPAddrs {
		wg.Add(1)

		endpoint := fmt.Sprintf("http://%s/lookup?topic=%s", addr, url.QueryEscape(topic))
		c.logf("LOOKUPD: querying %s", endpoint)

		go func(endpoint string) {
			defer wg.Done()

			var resp respType
			err := http_api.NegotiateV1(endpoint, &resp)
			lock.Lock()
			defer lock.Unlock()
			if err != nil {
				c.logf("ERROR: lookupd %s - %s", endpoint, err.Error())
				return
			}
			success = true

			for _, producer := range resp.Producers {
				allSources = stringy.Add(allSources, producer.HTTPAddress())
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
func (c *ClusterInfo) GetNSQDTopics(nsqdHTTPAddrs []string) ([]string, error) {
	var topics []string
	var lock sync.Mutex
	var wg sync.WaitGroup
	success := false

	type respType struct {
		Topics []struct {
			Name string `json:"topic_name"`
		} `json:"topics"`
	}

	for _, addr := range nsqdHTTPAddrs {
		wg.Add(1)
		endpoint := fmt.Sprintf("http://%s/stats?format=json", addr)
		c.logf("NSQD: querying %s", endpoint)

		go func(endpoint string) {
			defer wg.Done()

			var resp respType
			err := http_api.NegotiateV1(endpoint, &resp)
			lock.Lock()
			defer lock.Unlock()
			if err != nil {
				c.logf("ERROR: lookupd %s - %s", endpoint, err.Error())
				return
			}
			success = true
			for _, topic := range resp.Topics {
				topics = stringy.Add(topics, topic.Name)
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
func (c *ClusterInfo) GetNSQDTopicProducers(topic string, nsqdHTTPAddrs []string) ([]string, error) {
	var addresses []string
	var lock sync.Mutex
	var wg sync.WaitGroup
	success := false

	type respType struct {
		Topics []struct {
			Name string `json:"topic_name"`
		} `json:"topics"`
	}

	for _, addr := range nsqdHTTPAddrs {
		wg.Add(1)
		endpoint := fmt.Sprintf("http://%s/stats?format=json", addr)
		c.logf("NSQD: querying %s", endpoint)

		go func(endpoint, addr string) {
			defer wg.Done()

			var resp respType
			err := http_api.NegotiateV1(endpoint, &resp)
			lock.Lock()
			defer lock.Unlock()
			if err != nil {
				c.logf("ERROR: nsqd %s - %s", endpoint, err.Error())
				return
			}
			success = true

			for _, t := range resp.Topics {
				if t.Name == topic {
					addresses = append(addresses, addr)
					return
				}
			}
		}(endpoint, addr)
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
func (c *ClusterInfo) GetNSQDStats(nsqdHTTPAddrs []string, selectedTopic string) ([]*TopicStats, map[string]*ChannelStats, error) {
	var lock sync.Mutex
	var wg sync.WaitGroup

	var topicStatsList TopicStatsList
	channelStatsMap := make(map[string]*ChannelStats)

	type respType struct {
		Topics []*TopicStats `json:"topics"`
	}

	success := false
	for _, addr := range nsqdHTTPAddrs {
		wg.Add(1)
		endpoint := fmt.Sprintf("http://%s/stats?format=json", addr)
		c.logf("NSQD: querying %s", endpoint)

		go func(endpoint string, addr string) {
			defer wg.Done()

			var resp respType
			err := http_api.NegotiateV1(endpoint, &resp)
			lock.Lock()
			defer lock.Unlock()

			if err != nil {
				c.logf("ERROR: lookupd %s - %s", endpoint, err.Error())
				return
			}
			success = true

			for _, topic := range resp.Topics {
				topic.Node = addr
				if selectedTopic != "" && topic.TopicName != selectedTopic {
					continue
				}
				topicStatsList = append(topicStatsList, topic)

				for _, channel := range topic.Channels {
					channel.Node = addr
					channel.TopicName = topic.TopicName
					key := channel.ChannelName
					if selectedTopic == "" {
						key = fmt.Sprintf("%s:%s", topic.TopicName, channel.ChannelName)
					}
					channelStats, ok := channelStatsMap[key]
					if !ok {
						channelStats = &ChannelStats{
							Node:        addr,
							TopicName:   topic.TopicName,
							ChannelName: channel.ChannelName,
						}
						channelStatsMap[key] = channelStats
					}
					for _, c := range channel.Clients {
						c.Node = addr
					}
					channelStats.Add(channel)
					channelStats.Clients = append(channelStats.Clients, channel.Clients...)
				}
			}
		}(endpoint, addr)
	}
	wg.Wait()
	if success == false {
		return nil, nil, errors.New("unable to query any nsqd")
	}
	sort.Sort(TopicStatsByHost{topicStatsList})
	return topicStatsList, channelStatsMap, nil
}
