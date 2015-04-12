package clusterinfo

import (
	"errors"
	"fmt"
	"net/url"
	"sort"
	"sync"

	"github.com/blang/semver"
	"github.com/nsqio/nsq/internal/http_api"
	"github.com/nsqio/nsq/internal/stringy"
)

type logger interface {
	Output(maxdepth int, s string) error
}

type ClusterInfo struct {
	log    logger
	client *http_api.Client
}

func New(log logger, client *http_api.Client) *ClusterInfo {
	return &ClusterInfo{
		log:    log,
		client: client,
	}
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
	err := c.client.NegotiateV1(endpoint, &resp)
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
	var success bool
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
			err := c.client.NegotiateV1(endpoint, &resp)
			if err != nil {
				c.logf("ERROR: lookupd %s - %s", endpoint, err)
				return
			}

			lock.Lock()
			defer lock.Unlock()
			success = true
			allTopics = append(allTopics, resp.Topics...)
		}(endpoint)
	}
	wg.Wait()

	if success == false {
		return nil, errors.New("unable to query any lookupd")
	}

	allTopics = stringy.Uniq(allTopics)
	sort.Strings(allTopics)
	return allTopics, nil
}

// GetLookupdTopicChannels returns a []string containing a union of the channels
// from all the given lookupd for the given topic
func (c *ClusterInfo) GetLookupdTopicChannels(topic string, lookupdHTTPAddrs []string) ([]string, error) {
	var success bool
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
			err := c.client.NegotiateV1(endpoint, &resp)
			lock.Lock()
			defer lock.Unlock()
			if err != nil {
				c.logf("ERROR: lookupd %s - %s", endpoint, err)
				return
			}
			success = true
			allChannels = append(allChannels, resp.Channels...)
		}(endpoint)
	}
	wg.Wait()

	if success == false {
		return nil, errors.New("unable to query any lookupd")
	}

	allChannels = stringy.Uniq(allChannels)
	sort.Strings(allChannels)
	return allChannels, nil
}

// GetLookupdProducers returns a ProducerList metadata for each node connected to the given lookupds
func (c *ClusterInfo) GetLookupdProducers(lookupdHTTPAddrs []string) (ProducerList, error) {
	var success bool
	var output []*Producer
	var lock sync.Mutex
	var wg sync.WaitGroup

	allProducers := make(map[string]*Producer)
	maxVersion, _ := semver.Parse("0.0.0")

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
			err := c.client.NegotiateV1(endpoint, &resp)
			if err != nil {
				c.logf("ERROR: lookupd %s - %s", endpoint, err)
				return
			}

			lock.Lock()
			defer lock.Unlock()
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

	if success == false {
		return nil, errors.New("unable to query any lookupd")
	}

	for _, producer := range allProducers {
		if producer.VersionObj.LT(maxVersion) {
			producer.OutOfDate = true
		}
	}
	sort.Sort(ProducersByHost{output})
	return output, nil
}

// GetLookupdTopicProducers returns a ProducerList of all the nsqd for a given topic by
// unioning the nodes returned from the given lookupd
func (c *ClusterInfo) GetLookupdTopicProducers(topic string, lookupdHTTPAddrs []string) (ProducerList, error) {
	var success bool
	var producerList ProducerList
	var lock sync.Mutex
	var wg sync.WaitGroup

	type respType struct {
		Producers ProducerList `json:"producers"`
	}

	for _, addr := range lookupdHTTPAddrs {
		wg.Add(1)
		endpoint := fmt.Sprintf("http://%s/lookup?topic=%s", addr, url.QueryEscape(topic))
		c.logf("LOOKUPD: querying %s", endpoint)
		go func(endpoint string) {
			defer wg.Done()

			var resp respType
			err := c.client.NegotiateV1(endpoint, &resp)
			if err != nil {
				c.logf("ERROR: lookupd %s - %s", endpoint, err)
				return
			}

			lock.Lock()
			defer lock.Unlock()
			success = true
			producerList = append(producerList, resp.Producers...)
		}(endpoint)
	}
	wg.Wait()

	if success == false {
		return nil, errors.New("unable to query any lookupd")
	}
	return producerList, nil
}

// GetNSQDTopics returns a []string containing all the topics produced by the given nsqd
func (c *ClusterInfo) GetNSQDTopics(nsqdHTTPAddrs []string) ([]string, error) {
	var success bool
	var topics []string
	var lock sync.Mutex
	var wg sync.WaitGroup

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
			err := c.client.NegotiateV1(endpoint, &resp)
			if err != nil {
				c.logf("ERROR: lookupd %s - %s", endpoint, err)
				return
			}

			lock.Lock()
			defer lock.Unlock()
			success = true
			for _, topic := range resp.Topics {
				topics = stringy.Add(topics, topic.Name)
			}
		}(endpoint)
	}
	wg.Wait()

	if success == false {
		return nil, errors.New("unable to query any nsqd")
	}

	sort.Strings(topics)
	return topics, nil
}

// GetNSQDProducers returns a ProducerList containing the metadata of all the provided nsqd
func (c *ClusterInfo) GetNSQDProducers(nsqdHTTPAddrs []string) (ProducerList, error) {
	var success bool
	var producerList ProducerList
	var lock sync.Mutex
	var wg sync.WaitGroup

	type infoRespType struct {
		Version          string `json:"version"`
		BroadcastAddress string `json:"broadcast_address"`
		Hostname         string `json:"hostname"`
		HTTPPort         int    `json:"http_port"`
		TCPPort          int    `json:"tcp_port"`
	}

	type statsRespType struct {
		Topics []struct {
			Name string `json:"topic_name"`
		} `json:"topics"`
	}

	for _, addr := range nsqdHTTPAddrs {
		wg.Add(1)

		go func(addr string) {
			defer wg.Done()

			endpoint := fmt.Sprintf("http://%s/info", addr)
			c.logf("NSQD: querying %s", endpoint)

			var infoResp infoRespType
			err := c.client.NegotiateV1(endpoint, &infoResp)
			if err != nil {
				c.logf("ERROR: nsqd %s - %s", endpoint, err)
				return
			}

			endpoint = fmt.Sprintf("http://%s/stats?format=json", addr)
			c.logf("NSQD: querying %s", endpoint)

			var statsResp statsRespType
			err = c.client.NegotiateV1(endpoint, &statsResp)
			if err != nil {
				c.logf("ERROR: nsqd %s - %s", endpoint, err)
				return
			}

			var producerTopics ProducerTopics
			for _, t := range statsResp.Topics {
				producerTopics = append(producerTopics, ProducerTopic{Topic: t.Name})
			}

			version, err := semver.Parse(infoResp.Version)
			if err != nil {
				version, _ = semver.Parse("0.0.0")
			}

			lock.Lock()
			defer lock.Unlock()
			success = true
			producerList = append(producerList, &Producer{
				Version:          infoResp.Version,
				VersionObj:       version,
				BroadcastAddress: infoResp.BroadcastAddress,
				Hostname:         infoResp.Hostname,
				HTTPPort:         infoResp.HTTPPort,
				TCPPort:          infoResp.TCPPort,
				Topics:           producerTopics,
			})
		}(addr)
	}
	wg.Wait()

	if success == false {
		return nil, errors.New("unable to query any nsqd")
	}
	return producerList, nil
}

// GetNSQDTopicProducers returns a ProducerList containing the addresses of all the nsqd
// that produce the given topic
func (c *ClusterInfo) GetNSQDTopicProducers(topic string, nsqdHTTPAddrs []string) (ProducerList, error) {
	var success bool
	var producerList ProducerList
	var lock sync.Mutex
	var wg sync.WaitGroup

	type infoRespType struct {
		Version          string `json:"version"`
		BroadcastAddress string `json:"broadcast_address"`
		Hostname         string `json:"hostname"`
		HTTPPort         int    `json:"http_port"`
		TCPPort          int    `json:"tcp_port"`
	}

	type statsRespType struct {
		Topics []struct {
			Name string `json:"topic_name"`
		} `json:"topics"`
	}

	for _, addr := range nsqdHTTPAddrs {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()

			endpoint := fmt.Sprintf("http://%s/stats?format=json", addr)
			c.logf("NSQD: querying %s", endpoint)

			var statsResp statsRespType
			err := c.client.NegotiateV1(endpoint, &statsResp)
			if err != nil {
				c.logf("ERROR: nsqd %s - %s", endpoint, err)
				return
			}

			var producerTopics ProducerTopics
			for _, t := range statsResp.Topics {
				producerTopics = append(producerTopics, ProducerTopic{Topic: t.Name})
			}

			lock.Lock()
			success = true
			lock.Unlock()

			for _, t := range statsResp.Topics {
				if t.Name == topic {
					endpoint := fmt.Sprintf("http://%s/info", addr)
					c.logf("NSQD: querying %s", endpoint)

					var infoResp infoRespType
					err := c.client.NegotiateV1(endpoint, &infoResp)
					if err != nil {
						c.logf("ERROR: nsqd %s - %s", endpoint, err)
						return
					}

					version, err := semver.Parse(infoResp.Version)
					if err != nil {
						version, _ = semver.Parse("0.0.0")
					}

					lock.Lock()
					defer lock.Unlock()
					producerList = append(producerList, &Producer{
						Version:          infoResp.Version,
						VersionObj:       version,
						BroadcastAddress: infoResp.BroadcastAddress,
						Hostname:         infoResp.Hostname,
						HTTPPort:         infoResp.HTTPPort,
						TCPPort:          infoResp.TCPPort,
						Topics:           producerTopics,
					})

					return
				}
			}
		}(addr)
	}
	wg.Wait()

	if success == false {
		return nil, errors.New("unable to query any nsqd")
	}
	return producerList, nil
}

// GetNSQDStats returns aggregate topic and channel stats from the given NSQD instances
//
// if selectedTopic is empty, this will return stats for *all* topic/channels
// and the ChannelStats dict will be keyed by topic + ':' + channel
func (c *ClusterInfo) GetNSQDStats(producerList ProducerList, selectedTopic string) ([]*TopicStats, map[string]*ChannelStats, error) {
	var lock sync.Mutex
	var wg sync.WaitGroup
	var topicStatsList TopicStatsList

	channelStatsMap := make(map[string]*ChannelStats)

	type respType struct {
		Topics []*TopicStats `json:"topics"`
	}

	success := false
	for _, p := range producerList {
		wg.Add(1)
		endpoint := fmt.Sprintf("http://%s/stats?format=json", p.HTTPAddress())
		c.logf("NSQD: querying %s", endpoint)
		go func(endpoint string, p *Producer) {
			defer wg.Done()

			var resp respType
			err := c.client.NegotiateV1(endpoint, &resp)
			if err != nil {
				c.logf("ERROR: lookupd %s - %s", endpoint, err)
				return
			}

			lock.Lock()
			defer lock.Unlock()
			success = true
			for _, topic := range resp.Topics {
				topic.Node = p.HTTPAddress()
				topic.Hostname = p.Hostname
				if selectedTopic != "" && topic.TopicName != selectedTopic {
					continue
				}
				topicStatsList = append(topicStatsList, topic)

				for _, channel := range topic.Channels {
					channel.Node = p.HTTPAddress()
					channel.Hostname = p.Hostname
					channel.TopicName = topic.TopicName
					key := channel.ChannelName
					if selectedTopic == "" {
						key = fmt.Sprintf("%s:%s", topic.TopicName, channel.ChannelName)
					}
					channelStats, ok := channelStatsMap[key]
					if !ok {
						channelStats = &ChannelStats{
							Node:        p.HTTPAddress(),
							TopicName:   topic.TopicName,
							ChannelName: channel.ChannelName,
						}
						channelStatsMap[key] = channelStats
					}
					for _, c := range channel.Clients {
						c.Node = p.HTTPAddress()
					}
					channelStats.Add(channel)
					channelStats.Clients = append(channelStats.Clients, channel.Clients...)
				}
			}
		}(endpoint, p)
	}
	wg.Wait()

	if success == false {
		return nil, nil, errors.New("unable to query any nsqd")
	}

	sort.Sort(TopicStatsByHost{topicStatsList})
	return topicStatsList, channelStatsMap, nil
}
