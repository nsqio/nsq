package contrib

import (
	"fmt"
	"github.com/nsqio/nsq/nsqd"
	"time"
)

type NSQDDogStatsdOptions struct {
	DogStatsdAddress  string        `flag:"dogstatsd-address"`
	DogStatsdPrefix   string        `flag:"dogstatsd-prefix"`
	DogStatsdInterval time.Duration `flag:"dogstatsd-interval"`
}

func NewNSQDDogStatsdDefaultOptions() *NSQDDogStatsdOptions {
	return &NSQDDogStatsdOptions{
		DogStatsdPrefix:   "nsq.",
		DogStatsdInterval: 10 * time.Second,
	}
}

type NSQDDogStatsd struct {
	nsqd        *nsqd.NSQD
	contribOpts *NSQDContribOptions
}

func (dd *NSQDDogStatsd) Active() bool {
	if dd.contribOpts.DogStatsdAddress != "" {
		return true
	} else {
		return false
	}
}

func (dd *NSQDDogStatsd) Start() {
	logger.Println("Starting nsqd datadog")

	dd.nsqd.RegisterAddon(dd.Loop)
}

func (dd *NSQDDogStatsd) Loop() {
	// var lastMemStats *nsqd.memStats
	var lastStats []nsqd.TopicStats
	var stat string

	ticker := time.NewTicker(dd.contribOpts.DogStatsdInterval)

	dd.nsqd.Logf(nsqd.LOG_DEBUG, "Loop started")
	exitChan := *dd.nsqd.ExitChan()

	for {
		select {
		case <-exitChan:
			goto exit
		case <-ticker.C:
			dd.nsqd.Logf(nsqd.LOG_DEBUG, "LOOPING")

			client := NewDataDogClient(
				dd.contribOpts.DogStatsdAddress,
				dd.contribOpts.DogStatsdPrefix,
			)
			err := client.CreateSocket()
			if err != nil {
				dd.nsqd.Logf(nsqd.LOG_ERROR, "failed to create UDP socket to dogstatsd(%s)", client)
				continue
			}

			dd.nsqd.Logf(nsqd.LOG_INFO, "DOGSTATSD: pushing stats to %s", client)

			stats := dd.nsqd.GetStats()
			for _, topic := range stats {
				// try to find the topic in the last collection
				lastTopic := nsqd.TopicStats{}
				for _, checkTopic := range lastStats {
					if topic.TopicName == checkTopic.TopicName {
						lastTopic = checkTopic
						break
					}
				}
				diff := topic.MessageCount - lastTopic.MessageCount

				client.Incr("message_count", int64(diff), &DataDogTags{
					tags: map[string]string{"topic_name": topic.TopicName},
				})

				client.Gauge("topic.depth", topic.Depth, &DataDogTags{
					tags: map[string]string{"topic_name": topic.TopicName},
				})

				client.Gauge("topic.backend_depth", topic.BackendDepth, &DataDogTags{
					tags: map[string]string{"topic_name": topic.TopicName},
				})

				for _, item := range topic.E2eProcessingLatency.Percentiles {
					stat = fmt.Sprintf("topic.e2e_processing_latency_%.0f", item["quantile"]*100.0)
					// We can cast the value to int64 since a value of 1 is the
					// minimum resolution we will have, so there is no loss of
					// accuracy
					client.Gauge(stat, int64(item["value"]), &DataDogTags{
						tags: map[string]string{"topic_name": topic.TopicName},
					})
				}

				for _, channel := range topic.Channels {
					// try to find the channel in the last collection
					lastChannel := nsqd.ChannelStats{}
					for _, checkChannel := range lastTopic.Channels {
						if channel.ChannelName == checkChannel.ChannelName {
							lastChannel = checkChannel
							break
						}
					}
					diff := channel.MessageCount - lastChannel.MessageCount
					client.Incr("channel.message_count", int64(diff), &DataDogTags{
						tags: map[string]string{
							"topic_name":   topic.TopicName,
							"channel_name": channel.ChannelName,
						},
					})

					client.Gauge("channel.depth", channel.Depth, &DataDogTags{
						tags: map[string]string{
							"topic_name":   topic.TopicName,
							"channel_name": channel.ChannelName,
						},
					})

					client.Gauge("channel.backend_depth", channel.BackendDepth, &DataDogTags{
						tags: map[string]string{
							"topic_name":   topic.TopicName,
							"channel_name": channel.ChannelName,
						},
					})

					// stat = fmt.Sprintf("topic.%s.channel.%s.in_flight_count", topic.TopicName, channel.ChannelName)
					client.Gauge("channel.in_flight_count", int64(channel.InFlightCount), &DataDogTags{
						tags: map[string]string{
							"topic_name":   topic.TopicName,
							"channel_name": channel.ChannelName,
						},
					})

					// stat = fmt.Sprintf("topic.%s.channel.%s.deferred_count", topic.TopicName, channel.ChannelName)
					client.Gauge("channel.deferred_count", int64(channel.DeferredCount), &DataDogTags{
						tags: map[string]string{
							"topic_name":   topic.TopicName,
							"channel_name": channel.ChannelName,
						},
					})

					diff = channel.RequeueCount - lastChannel.RequeueCount
					// stat = fmt.Sprintf("topic.%s.channel.%s.requeue_count", topic.TopicName, channel.ChannelName)
					client.Incr("channel.requeue_count", int64(diff), &DataDogTags{
						tags: map[string]string{
							"topic_name":   topic.TopicName,
							"channel_name": channel.ChannelName,
						},
					})

					diff = channel.TimeoutCount - lastChannel.TimeoutCount
					// stat = fmt.Sprintf("topic.%s.channel.%s.timeout_count", topic.TopicName, channel.ChannelName)
					client.Incr("channel.timeout_count", int64(diff), &DataDogTags{
						tags: map[string]string{
							"topic_name":   topic.TopicName,
							"channel_name": channel.ChannelName,
						},
					})

					// stat = fmt.Sprintf("topic.%s.channel.%s.clients", topic.TopicName, channel.ChannelName)
					client.Gauge("channel.clients", int64(len(channel.Clients)), &DataDogTags{
						tags: map[string]string{
							"topic_name":   topic.TopicName,
							"channel_name": channel.ChannelName,
						},
					})

					for _, item := range channel.E2eProcessingLatency.Percentiles {
						stat = fmt.Sprintf("channel.e2e_processing_latency_%.0f", item["quantile"]*100.0)
						client.Gauge(stat, int64(item["value"]), &DataDogTags{
							tags: map[string]string{
								"topic_name":   topic.TopicName,
								"channel_name": channel.ChannelName,
							},
						})
					}
				}
			}
			lastStats = stats
			client.Close()
		}
	}

exit:
	ticker.Stop()
}
