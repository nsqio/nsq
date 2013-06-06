package main

import (
	"fmt"
	"github.com/bitly/nsq/util"
	"log"
	"time"
)

func statsdLoop(addr string, prefix string, interval time.Duration) {
	lastStats := make([]TopicStats, 0)
	ticker := time.NewTicker(interval)
	for {
		select {
		case <-ticker.C:
			statsd := util.NewStatsdClient(addr, prefix)
			err := statsd.CreateSocket()
			if err != nil {
				log.Printf("ERROR: failed to create UDP socket to statsd(%s)", statsd)
				continue
			}

			log.Printf("STATSD: pushing stats to %s", statsd)

			stats := nsqd.getStats()
			for _, topic := range stats {
				// try to find the topic in the last collection
				lastTopic := TopicStats{}
				for _, checkTopic := range lastStats {
					if topic.TopicName == checkTopic.TopicName {
						lastTopic = checkTopic
						break
					}
				}
				diff := topic.MessageCount - lastTopic.MessageCount
				stat := fmt.Sprintf("topic.%s.message_count", topic.TopicName)
				statsd.Incr(stat, int(diff))

				stat = fmt.Sprintf("topic.%s.depth", topic.TopicName)
				statsd.Gauge(stat, int(topic.Depth))

				stat = fmt.Sprintf("topic.%s.backend_depth", topic.TopicName)
				statsd.Gauge(stat, int(topic.BackendDepth))

				for _, channel := range topic.Channels {
					// try to find the channel in the last collection
					lastChannel := ChannelStats{}
					for _, checkChannel := range lastTopic.Channels {
						if channel.ChannelName == checkChannel.ChannelName {
							lastChannel = checkChannel
							break
						}
					}
					diff := channel.MessageCount - lastChannel.MessageCount
					stat := fmt.Sprintf("topic.%s.channel.%s.message_count", topic.TopicName, channel.ChannelName)
					statsd.Incr(stat, int(diff))

					stat = fmt.Sprintf("topic.%s.channel.%s.depth", topic.TopicName, channel.ChannelName)
					statsd.Gauge(stat, int(channel.Depth))

					stat = fmt.Sprintf("topic.%s.channel.%s.backend_depth", topic.TopicName, channel.ChannelName)
					statsd.Gauge(stat, int(channel.BackendDepth))

					stat = fmt.Sprintf("topic.%s.channel.%s.in_flight_count", topic.TopicName, channel.ChannelName)
					statsd.Gauge(stat, int(channel.InFlightCount))

					stat = fmt.Sprintf("topic.%s.channel.%s.deferred_count", topic.TopicName, channel.ChannelName)
					statsd.Gauge(stat, int(channel.DeferredCount))

					diff = channel.RequeueCount - lastChannel.RequeueCount
					stat = fmt.Sprintf("topic.%s.channel.%s.requeue_count", topic.TopicName, channel.ChannelName)
					statsd.Incr(stat, int(diff))

					diff = channel.TimeoutCount - lastChannel.TimeoutCount
					stat = fmt.Sprintf("topic.%s.channel.%s.timeout_count", topic.TopicName, channel.ChannelName)
					statsd.Incr(stat, int(diff))

					stat = fmt.Sprintf("topic.%s.channel.%s.clients", topic.TopicName, channel.ChannelName)
					statsd.Gauge(stat, len(channel.Clients))
				}
			}

			lastStats = stats
			statsd.Close()
		}
	}
	ticker.Stop()
}
