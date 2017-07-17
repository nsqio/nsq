package contrib

import (
	"fmt"
	"github.com/nsqio/nsq/nsqd"
	"time"
	"flag"
	"github.com/nsqio/nsq/internal/lg"
)

type NSQDDogStatsdOptions struct {
	DogStatsdAddress  string        `flag:"dogstatsd-address"`
	DogStatsdPrefix   string        `flag:"dogstatsd-prefix"`
	DogStatsdInterval time.Duration `flag:"dogstatsd-interval"`
}

func NewNSQDDogStatsdContribFlags(opts *NSQDDogStatsdOptions) *flag.FlagSet {
	flagSet := flag.NewFlagSet("dogstatsd", flag.ExitOnError)
	flagSet.StringVar(
		&opts.DogStatsdAddress,
		"dogstatsd-address",
		"",
		"UDP <addr>:<port> of a statsd daemon for pushing stats",
	)
	flagSet.DurationVar(
		&opts.DogStatsdInterval,
		"dogstatsd-interval",
		10 * time.Second,
		"duration between pushing to dogstatsd",
	)
	// flagSet.Bool("statsd-mem-stats", opts.StatsdMemStats, "toggle sending memory and GC stats to statsd")
	flagSet.StringVar(
		&opts.DogStatsdPrefix,
		"dogstatsd-prefix",
		"nsq.",
		"prefix used for keys sent to statsd (%s for host replacement)",
	)
	return flagSet
}

func NewNSQDDogStatsd(contribOpts []string, n *nsqd.NSQD, logf lg.AppLogFunc) INSQDAddon {
	logf(nsqd.LOG_INFO, "Received options: %+v", contribOpts)

	dogStatsdOpts := &NSQDDogStatsdOptions{}
	flagSet := NewNSQDDogStatsdContribFlags(dogStatsdOpts)

	flagSet.Parse(contribOpts)
	logf(nsqd.LOG_INFO, "Parsed Options: %+v", dogStatsdOpts)

	// pass the dogstats specific opts on
	return &NSQDDogStatsd{
		opts: dogStatsdOpts,
		nsqd:        n,
		logf: logf,
	}
}

type NSQDDogStatsd struct {
	nsqd        *nsqd.NSQD
	opts *NSQDDogStatsdOptions
	logf lg.AppLogFunc
}

func (dd *NSQDDogStatsd) Enabled() bool {
	dd.logf(nsqd.LOG_INFO, "%+v", dd.opts)
	if dd.opts.DogStatsdAddress != "" {
		return true
	} else {
		return false
	}
}

func (dd *NSQDDogStatsd) Start() {
	dd.logf(nsqd.LOG_INFO, "Starting Datadog NSQD Monitor")
	dd.nsqd.RegisterAddon(dd.Loop)
}

func (dd *NSQDDogStatsd) Loop() {
	// var lastMemStats *nsqd.memStats
	var lastStats []nsqd.TopicStats
	var stat string

	ticker := time.NewTicker(dd.opts.DogStatsdInterval)

	dd.logf(nsqd.LOG_DEBUG, "Loop started")
	exitChan := *dd.nsqd.ExitChan()

	for {
		select {
		case <-exitChan:
			goto exit
		case <-ticker.C:
			dd.logf(nsqd.LOG_DEBUG, "LOOPING")

			client := NewDataDogClient(
				dd.opts.DogStatsdAddress,
				dd.opts.DogStatsdPrefix,
			)
			err := client.CreateSocket()
			if err != nil {
				dd.logf(nsqd.LOG_ERROR, "failed to create UDP socket to dogstatsd(%s)", client)
				continue
			}

			dd.logf(nsqd.LOG_INFO, "DOGSTATSD: pushing stats to %s", client)

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
