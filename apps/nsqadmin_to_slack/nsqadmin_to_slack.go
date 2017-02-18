package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
	"net/http"

	"github.com/nsqio/go-nsq"
	"github.com/nsqio/nsq/internal/version"
	"github.com/nsqio/nsq/internal/app"
	"github.com/nsqio/nsq/apps/nsqadmin_to_slack/internal/slack"
)


func main() {
	topic         := flag.String("topic", "", "NSQ topic")
	channel       := flag.String("channel", "nsqadmin_to_hipchat", "NSQ channel")
	slackChannel  := flag.String("slack-channel", "", "Slack channel. i.e. #test")
	slackToken    := flag.String("slack-token", "", "Slack API Token (may alternately be specified via SLACK_TOKEN environment variable)")
	maxInFlight   := flag.Int("max-in-flight", 200, "max number of messages to allow in flight")
	showVersion := flag.Bool("version", false, "print version string")
	nsqdTCPAddrs     := app.StringArray{}
	lookupdHTTPAddrs := app.StringArray{}
	flag.Var(&nsqdTCPAddrs, "nsqd-tcp-address", "nsqd TCP address (may be given multiple times)")
	flag.Var(&lookupdHTTPAddrs, "lookupd-http-address", "lookupd HTTP address (may be given multiple times)")
	cfg := nsq.NewConfig()
	flag.Var(&nsq.ConfigFlag{cfg}, "consumer-opt", "option to passthrough to nsq.Consumer (may be given multiple times, http://godoc.org/github.com/nsqio/go-nsq#Config)")

	flag.Parse()

	if *showVersion {
		fmt.Printf("nsqadmin_to_hipchat v%s\n", version.Binary)
		return
	}

	if *channel == "" {
		log.Fatal("--channel is required")
	}

	if *topic == "" {
		log.Fatal("--topic is required")
	}
	if *slackChannel == "" {
		log.Fatal("--slack-channel is required")
	}
	if !strings.HasPrefix(*slackChannel, "#") {
		log.Fatalf("--slack-channel=%q must start with #", *slackChannel)
	}
	
	if *slackToken == "" {
		*slackToken  = os.Getenv("SLACK_TOKEN")
		os.Unsetenv("SLACK_TOKEN")
	}
	
	if *slackToken == "" {
		log.Fatal("--slack-token or environment variable SLACK_TOKEN required")
	}

	if len(nsqdTCPAddrs) == 0 && len(lookupdHTTPAddrs) == 0 {
		log.Fatal("--nsqd-tcp-address or --lookupd-http-address required")
	}
	if len(nsqdTCPAddrs) > 0 && len(lookupdHTTPAddrs) > 0 {
		log.Fatal("use --nsqd-tcp-address or --lookupd-http-address not both")
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	cfg.UserAgent = fmt.Sprintf("nsqadmin_to_hipchat/%s go-nsq/%s", version.Binary, nsq.VERSION)
	cfg.MaxInFlight = *maxInFlight

	consumer, err := nsq.NewConsumer(*topic, *channel, cfg)
	if err != nil {
		log.Fatal(err)
	}

	handler := &Handler{slack.New(*slackToken), *slackChannel}
	handler.Slack.Client = &http.Client{
		Timeout: time.Second * 10,
	}

	consumer.AddHandler(handler)

	err = consumer.ConnectToNSQDs(nsqdTCPAddrs)
	if err != nil {
		log.Fatal(err)
	}

	err = consumer.ConnectToNSQLookupds(lookupdHTTPAddrs)
	if err != nil {
		log.Fatal(err)
	}

	for {
		select {
		case <-consumer.StopChan:
			return
		case <-sigChan:
			consumer.Stop()
		}
	}
}
