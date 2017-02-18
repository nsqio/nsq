nsqadmin_to_slack sends event actions from nsqadmin to a slack channel. It's inspired by [nsqadmin2hipchat](https://github.com/danielhfrank/nsqadmin2hipchat) by [@danielhfrank](https://github.com/danielhfrank).

This was adapted from a more generic internal nofication system in use at Bitly.

### nsqadmin configuration

nsqadmin supports creating a datastream of events from admin actions. These events cover actions like pause/unpause/empty/remove for topics and channels. It's typically used to send these events to a nsqd topic using the nsqd http PUB api.

> nsqadmin --notification-http-endpoint="http://127.0.0.1:4151/pub?topic=nsqadmin_events"

If HTTP requests to nqsadmin have a Basic authorization header (often from running behind oauth2_proxy(https://github.com/bitly/oauth2_proxy)) that will be recorded in the event as the User initiating action. If the header X-Forwarded-Email is set, that will be used to match to the right slack user.

The structure of these events is defined [here](https://github.com/nsqio/nsq/blob/master/nsqadmin/notify.go#L12-L23).

### nsqadmin_to_slack configuration

nsqadmin_to_slack supports reading nsqadmin events from a nsqd topic, and sending properly formatted events to slack.

```
Usage of ./nsqadmin_to_slack:
  -channel string
    	NSQ channel (default "nsqadmin_to_hipchat")
  -consumer-opt value
    	option to passthrough to nsq.Consumer (may be given multiple times, http://godoc.org/github.com/nsqio/go-nsq#Config)
  -lookupd-http-address value
    	lookupd HTTP address (may be given multiple times)
  -max-in-flight int
    	max number of messages to allow in flight (default 200)
  -nsqd-tcp-address value
    	nsqd TCP address (may be given multiple times)
  -slack-channel string
    	Slack channel. i.e. #test
  -slack-token string
    	Slack API Token (may alternately be specified via SLACK_TOKEN environment variable)
  -topic string
    	NSQ topic
  -version
    	print version string
```