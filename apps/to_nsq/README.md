# to_nsq

A tool for publishing to an nsq topic data from `stdin`.

## Usage

Send each line of a file:

```
cat source.txt | to_nsq -scan="lines" -topic="topic" -nsqd-tcp-address="127.0.0.1:4150"
```

Manually enter lines to send:

```
to_nsq -scan="lines" -topic="topic" -nsqd-tcp-address="127.0.0.1:4150"
```
