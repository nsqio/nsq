# to_nsq

A tool for publishing to an nsq topic with data from `stdin`.

## Usage

```
Usage of ./to_nsq:
  -delimiter string
    	character to split input from stdin (default "\n")
  -nsqd-tcp-address value
    	destination nsqd TCP address (may be given multiple times)
  -producer-opt value
    	option to passthrough to nsq.Producer (may be given multiple times, http://godoc.org/github.com/nsqio/go-nsq#Config)
  -rate int
    	Throttle messages to n/second. 0 to disable
  -topic string
    	NSQ topic to publish to
```
    
### Examples

Publish each line of a file:

```bash
$ cat source.txt | to_nsq -topic="topic" -nsqd-tcp-address="127.0.0.1:4150"
```

Publish three messages, in one go:

```bash
$ echo "one,two,three" | to_nsq -delimiter="," -topic="topic" -nsqd-tcp-address="127.0.0.1:4150"
```