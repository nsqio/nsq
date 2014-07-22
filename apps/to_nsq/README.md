# to_nsq

A tool for publishing to an nsq topic data from `stdin`.

## Usage

Publish each line of a file:

```
cat source.txt | to_nsq -topic="topic" -nsqd-tcp-address="127.0.0.1:4150"
```

Publish manually entered lines in a shell:

```
to_nsq -topic="topic" -nsqd-tcp-address="127.0.0.1:4150"
one
two
three
(Ctrl+C to stop)
```

Publish comma separated values from a source file:

```
cat source.txt | to_nsq -delimiter="," -topic="topic" -nsqd-tcp-address="127.0.0.1:4150"
```

Publish three messages, in one go:

```
echo "one,two,three" | to_nsq -delimiter="," -topic="topic" -nsqd-tcp-address="127.0.0.1:4150"
```