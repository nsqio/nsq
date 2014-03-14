--- 
title: utilities
layout: post
category: components
permalink: /components/utilities.html
---

These are utilities that facilitate common configurations, and provide introspection into data streams.

### nsq_stat

Polls `/stats` for all the producers of the specified topic/channel and displays aggregate stats

    ---------------depth---------------+--------------metadata---------------
      total    mem    disk inflt   def |     req     t-o         msgs clients
      24660  24660       0     0    20 |  102688       0    132492418       1
      25001  25001       0     0    20 |  102688       0    132493086       1
      21132  21132       0     0    21 |  102688       0    132493729       1


#### Command Line Options

    -channel="": NSQ channel
    -lookupd-http-address=: lookupd HTTP address (may be given multiple times)
    -nsqd-http-address=: nsqd HTTP address (may be given multiple times)
    -status-every=2s: duration of time between polling/printing output
    -topic="": NSQ topic
    -version=false: print version


### nsq_tail

Reads the specified topic/channel and writes to stdout

#### Command Line Options

    -channel="": nsq channel
    -lookupd-http-address=: lookupd HTTP address (may be given multiple times)
    -max-in-flight=200: max number of messages to allow in flight
    -n=0: total messages to show (will wait if starved)
    -nsqd-tcp-address=: nsqd TCP address (may be given multiple times)
    -reader-opt=: option to passthrough to nsq.Reader (may be given multiple times)
    -topic="": nsq topic
    -version=false: print version string

### nsq\_to_file

Reads the specified topic/channel and writes out to a newline deliminated file, and optionally rolls the file.

#### Command Line Options

    -channel="nsq_to_file": nsq channel
    -datetime-format="%Y-%m-%d_%H": strftime compatible format for <DATETIME> in filename format
    -filename-format="<TOPIC>.<HOST><GZIPREV>.<DATETIME>.log": output filename format (<TOPIC>, <HOST>, <DATETIME>, <GZIPREV> are replaced. <GZIPREV> is a suffix when an existing gzip file already exists)
    -gzip=false: gzip output files.
    -gzip-compression=3: (deprecated) use --gzip-level, gzip compression level (1 = BestSpeed, 2 = BestCompression, 3 = DefaultCompression)
    -gzip-level=6: gzip compression level (1-9, 1=BestSpeed, 9=BestCompression)
    -host-identifier="": value to output in log filename in place of hostname. <SHORT_HOST> and <HOSTNAME> are valid replacement tokens
    -lookupd-http-address=: lookupd HTTP address (may be given multiple times)
    -max-in-flight=200: max number of messages to allow in flight
    -nsqd-tcp-address=: nsqd TCP address (may be given multiple times)
    -output-dir="/tmp": directory to write output files to
    -reader-opt=: option to passthrough to nsq.Reader (may be given multiple times)
    -skip-empty-files=false: Skip writting empty files
    -topic="": nsq topic
    -verbose=false: (depgrecated) use --reader-opt=verbose
    -version=false: print version string


### nsq\_to_http

Reads the specified topic/channel and performs HTTP requests (GET/POST) to the specified endpoints

#### Command Line Options

    -channel="nsq_to_http": nsq channel
    -content-type="application/octet-stream": the Content-Type used for POST requests
    -get=: HTTP address to make a GET request to. '%s' will be printf replaced with data (may be given multiple times)
    -http-timeout=20s: timeout for HTTP connect/read/write (each)
    -http-timeout-ms=20000: (deprecated) use --http-timeout=X, timeout for HTTP connect/read/write (each)
    -lookupd-http-address=: lookupd HTTP address (may be given multiple times)
    -max-backoff-duration=2m0s: (deprecated) use --reader-opt=max_backoff_duration=X, the maximum backoff duration
    -max-in-flight=200: max number of messages to allow in flight
    -mode="round-robin": the upstream request mode options: multicast, round-robin, hostpool
    -n=100: number of concurrent publishers
    -nsqd-tcp-address=: nsqd TCP address (may be given multiple times)
    -post=: HTTP address to make a POST request to.  data will be in the body (may be given multiple times)
    -reader-opt=: option to passthrough to nsq.Reader (may be given multiple times)
    -round-robin=false: (deprecated) use --mode=round-robin, enable round robin mode
    -sample=1: % of messages to publish (float b/w 0 -> 1)
    -status-every=250: the # of requests between logging status (per handler), 0 disables
    -throttle-fraction=1: (deprecated) use --sample=X, publish only a fraction of messages
    -topic="": nsq topic
    -verbose=false: (depgrecated) use --reader-opt=verbose
    -version=false: print version string


### nsq\_to_nsq

Reads the specified topic/channel and re-publishes the messages to destination nsqd via TCP

#### Command Line Options

    -channel="nsq_to_nsq": nsq channel
    -destination-nsqd-tcp-address=: destination nsqd TCP address (may be given multiple times)
    -destination-topic="": destination nsq topic
    -lookupd-http-address=: lookupd HTTP address (may be given multiple times)
    -max-backoff-duration=2m0s: (deprecated) use --reader-opt=max_backoff_duration=X, the maximum backoff duration
    -max-in-flight=200: max number of messages to allow in flight
    -mode="round-robin": the upstream request mode options: round-robin (default), hostpool
    -nsqd-tcp-address=: nsqd TCP address (may be given multiple times)
    -reader-opt=: option to passthrough to nsq.Reader (may be given multiple times)
    -require-json-field="": for JSON messages: only pass messages that contain this field
    -require-json-value="": for JSON messages: only pass messages in which the required field has this value
    -status-every=250: the # of requests between logging status (per destination), 0 disables
    -topic="": nsq topic
    -verbose=false: (depgrecated) use --reader-opt=verbose
    -version=false: print version string
    -whitelist-json-field=: for JSON messages: pass this field (may be given multiple times)
