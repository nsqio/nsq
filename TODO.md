
### Roadmap

 * v0.1 working nsqd/go-nsqreader (useable drop in replacement for simplequeue/go-queuereader)
 * v0.2 working python client (usable drop in replacement for simplequeue/BaseReader)
 * v0.3 nsqadmin - more complete administrative commands, UI for topology/stats
 * v0.4 working tls for communication channel - usable between datacenters

### Things for v0.3

 * upon topic creation, lookup channels against lookupd
 * cleanup (expire) topics/channels

### other items

 * heartbeats for TCP clients (and a timeout for idle clients not responding)
