
### Roadmap

 * v0.1 working nsqd/go-nsqreader (useable drop in replacement for simplequeue/go-queuereader) #2001
 * v0.2 working python client (usable drop in replacement for simplequeue/BaseReader)
 * v0.3 working nsqlookupd (and updated clients to use it) - useable across services (replacing pubsub)
 * v0.4 working tls for communication channel - usable between datacenters
 * v0.5 working nsqadmin for seeing overview of nsq topology
 * v0.6 nsqadmin displaying performance/message throughput/stats

### Things for v0.3

 * upon topic creation, lookup channels against lookupd
 * graceful restart
 * cleanup (expire) topics/channels
