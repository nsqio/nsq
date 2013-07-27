### Overview

**NSQ** is a realtime message processing system designed to operate at bitly's scale, handling
billions of messages per day.

It promotes *distributed* and *decentralized* topologies [without single points of failure][spof],
enabling fault tolerance and high availability coupled with a reliable [message delivery
guarantee][message_guarantee].

Operationally, **NSQ** is easy to configure and deploy (all parameters are specified on the command
line and compiled binaries have no runtime dependencies). For maximum flexibility, it is agnostic to
data format (messages can be JSON, MsgPack, go-protobuf, or anything else). Official Go and Python
libraries are available out of the box and, if you're interested in building your own client,
there's a [protocol spec][protocol] (see [client libraries][client_libraries]).

### Getting Help

If you're having trouble or have questions about NSQ, the best place to ask is the [user group][google_group].

### Issues

All issues should be reported via [github issues][github_issues]. Don't forget to search through the
existing issues to see if that topic has come up before posting.

### Contributing

**NSQ** has growing community and contributions are always welcome (particularly documentation). To
contribute, fork the project on [github][github_nsq] and send a pull request.

[spof]: #
[message_guarantee]: #
[protocol]: #
[client_libraries]: #
[github_issues]: https://github.com/bitly/nsq/issues
[github_nsq]: http://github.com/bitly/nsq
[google_group]: http://groups.google.com/group/nsq-users
