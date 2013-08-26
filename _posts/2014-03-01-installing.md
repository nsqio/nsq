--- 
title: Installing
layout: post
category: deployment
permalink: /deployment/installing.html
---

### <a name="binary">Binary Releases</a>

Pre-built binaries (`nsqd`, `nsqlookupd`, `nsqadmin`, and all example apps) for linux and darwin are
available for download:

Built w/ Go 1.0.3:

 * [nsq-0.2.22.darwin-amd64.go1.0.3.tar.gz][0.2.22_darwin]
 * [nsq-0.2.22.linux-amd64.go1.0.3.tar.gz][0.2.22_linux]
 * [nsq-0.2.21.darwin-amd64.go1.0.3.tar.gz][0.2.21_darwin]
 * [nsq-0.2.21.linux-amd64.go1.0.3.tar.gz][0.2.21_linux]
 * [nsq-0.2.20.darwin-amd64.tar.gz][0.2.20_darwin]
 * [nsq-0.2.20.linux-amd64.tar.gz][0.2.20_linux]
 * [nsq-0.2.19.darwin-amd64.tar.gz][0.2.19_darwin]
 * [nsq-0.2.19.linux-amd64.tar.gz][0.2.19_linux]
 * [nsq-0.2.18.darwin-amd64.tar.gz][0.2.18_darwin]
 * [nsq-0.2.18.linux-amd64.tar.gz][0.2.18_linux]
 * [nsq-0.2.17.darwin-amd64.tar.gz][0.2.17_darwin]
 * [nsq-0.2.17.linux-amd64.tar.gz][0.2.17_linux]
 * [nsq-0.2.16.darwin-amd64.tar.gz][0.2.16_darwin]
 * [nsq-0.2.16.linux-amd64.tar.gz][0.2.16_linux]
 * [nsq-0.2.15.darwin-amd64.tar.gz][0.2.15_darwin]
 * [nsq-0.2.15.linux-amd64.tar.gz][0.2.15_linux]

Built w/ Go 1.1.x:

 * [nsq-0.2.22.darwin-amd64.go1.1.2.tar.gz][0.2.22_darwin_go112]
 * [nsq-0.2.22.linux-amd64.go1.1.2.tar.gz][0.2.22_linux_go112]
 * [nsq-0.2.21.darwin-amd64.go1.1.1.tar.gz][0.2.21_darwin_go111]
 * [nsq-0.2.21.linux-amd64.go1.1.1.tar.gz][0.2.21_linux_go111]

### Building From Source

#### Pre-requisites

 * **[golang](http://golang.org/doc/install)** (version **`1.0.3+`** is required)

 * **[go-hostpool](https://github.com/bitly/go-hostpool)**

 * **[go-simplejson](https://github.com/bitly/go-simplejson)**

 * **[assert](https://github.com/bmizerany/assert)** (required for running tests)

NOTE: running `go get` as described in the _Compiling_ section will automatically download and
install the pre-requisites.

#### Compiling

Use `go get` do download and compile the packages and binaries:

{% highlight bash %}
$ go get github.com/bitly/nsq/...
{% endhighlight %}

#### Testing

{% highlight bash %}
$ ./test.sh
{% endhighlight %}

[0.2.22_darwin]: https://s3.amazonaws.com/bitly-downloads/nsq/nsq-0.2.22.darwin-amd64.go1.0.3.tar.gz
[0.2.22_linux]: https://s3.amazonaws.com/bitly-downloads/nsq/nsq-0.2.22.linux-amd64.go1.0.3.tar.gz
[0.2.22_darwin_go112]: https://s3.amazonaws.com/bitly-downloads/nsq/nsq-0.2.22.darwin-amd64.go1.1.2.tar.gz
[0.2.22_linux_go112]: https://s3.amazonaws.com/bitly-downloads/nsq/nsq-0.2.22.linux-amd64.go1.1.2.tar.gz
[0.2.21_darwin_go111]: https://s3.amazonaws.com/bitly-downloads/nsq/nsq-0.2.21.darwin-amd64.go1.1.1.tar.gz
[0.2.21_linux_go111]: https://s3.amazonaws.com/bitly-downloads/nsq/nsq-0.2.21.linux-amd64.go1.1.1.tar.gz
[0.2.21_darwin]: https://s3.amazonaws.com/bitly-downloads/nsq/nsq-0.2.21.darwin-amd64.go1.0.3.tar.gz
[0.2.21_linux]: https://s3.amazonaws.com/bitly-downloads/nsq/nsq-0.2.21.linux-amd64.go1.0.3.tar.gz
[0.2.20_darwin]: https://s3.amazonaws.com/bitly-downloads/nsq/nsq-0.2.20.darwin-amd64.tar.gz
[0.2.20_linux]: https://s3.amazonaws.com/bitly-downloads/nsq/nsq-0.2.20.linux-amd64.tar.gz
[0.2.19_darwin]: https://s3.amazonaws.com/bitly-downloads/nsq/nsq-0.2.19.darwin-amd64.tar.gz
[0.2.19_linux]: https://s3.amazonaws.com/bitly-downloads/nsq/nsq-0.2.19.linux-amd64.tar.gz
[0.2.18_darwin]: https://s3.amazonaws.com/bitly-downloads/nsq/nsq-0.2.18.darwin-amd64.tar.gz
[0.2.18_linux]: https://s3.amazonaws.com/bitly-downloads/nsq/nsq-0.2.18.linux-amd64.tar.gz
[0.2.17_darwin]: https://s3.amazonaws.com/bitly-downloads/nsq/nsq-0.2.17.darwin-amd64.tar.gz
[0.2.17_linux]: https://s3.amazonaws.com/bitly-downloads/nsq/nsq-0.2.17.linux-amd64.tar.gz
[0.2.16_darwin]: https://s3.amazonaws.com/bitly-downloads/nsq/nsq-0.2.16.darwin-amd64.tar.gz
[0.2.16_linux]: https://s3.amazonaws.com/bitly-downloads/nsq/nsq-0.2.16.linux-amd64.tar.gz
[0.2.15_darwin]: https://s3.amazonaws.com/bitly-downloads/nsq/nsq-0.2.15.darwin-amd64.tar.gz
[0.2.15_linux]: https://s3.amazonaws.com/bitly-downloads/nsq/nsq-0.2.15.linux-amd64.tar.gz
