--- 
title: Installing
layout: post
category: deployment
permalink: /deployment/installing.html
---

### <a name="binary">Binary Releases</a>

Pre-built binaries (`nsqd`, `nsqlookupd`, `nsqadmin`, and all example apps) for linux and darwin are
available for download:

#### Current Stable Release: **`v0.3.2`**

 * [nsq-0.3.2.darwin-amd64.go1.4.1.tar.gz][0.3.2_darwin_go141]
 * [nsq-0.3.2.linux-amd64.go1.4.1.tar.gz][0.3.2_linux_go141]

#### Older Stable Releases

 * [nsq-0.3.1.darwin-amd64.go1.4.1.tar.gz][0.3.1_darwin_go141]
 * [nsq-0.3.1.linux-amd64.go1.4.1.tar.gz][0.3.1_linux_go141]
 * [nsq-0.3.0.darwin-amd64.go1.3.3.tar.gz][0.3.0_darwin_go133]
 * [nsq-0.3.0.linux-amd64.go1.3.3.tar.gz][0.3.0_linux_go133]
 * [nsq-0.2.31.darwin-amd64.go1.3.1.tar.gz][0.2.31_darwin_go131]
 * [nsq-0.2.31.linux-amd64.go1.3.1.tar.gz][0.2.31_linux_go131]
 * [nsq-0.2.30.darwin-amd64.go1.3.tar.gz][0.2.30_darwin_go13]
 * [nsq-0.2.30.linux-amd64.go1.3.tar.gz][0.2.30_linux_go13]

### Docker

See [the docs][docker_docs] for deploying NSQ with [Docker][docker].

### OSX

     $ brew install nsq

### Building From Source

#### Pre-requisites

 * **[golang](http://golang.org/doc/install)** (version **`1.2+`** is required)
 * **[gpm](https://github.com/pote/gpm)** (dependency manager)

#### Compiling

NSQ uses **[gpm](https://github.com/pote/gpm)** to manage dependencies and produce reliable
builds.  **Using `gpm` is the preferred method when compiling from source.**

{% highlight bash %}
$ gpm install
$ go get github.com/bitly/nsq/...
{% endhighlight %}

NSQ remains `go get` compatible but it is not recommended as it is not guaranteed to
produce reliable builds (pinned dependencies need to be satisfied manually).

#### Testing

{% highlight bash %}
$ ./test.sh
{% endhighlight %}

[0.3.2_darwin_go141]: https://s3.amazonaws.com/bitly-downloads/nsq/nsq-0.3.2.darwin-amd64.go1.4.1.tar.gz
[0.3.2_linux_go141]: https://s3.amazonaws.com/bitly-downloads/nsq/nsq-0.3.2.linux-amd64.go1.4.1.tar.gz

[0.3.1_darwin_go141]: https://s3.amazonaws.com/bitly-downloads/nsq/nsq-0.3.1.darwin-amd64.go1.4.1.tar.gz
[0.3.1_linux_go141]: https://s3.amazonaws.com/bitly-downloads/nsq/nsq-0.3.1.linux-amd64.go1.4.1.tar.gz

[0.3.0_darwin_go133]: https://s3.amazonaws.com/bitly-downloads/nsq/nsq-0.3.0.darwin-amd64.go1.3.3.tar.gz
[0.3.0_linux_go133]: https://s3.amazonaws.com/bitly-downloads/nsq/nsq-0.3.0.linux-amd64.go1.3.3.tar.gz

[0.2.31_darwin_go131]: https://s3.amazonaws.com/bitly-downloads/nsq/nsq-0.2.31.darwin-amd64.go1.3.1.tar.gz
[0.2.31_linux_go131]: https://s3.amazonaws.com/bitly-downloads/nsq/nsq-0.2.31.linux-amd64.go1.3.1.tar.gz

[0.2.30_darwin_go13]: https://s3.amazonaws.com/bitly-downloads/nsq/nsq-0.2.30.darwin-amd64.go1.3.tar.gz
[0.2.30_linux_go13]: https://s3.amazonaws.com/bitly-downloads/nsq/nsq-0.2.30.linux-amd64.go1.3.tar.gz

[docker]: https://docker.io/
[docker_docs]: {{ site.baseurl }}/deployment/docker.html
