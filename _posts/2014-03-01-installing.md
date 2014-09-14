--- 
title: Installing
layout: post
category: deployment
permalink: /deployment/installing.html
---

### <a name="binary">Binary Releases</a>

Pre-built binaries (`nsqd`, `nsqlookupd`, `nsqadmin`, and all example apps) for linux and darwin are
available for download:

#### Current Stable Release: **`v0.2.31`**

**Go 1.3.1**:

 * [nsq-0.2.31.darwin-amd64.go1.3.1.tar.gz][0.2.31_darwin_go131]
 * [nsq-0.2.31.linux-amd64.go1.3.1.tar.gz][0.2.31_linux_go131]

**Go 1.2.2**:

 * [nsq-0.2.31.darwin-amd64.go1.2.2.tar.gz][0.2.31_darwin_go122]
 * [nsq-0.2.31.linux-amd64.go1.2.2.tar.gz][0.2.31_linux_go122]

#### Older Stable Releases

 * [nsq-0.2.30.darwin-amd64.go1.3.tar.gz][0.2.30_darwin_go13]
 * [nsq-0.2.30.linux-amd64.go1.3.tar.gz][0.2.30_linux_go13]
 * [nsq-0.2.28.darwin-amd64.go1.2.1.tar.gz][0.2.28_darwin_go121]
 * [nsq-0.2.28.linux-amd64.go1.2.1.tar.gz][0.2.28_linux_go121]
 * [nsq-0.2.27.darwin-amd64.go1.2.tar.gz][0.2.27_darwin_go12]
 * [nsq-0.2.27.linux-amd64.go1.2.tar.gz][0.2.27_linux_go12]

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

[0.2.31_darwin_go131]: https://s3.amazonaws.com/bitly-downloads/nsq/nsq-0.2.31.darwin-amd64.go1.3.1.tar.gz
[0.2.31_linux_go131]: https://s3.amazonaws.com/bitly-downloads/nsq/nsq-0.2.31.linux-amd64.go1.3.1.tar.gz
[0.2.31_darwin_go122]: https://s3.amazonaws.com/bitly-downloads/nsq/nsq-0.2.31.darwin-amd64.go1.2.2.tar.gz
[0.2.31_linux_go122]: https://s3.amazonaws.com/bitly-downloads/nsq/nsq-0.2.31.linux-amd64.go1.2.2.tar.gz

[0.2.30_darwin_go13]: https://s3.amazonaws.com/bitly-downloads/nsq/nsq-0.2.30.darwin-amd64.go1.3.tar.gz
[0.2.30_linux_go13]: https://s3.amazonaws.com/bitly-downloads/nsq/nsq-0.2.30.linux-amd64.go1.3.tar.gz

[0.2.28_darwin_go121]: https://s3.amazonaws.com/bitly-downloads/nsq/nsq-0.2.28.darwin-amd64.go1.2.1.tar.gz
[0.2.28_linux_go121]: https://s3.amazonaws.com/bitly-downloads/nsq/nsq-0.2.28.linux-amd64.go1.2.1.tar.gz

[0.2.27_darwin_go12]: https://s3.amazonaws.com/bitly-downloads/nsq/nsq-0.2.27.darwin-amd64.go1.2.tar.gz
[0.2.27_linux_go12]: https://s3.amazonaws.com/bitly-downloads/nsq/nsq-0.2.27.linux-amd64.go1.2.tar.gz

[docker]: https://docker.io/
[docker_docs]: {{ site.baseurl }}/deployment/docker.html
