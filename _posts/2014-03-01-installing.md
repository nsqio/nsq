--- 
title: Installing
layout: post
category: deployment
permalink: /deployment/installing.html
---

### <a name="binary">Binary Releases</a>

Pre-built binaries (`nsqd`, `nsqlookupd`, `nsqadmin`, and all example apps) for linux and darwin are
available for download:

#### Current Stable Release: **`v0.2.30`**

**Go 1.3**:

 * [nsq-0.2.30.darwin-amd64.go1.3.tar.gz][0.2.30_darwin_go13]
 * [nsq-0.2.30.linux-amd64.go1.3.tar.gz][0.2.30_linux_go13]

**Go 1.2.2**:

 * [nsq-0.2.30.darwin-amd64.go1.2.2.tar.gz][0.2.30_darwin_go122]
 * [nsq-0.2.30.linux-amd64.go1.2.2.tar.gz][0.2.30_linux_go122]

**Go 1.0.3**:

 * [nsq-0.2.30.darwin-amd64.go1.0.3.tar.gz][0.2.30_darwin_go103]
 * [nsq-0.2.30.linux-amd64.go1.0.3.tar.gz][0.2.30_linux_go103]

#### Older Stable Releases

**Go 1.2.x**:

 * [nsq-0.2.28.darwin-amd64.go1.2.1.tar.gz][0.2.28_darwin_go121]
 * [nsq-0.2.28.linux-amd64.go1.2.1.tar.gz][0.2.28_linux_go121]
 * [nsq-0.2.27.darwin-amd64.go1.2.tar.gz][0.2.27_darwin_go12]
 * [nsq-0.2.27.linux-amd64.go1.2.tar.gz][0.2.27_linux_go12]

**Go 1.0.3**:

 * [nsq-0.2.28.darwin-amd64.go1.0.3.tar.gz][0.2.28_darwin]
 * [nsq-0.2.28.linux-amd64.go1.0.3.tar.gz][0.2.28_linux]
 * [nsq-0.2.27.darwin-amd64.go1.0.3.tar.gz][0.2.27_darwin]
 * [nsq-0.2.27.linux-amd64.go1.0.3.tar.gz][0.2.27_linux]

### OSX

     $ brew install nsq

### Building From Source

#### Pre-requisites

 * **[golang](http://golang.org/doc/install)** (version **`1.0.3+`** is required)
 * **[godep](https://github.com/kr/godep)** (dependency manager)

#### Compiling

NSQ uses **[godep](https://github.com/kr/godep)** to manage dependencies and produce reliable
builds.  **Using `godep` is the preferred method when compiling from source.**

{% highlight bash %}
$ godep get github.com/bitly/nsq/...
{% endhighlight %}

NSQ remains `go get` compatible but it is not recommended as it is not guaranteed to
produce reliable builds (pinned dependencies need to be satisfied manually).

{% highlight bash %}
$ go get github.com/bitly/nsq/...
{% endhighlight %}

#### Testing

{% highlight bash %}
$ ./test.sh
{% endhighlight %}

### Docker Containers

We've published official [docker][docker] containers for [`nsqd`][docker_nsqd] and
[`nsqlookupd`][docker_nsqlookupd].

[0.2.30_darwin_go13]: https://s3.amazonaws.com/bitly-downloads/nsq/nsq-0.2.30.darwin-amd64.go1.3.tar.gz
[0.2.30_linux_go13]: https://s3.amazonaws.com/bitly-downloads/nsq/nsq-0.2.30.linux-amd64.go1.3.tar.gz
[0.2.30_darwin_go122]: https://s3.amazonaws.com/bitly-downloads/nsq/nsq-0.2.30.darwin-amd64.go1.2.2.tar.gz
[0.2.30_linux_go122]: https://s3.amazonaws.com/bitly-downloads/nsq/nsq-0.2.30.linux-amd64.go1.2.2.tar.gz
[0.2.30_darwin_go103]: https://s3.amazonaws.com/bitly-downloads/nsq/nsq-0.2.30.darwin-amd64.go1.0.3.tar.gz
[0.2.30_linux_go103]: https://s3.amazonaws.com/bitly-downloads/nsq/nsq-0.2.30.linux-amd64.go1.0.3.tar.gz

[0.2.28_darwin_go121]: https://s3.amazonaws.com/bitly-downloads/nsq/nsq-0.2.28.darwin-amd64.go1.2.1.tar.gz
[0.2.28_linux_go121]: https://s3.amazonaws.com/bitly-downloads/nsq/nsq-0.2.28.linux-amd64.go1.2.1.tar.gz
[0.2.28_darwin]: https://s3.amazonaws.com/bitly-downloads/nsq/nsq-0.2.28.darwin-amd64.go1.0.3.tar.gz
[0.2.28_linux]: https://s3.amazonaws.com/bitly-downloads/nsq/nsq-0.2.28.linux-amd64.go1.0.3.tar.gz

[0.2.27_darwin_go12]: https://s3.amazonaws.com/bitly-downloads/nsq/nsq-0.2.27.darwin-amd64.go1.2.tar.gz
[0.2.27_linux_go12]: https://s3.amazonaws.com/bitly-downloads/nsq/nsq-0.2.27.linux-amd64.go1.2.tar.gz
[0.2.27_darwin]: https://s3.amazonaws.com/bitly-downloads/nsq/nsq-0.2.27.darwin-amd64.go1.0.3.tar.gz
[0.2.27_linux]: https://s3.amazonaws.com/bitly-downloads/nsq/nsq-0.2.27.linux-amd64.go1.0.3.tar.gz

[docker]: https://docker.io/
[docker_nsqd]: https://index.docker.io/u/mreiferson/nsqd/
[docker_nsqlookupd]: https://index.docker.io/u/mreiferson/nsqlookupd/
