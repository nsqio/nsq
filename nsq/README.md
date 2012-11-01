## nsq

`nsq` is the official Go package for [NSQ][nsq].

It provides the building blocks for developing applications on the [NSQ][nsq] platform in Go.

Low-level functions and types are provided to communicate over the [NSQ protocol][protocol] as well
as a high-level [Reader][reader] library to implement consumers.

See the [examples][examples] directory for utilities built using this package that provide support
for common tasks.

### Installing

    $ go get github.com/bitly/nsq/nsq

### Importing

```go
import "github.com/bitly/nsq/nsq"
```

### Docs

See [gopkgdoc][nsq_gopkgdoc] for pretty documentation or:

    # in the nsq package directory
    $ go doc

[nsq]: https://github.com/bitly/nsq
[nsq_gopkgdoc]: http://go.pkgdoc.org/github.com/bitly/nsq/nsq
[protocol]: https://github.com/bitly/nsq/blob/master/docs/protocol.md
[examples]: https://github.com/bitly/nsq/tree/master/examples
[reader]: http://go.pkgdoc.org/github.com/bitly/nsq/nsq#Reader
