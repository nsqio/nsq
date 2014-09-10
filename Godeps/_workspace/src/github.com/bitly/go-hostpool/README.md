go-hostpool
===========

Intelligently and flexibly pool among multiple hosts from your Go application.
Usage example:

```go
hp := hostpool.NewEpsilonGreedy([]string{"a", "b"}, 0, &hostpool.LinearEpsilonValueCalculator{})
hostResponse := hp.Get()
hostname := hostResponse.Host()
err := _ // (make a request with hostname)
hostResponse.Mark(err)
```

View more detailed documentation on godoc.org
