package statsd

import (
	"fmt"
	"io"
)

type Client struct {
	w      io.Writer
	prefix string
}

func NewClient(w io.Writer, prefix string) *Client {
	return &Client{
		w:      w,
		prefix: prefix,
	}
}

func (c *Client) Incr(stat string, count int64) error {
	return c.send(stat, "%d|c", count)
}

func (c *Client) Decr(stat string, count int64) error {
	return c.send(stat, "%d|c", -count)
}

func (c *Client) Timing(stat string, delta int64) error {
	return c.send(stat, "%d|ms", delta)
}

func (c *Client) Gauge(stat string, value int64) error {
	return c.send(stat, "%d|g", value)
}

func (c *Client) send(stat string, format string, value int64) error {
	format = fmt.Sprintf("%s%s:%s\n", c.prefix, stat, format)
	_, err := fmt.Fprintf(c.w, format, value)
	return err
}
