package statsd

import (
	"errors"
	"fmt"
	"net"
	"time"
)

type Client struct {
	conn   net.Conn
	addr   string
	prefix string
}

func NewClient(addr string, prefix string) *Client {
	return &Client{
		addr:   addr,
		prefix: prefix,
	}
}

func (c *Client) String() string {
	return c.addr
}

func (c *Client) CreateSocket() error {
	conn, err := net.DialTimeout("udp", c.addr, time.Second)
	if err != nil {
		return err
	}
	c.conn = conn
	return nil
}

func (c *Client) Close() error {
	return c.conn.Close()
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
	if c.conn == nil {
		return errors.New("not connected")
	}
	format = fmt.Sprintf("%s%s:%s", c.prefix, stat, format)
	_, err := fmt.Fprintf(c.conn, format, value)
	return err
}
