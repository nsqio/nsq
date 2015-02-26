package util

import (
	"errors"
	"fmt"
	"net"
	"time"
)

type StatsdClient struct {
	conn   net.Conn
	addr   string
	prefix string
}

func NewStatsdClient(addr string, prefix string) *StatsdClient {
	return &StatsdClient{
		addr:   addr,
		prefix: prefix,
	}
}

func (c *StatsdClient) String() string {
	return c.addr
}

func (c *StatsdClient) CreateSocket() error {
	conn, err := net.DialTimeout("udp", c.addr, time.Second)
	if err != nil {
		return err
	}
	c.conn = conn
	return nil
}

func (c *StatsdClient) Close() error {
	return c.conn.Close()
}

func (c *StatsdClient) Incr(stat string, count int64) error {
	return c.send(stat, "%d|c", count)
}

func (c *StatsdClient) Decr(stat string, count int64) error {
	return c.send(stat, "%d|c", -count)
}

func (c *StatsdClient) Timing(stat string, delta int64) error {
	return c.send(stat, "%d|ms", delta)
}

func (c *StatsdClient) Gauge(stat string, value int64) error {
	return c.send(stat, "%d|g", value)
}

func (c *StatsdClient) send(stat string, format string, value int64) error {
	if c.conn == nil {
		return errors.New("not connected")
	}
	format = fmt.Sprintf("%s%s:%s", c.prefix, stat, format)
	_, err := fmt.Fprintf(c.conn, format, value)
	return err
}
