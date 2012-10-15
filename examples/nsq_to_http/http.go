package main

import (
	"io"
	"net"
	"net/http"
	"time"
)

var transport *http.Transport
var httpclient *http.Client

func init() {
	// use custom transport for deadlines
	transport = &http.Transport{
		Dial: func(netw, addr string) (net.Conn, error) {
			c, err := net.DialTimeout(netw, addr, time.Duration(*httpTimeoutMs)*time.Millisecond)
			if err != nil {
				return nil, err
			}
			return &deadlinedConn{c}, nil
		},
	}
	httpclient = &http.Client{Transport: transport}
}

type deadlinedConn struct {
	net.Conn
}

func (c *deadlinedConn) Read(b []byte) (n int, err error) {
	c.Conn.SetReadDeadline(time.Now().Add(time.Duration(*httpTimeoutMs) * time.Millisecond))
	return c.Conn.Read(b)
}

func (c *deadlinedConn) Write(b []byte) (n int, err error) {
	c.Conn.SetWriteDeadline(time.Now().Add(time.Duration(*httpTimeoutMs) * time.Millisecond))
	return c.Conn.Write(b)
}

func HttpGet(endpoint string) (*http.Response, error) {
	req, err := http.NewRequest("GET", endpoint, nil)
	if err != nil {
		return nil, err
	}
	return httpclient.Do(req)
}

func HttpPost(endpoint string, body io.Reader) (*http.Response, error) {
	req, err := http.NewRequest("POST", endpoint, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	return httpclient.Do(req)
}
