package nsq

import (
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"time"

	"github.com/bitly/go-simplejson"
)

type deadlinedConn struct {
	Timeout time.Duration
	net.Conn
}

func (c *deadlinedConn) Read(b []byte) (n int, err error) {
	c.Conn.SetReadDeadline(time.Now().Add(c.Timeout))
	return c.Conn.Read(b)
}

func (c *deadlinedConn) Write(b []byte) (n int, err error) {
	c.Conn.SetWriteDeadline(time.Now().Add(c.Timeout))
	return c.Conn.Write(b)
}

func newDeadlineTransport(timeout time.Duration) *http.Transport {
	transport := &http.Transport{
		Dial: func(netw, addr string) (net.Conn, error) {
			c, err := net.DialTimeout(netw, addr, timeout)
			if err != nil {
				return nil, err
			}
			return &deadlinedConn{timeout, c}, nil
		},
	}
	return transport
}

func apiRequestNegotiateV1(method string, endpoint string, body io.Reader) (*simplejson.Json, error) {
	httpclient := &http.Client{Transport: newDeadlineTransport(2 * time.Second)}
	req, err := http.NewRequest(method, endpoint, body)
	if err != nil {
		return nil, err
	}

	req.Header.Add("Accept", "application/vnd.nsq; version=1.0")

	resp, err := httpclient.Do(req)
	if err != nil {
		return nil, err
	}

	respBody, err := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("got response %s %q", resp.Status, respBody)
	}

	if len(respBody) == 0 {
		respBody = []byte("{}")
	}

	data, err := simplejson.NewJson(respBody)
	if err != nil {
		return nil, err
	}

	if resp.Header.Get("X-NSQ-Content-Type") == "nsq; version=1.0" {
		return data, nil
	}
	return data.Get("data"), nil
}
