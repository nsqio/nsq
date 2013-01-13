package nsq

import (
	"errors"
	"fmt"
	"github.com/bitly/go-simplejson"
	"io/ioutil"
	"net"
	"net/http"
	"time"
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

// A custom http.Transport with support for deadline timeouts
func NewDeadlineTransport(timeout time.Duration) *http.Transport {
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

// ApiRequest is a helper function to perform an HTTP request
// and parse our NSQ daemon's expected response format, with deadlines.
//
//     {"status_code":200, "status_txt":"OK", "data":{...}}
func ApiRequest(endpoint string) (*simplejson.Json, error) {
	httpclient := &http.Client{Transport: NewDeadlineTransport(2 * time.Second)}
	req, err := http.NewRequest("GET", endpoint, nil)
	if err != nil {
		return nil, err
	}

	resp, err := httpclient.Do(req)
	if err != nil {
		return nil, err
	}

	body, err := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		return nil, err
	}

	data, err := simplejson.NewJson(body)
	if err != nil {
		return nil, err
	}

	statusCode := data.Get("status_code").MustInt()
	statusTxt := data.Get("status_txt").MustString()
	if statusCode != 200 {
		return nil, errors.New(fmt.Sprintf("response status_code = %d, status_txt = %s", 
			statusCode, statusTxt))
	}
	return data.Get("data"), nil
}
