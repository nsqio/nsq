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
	net.Conn
}

func (c *deadlinedConn) Read(b []byte) (n int, err error) {
	c.Conn.SetReadDeadline(time.Now().Add(time.Second * 2))
	return c.Conn.Read(b)
}

func (c *deadlinedConn) Write(b []byte) (n int, err error) {
	c.Conn.SetWriteDeadline(time.Now().Add(time.Second * 2))
	return c.Conn.Write(b)
}

// ApiRequest is a helper function to perform an HTTP request
// and parse our NSQ daemon's expected response format, with deadlines.
//
//     {"status_code":200, "status_txt":"OK", "data":{...}}
func ApiRequest(endpoint string) (*simplejson.Json, error) {
	transport := &http.Transport{
		Dial: func(netw, addr string) (net.Conn, error) {
			c, err := net.DialTimeout(netw, addr, time.Second*2)
			if err != nil {
				return nil, err
			}
			return &deadlinedConn{c}, nil
		},
	}
	// use custom transport for deadline timeouts
	httpclient := &http.Client{Transport: transport}
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

	statusCode, err := data.Get("status_code").Int()
	if err != nil {
		return nil, err
	}
	if statusCode != 200 {
		return nil, errors.New(fmt.Sprintf("response status_code == %d", statusCode))
	}
	return data.Get("data"), nil
}
