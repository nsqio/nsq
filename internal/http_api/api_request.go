package http_api

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
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

// NegotiateV1 is a helper function to perform a v1 HTTP request
// and fallback to parsing the old backwards-compatible response format
// storing the result in the value pointed to by v.
func NegotiateV1(endpoint string, v interface{}) error {
	// TODO: deprecated, remove in 1.0 (replace calls with GETV1)
	httpclient := &http.Client{Transport: NewDeadlineTransport(2 * time.Second)}
	req, err := http.NewRequest("GET", endpoint, nil)
	if err != nil {
		return err
	}

	req.Header.Add("Accept", "application/vnd.nsq; version=1.0")

	resp, err := httpclient.Do(req)
	if err != nil {
		return err
	}

	respBody, err := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		return err
	}
	if resp.StatusCode != 200 {
		return fmt.Errorf("got response %s %q", resp.Status, respBody)
	}

	if len(respBody) == 0 {
		respBody = []byte("{}")
	}

	// unwrap pre-1.0 api response
	if resp.Header.Get("X-NSQ-Content-Type") != "nsq; version=1.0" {
		var u struct {
			StatusCode int64           `json:"status_code"`
			Data       json.RawMessage `json:"data"`
		}
		err := json.Unmarshal(respBody, u)
		if err != nil {
			return err
		}
		if u.StatusCode != 200 {
			return fmt.Errorf("got 200 response, but api status code of %d", u.StatusCode)
		}
		respBody = u.Data
	}

	return json.Unmarshal(respBody, v)
}

// GETV1 is a helper function to perform a V1 HTTP request
// and parse our NSQ daemon's expected response format, with deadlines.
func GETV1(endpoint string, v interface{}) error {
	httpclient := &http.Client{Transport: NewDeadlineTransport(2 * time.Second)}

retry:
	req, err := http.NewRequest("GET", endpoint, nil)
	if err != nil {
		return err
	}

	req.Header.Add("Accept", "application/vnd.nsq; version=1.0")

	resp, err := httpclient.Do(req)
	if err != nil {
		return err
	}

	body, err := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		return err
	}
	if resp.StatusCode != 200 {
		if resp.StatusCode == 403 && !strings.HasPrefix(endpoint, "https") {
			endpoint, err = httpsEndpoint(endpoint, body)
			if err != nil {
				return err
			}
			goto retry
		}
		return fmt.Errorf("got response %s %q", resp.Status, body)
	}
	err = json.Unmarshal(body, &v)
	if err != nil {
		return err
	}

	return nil
}

// PostV1 is a helper function to perform a V1 HTTP request
// and parse our NSQ daemon's expected response format, with deadlines.
func POSTV1(endpoint string) error {
	httpclient := &http.Client{Transport: NewDeadlineTransport(2 * time.Second)}

retry:
	req, err := http.NewRequest("POST", endpoint, nil)
	if err != nil {
		return err
	}

	req.Header.Add("Accept", "application/vnd.nsq; version=1.0")

	resp, err := httpclient.Do(req)
	if err != nil {
		return err
	}

	body, err := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		return err
	}
	if resp.StatusCode != 200 {
		if resp.StatusCode == 403 && !strings.HasPrefix(endpoint, "https") {
			endpoint, err = httpsEndpoint(endpoint, body)
			if err != nil {
				return err
			}
			goto retry
		}
		return fmt.Errorf("got response %s %q", resp.Status, body)
	}

	return nil
}

func httpsEndpoint(endpoint string, body []byte) (string, error) {
	var forbiddenResp struct {
		HTTPSPort int `json:"https_port"`
	}
	err := json.Unmarshal(body, &forbiddenResp)
	if err != nil {
		return "", err
	}

	u, err := url.Parse(endpoint)
	if err != nil {
		return "", err
	}

	host, _, err := net.SplitHostPort(u.Host)
	if err != nil {
		return "", err
	}

	u.Scheme = "https"
	u.Host = net.JoinHostPort(host, strconv.Itoa(forbiddenResp.HTTPSPort))
	return u.String(), nil
}
