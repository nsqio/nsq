package main

import (
	"bytes"
	"fmt"
	"net/http"

	"github.com/nsqio/nsq/internal/version"
)

var httpclient *http.Client
var userAgent string

func init() {
	userAgent = fmt.Sprintf("nsq_to_http v%s", version.Binary)
}

func HTTPGet(endpoint string) (*http.Response, error) {
	req, err := http.NewRequest("GET", endpoint, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", userAgent)
	for key, val := range validCustomHeaders {
		req.Header.Set(key, val)
	}
	return httpclient.Do(req)
}

func HTTPPost(endpoint string, body *bytes.Buffer) (*http.Response, error) {
	req, err := http.NewRequest("POST", endpoint, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", userAgent)
	req.Header.Set("Content-Type", *contentType)
	for key, val := range validCustomHeaders {
		req.Header.Set(key, val)
	}
	return httpclient.Do(req)
}
