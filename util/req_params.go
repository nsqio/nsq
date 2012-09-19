package util

import (
	"errors"
	"io/ioutil"
	"net/http"
	"net/url"
)

type ReqParams struct {
	params url.Values
	Body   []byte
}

func NewReqParams(req *http.Request) (*ReqParams, error) {
	reqParams, err := url.ParseQuery(req.URL.RawQuery)
	if err != nil {
		return nil, err
	}

	data, err := ioutil.ReadAll(req.Body)
	if err != nil {
		return nil, err
	}

	return &ReqParams{reqParams, data}, nil
}

func (r *ReqParams) Query(key string) (string, error) {
	keyData := r.params[key]
	if len(keyData) == 0 {
		return "", errors.New("key not in query params")
	}
	return keyData[0], nil
}
