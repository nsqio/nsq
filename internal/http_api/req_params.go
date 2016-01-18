package http_api

import (
	"errors"
	"io/ioutil"
	"net/http"
	"net/url"
)

type ReqParams struct {
	url.Values
	Body []byte
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

func (r *ReqParams) Get(key string) (string, error) {
	v, ok := r.Values[key]
	if !ok {
		return "", errors.New("key not in query params")
	}
	return v[0], nil
}

func (r *ReqParams) GetAll(key string) ([]string, error) {
	v, ok := r.Values[key]
	if !ok {
		return nil, errors.New("key not in query params")
	}
	return v, nil
}

type PostParams struct {
	*http.Request
}

func (p *PostParams) Get(key string) (string, error) {
	if p.Request.Form == nil {
		p.Request.ParseMultipartForm(1 << 20)
	}
	if vs, ok := p.Request.Form[key]; ok {
		return vs[0], nil
	}
	return "", errors.New("key not in post params")
}
