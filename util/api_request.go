package util

import (
	"bitly/simplejson"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
)

func ApiRequest(endpoint string) (*simplejson.Json, error) {
	httpclient := &http.Client{}
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
