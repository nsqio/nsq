package slack

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
)

func (s *Slack) call(method string, params url.Values, v interface{}) error {
	log.Printf("Slack %s %s", method, params.Encode())

	params.Set("token", s.Token)
	baseEndpoint := "https://slack.com/api/" + method
	endpoint := baseEndpoint + "?" + params.Encode()
	params.Set("token", "REDACTED")

	client := s.Client
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Get(endpoint)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return fmt.Errorf("error got StatusCode=%d for %s %s", resp.StatusCode, baseEndpoint, params.Encode())
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	type statusWrapper struct {
		Ok bool `json:"ok"`
	}
	var data statusWrapper
	err = json.Unmarshal(body, &data)
	if err != nil {
		return err
	}
	if !data.Ok {
		return fmt.Errorf("error got response %s for %s %s", body, method, params.Encode())
	}
	if v == nil {
		return nil
	}
	return json.Unmarshal(body, v)
}
