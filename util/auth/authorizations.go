package auth

import (
	"errors"
	"fmt"
	"log"
	"net/url"
	"regexp"
	"time"

	"github.com/bitly/nsq/util"
)

type Authorization struct {
	Topic       string   `json:"topic"`
	Channels    []string `json:"channels"`
	Permissions []string `json:"permissions"`
}

type AuthState struct {
	TTL            int             `json:"ttl"`
	Authorizations []Authorization `json:"authorizations"`
	Identity       string          `json:"identity"`
	IdentityUrl    string          `json:"identity_url"`
	Expires        time.Time
}

func (a *Authorization) HasPermission(permission string) bool {
	for _, p := range a.Permissions {
		if permission == p {
			return true
		}
	}
	return false
}

func (a *Authorization) IsAllowed(topic, channel string) bool {
	if channel != "" {
		if !a.HasPermission("subscribe") {
			return false
		}
	} else {
		if !a.HasPermission("publish") {
			return false
		}
	}

	topicRegex := regexp.MustCompile(a.Topic)

	if !topicRegex.MatchString(topic) {
		return false
	}

	for _, c := range a.Channels {
		channelRegex := regexp.MustCompile(c)
		if channelRegex.MatchString(channel) {
			return true
		}
	}
	return false
}

func (a *AuthState) IsAllowed(topic, channel string) bool {
	for _, aa := range a.Authorizations {
		if aa.IsAllowed(topic, channel) {
			return true
		}
	}
	return false
}

func (a *AuthState) IsExpired() bool {
	if a.Expires.Before(time.Now()) {
		return true
	}
	return false
}

func QueryAnyAuthd(authd []string, remoteIp, tlsEnabled, authSecret string) (*AuthState, error) {
	for _, a := range authd {
		authState, err := QueryAuthd(a, remoteIp, tlsEnabled, authSecret)
		if err != nil {
			log.Printf("Error: failed auth against %s %s", a, err)
			continue
		}
		return authState, nil
	}
	return nil, errors.New("Unable to access auth server")
}

func QueryAuthd(authd, remoteIp, tlsEnabled, authSecret string) (*AuthState, error) {

	v := url.Values{}
	v.Set("remote_ip", remoteIp)
	v.Set("tls", tlsEnabled)
	v.Set("secret", authSecret)

	endpoint := fmt.Sprintf("http://%s/auth?%s", authd, v.Encode())

	var authState AuthState
	if err := util.ApiRequestV1(endpoint, &authState); err != nil {
		return nil, err
	}

	// validation on response
	for _, auth := range authState.Authorizations {
		for _, p := range auth.Permissions {
			switch p {
			case "subscribe", "publish":
			default:
				return nil, fmt.Errorf("unknown permission %s", p)
			}
		}

		if _, err := regexp.Compile(auth.Topic); err != nil {
			return nil, fmt.Errorf("unable to compile topic %q %s", auth.Topic, err)
		}

		for _, channel := range auth.Channels {
			if _, err := regexp.Compile(channel); err != nil {
				return nil, fmt.Errorf("unable to compile channel %q %s", channel, err)
			}
		}
	}

	if authState.TTL <= 0 {
		return nil, fmt.Errorf("invalid TTL %d (must be >0)", authState.TTL)
	}

	authState.Expires = time.Now().Add(time.Duration(authState.TTL) * time.Second)
	return &authState, nil
}
