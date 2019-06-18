package auth

import (
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net/url"
	"regexp"
	"time"

	"github.com/nsqio/nsq/internal/http_api"
)

type Authorization struct {
	Topic       string   `json:"topic"`
	Channels    []string `json:"channels"`
	Permissions []string `json:"permissions"`
}

type State struct {
	TTL            int             `json:"ttl"`
	Authorizations []Authorization `json:"authorizations"`
	Identity       string          `json:"identity"`
	IdentityURL    string          `json:"identity_url"`
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

func (a *State) IsAllowed(topic, channel string) bool {
	for _, aa := range a.Authorizations {
		if aa.IsAllowed(topic, channel) {
			return true
		}
	}
	return false
}

func (a *State) IsExpired() bool {
	if a.Expires.Before(time.Now()) {
		return true
	}
	return false
}

func QueryAnyAuthd(authd []string, remoteIP string, tlsEnabled bool, commonName string, authSecret string,
	connectTimeout time.Duration, requestTimeout time.Duration) (*State, error) {
	start := rand.Int()
	n := len(authd)
	for i := 0; i < n; i++ {
		a := authd[(i+start)%n]
		authState, err := QueryAuthd(a, remoteIP, tlsEnabled, commonName, authSecret, connectTimeout, requestTimeout)
		if err != nil {
			log.Printf("Error: failed auth against %s %s", a, err)
			continue
		}
		return authState, nil
	}
	return nil, errors.New("Unable to access auth server")
}

func QueryAuthd(authd string, remoteIP string, tlsEnabled bool, commonName string, authSecret string,
	connectTimeout time.Duration, requestTimeout time.Duration) (*State, error) {
	v := url.Values{}
	v.Set("remote_ip", remoteIP)
	if tlsEnabled {
		v.Set("tls", "true")
	} else {
		v.Set("tls", "false")
	}
	v.Set("secret", authSecret)
	v.Set("common_name", commonName)

	endpoint := fmt.Sprintf("http://%s/auth?%s", authd, v.Encode())

	var authState State
	client := http_api.NewClient(nil, connectTimeout, requestTimeout)
	if err := client.GETV1(endpoint, &authState); err != nil {
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
