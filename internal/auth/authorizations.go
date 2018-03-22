package auth

import (
	"errors"
	"fmt"
	"log"
	"net/url"
	"regexp"
	"strings"
	"sync"
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

var authStateCache map[string]State
var authStateCacheMutex sync.RWMutex

func init() {
	authStateCache = make(map[string]State)
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

func QueryAnyAuthd(authd []string, remoteIP, tlsEnabled, authSecret string,
	connectTimeout time.Duration, requestTimeout time.Duration) (*State, error) {
	for _, a := range authd {
		authState, err := QueryAuthd(a, remoteIP, tlsEnabled, authSecret, connectTimeout, requestTimeout)
		if err != nil {
			log.Printf("Error: failed auth against %s %s", a, err)
			continue
		}
		return authState, nil
	}
	return nil, errors.New("unable to access auth server")
}

func addScheme(url string) string {
	lowerUrl := strings.ToLower(url)
	if !strings.HasPrefix(lowerUrl, "http://") && !strings.HasPrefix(lowerUrl, "https://") {
		url = fmt.Sprintf("http://%s", url)
	}
	return url
}

func QueryAuthd(authd, remoteIP, tlsEnabled, authSecret string,
	connectTimeout time.Duration, requestTimeout time.Duration) (*State, error) {
	v := url.Values{}
	v.Set("remote_ip", remoteIP)
	v.Set("tls", tlsEnabled)
	v.Set("secret", authSecret)

	endpoint := fmt.Sprintf("%s/auth?%s", addScheme(authd), v.Encode())

	authStateCacheMutex.RLock()
	authState, ok := authStateCache[endpoint]
	authStateCacheMutex.RUnlock()
	if ok && !authState.IsExpired() {
		return &authState, nil
	}

	client := http_api.NewClient(nil, connectTimeout, requestTimeout)
	if err := client.GETV1(endpoint, &authState); err != nil {
		return nil, err
	}

	// validation on response
	for _, auth := range authState.Authorizations {
		for _, p := range auth.Permissions {
			// TODO (judwhite): Add more granular permissions, for example delete, pause, etc
			switch p {
			case "subscribe", "publish":
			default:
				log.Printf("Warning: unknown permission %s", p)
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

	authStateCacheMutex.Lock()
	authStateCache[endpoint] = authState
	authStateCacheMutex.Unlock()

	return &authState, nil
}
