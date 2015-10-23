package nsqd

import (
	"sync/atomic"

	"github.com/nsqio/nsq/internal/auth"
)

type AuthService struct {
	AuthState *auth.State
	AuthParameters
}

type AuthParameters struct {
	AuthHTTPAddresses []string
	RemoteIP          string
	TLS               int32
	AuthSecret        string
}

func (a *AuthService) Auth(secret string) error {
	a.AuthSecret = secret
	return a.QueryAuthd()
}

func (a *AuthService) QueryAuthd() error {
	tls := atomic.LoadInt32(&a.TLS) == 1
	tlsEnabled := "false"
	if tls {
		tlsEnabled = "true"
	}

	authState, err := auth.QueryAnyAuthd(a.AuthHTTPAddresses, a.RemoteIP, tlsEnabled, a.AuthSecret)
	if err != nil {
		return err
	}
	a.AuthState = authState
	return nil
}

func (a *AuthService) IsAuthorized(topic, channel string) (bool, error) {
	if a.AuthState == nil {
		return false, nil
	}
	if a.AuthState.IsExpired() {
		err := a.QueryAuthd()
		if err != nil {
			return false, err
		}
	}
	if a.AuthState.IsAllowed(topic, channel) {
		return true, nil
	}
	return false, nil
}

func (c *clientV2) HasAuthorizations() bool {
	if c.AuthState != nil {
		return len(c.AuthState.Authorizations) != 0
	}
	return false
}
