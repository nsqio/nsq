package nsqd

import (
	"sync/atomic"
	"time"

	"github.com/nsqio/nsq/internal/auth"
)

type AuthService struct {
	AuthState *auth.State
	AuthParameters
}

type AuthParameters struct {
	AuthHTTPAddresses        []string
	RemoteIP                 string
	TLS                      int32
	AuthSecret               string
	HTTPClientConnectTimeout time.Duration
	HTTPClientRequestTimeout time.Duration
}

func (a *AuthService) Auth(secret string) error {
	a.AuthSecret = secret
	return a.QueryAuthd()
}

func (a *AuthService) QueryAuthd() error {
	var tlsEnabled string
	tls := atomic.LoadInt32(&a.TLS)
	if tls == 1 {
		tlsEnabled = "true"
	} else {
		tlsEnabled = "false"
	}

	authState, err := auth.QueryAnyAuthd(a.AuthHTTPAddresses, a.RemoteIP, tlsEnabled, a.AuthSecret,
		a.HTTPClientConnectTimeout, a.HTTPClientRequestTimeout)
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
