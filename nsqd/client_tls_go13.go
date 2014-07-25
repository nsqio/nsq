// +build go1.3

package nsqd

import (
	"crypto/tls"
	"fmt"
)

func (p *prettyConnectionState) GetVersion() string {
	switch p.Version {
	case tls.VersionSSL30:
		return "SSL30"
	case tls.VersionTLS10:
		return "TLS1.0"
	case tls.VersionTLS11:
		return "TLS1.1"
	case tls.VersionTLS12:
		return "TLS1.2"
	default:
		return fmt.Sprintf("Unknown %d", p.Version)
	}
}
