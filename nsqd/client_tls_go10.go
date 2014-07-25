// +build !go1.3

package nsqd

func (p *prettyConnectionState) GetVersion() string {
	return "TLS1.0"
}
