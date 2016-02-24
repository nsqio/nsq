package nsqdserver

import (
	"crypto/tls"
	"github.com/absolute8511/nsq/consistence"
	"github.com/absolute8511/nsq/nsqd"
	"net"
	"sync/atomic"
	"time"
)

type context struct {
	clientIDSequence int64
	nsqd             *nsqd.NSQD
	nsqdCoord        *consistence.NsqdCoordinator
	tlsConfig        *tls.Config
	httpAddr         *net.TCPAddr
	tcpAddr          *net.TCPAddr
}

func (c *context) getOpts() *nsqd.Options {
	return c.nsqd.GetOpts()
}

func (c *context) isAuthEnabled() bool {
	return c.nsqd.IsAuthEnabled()
}

func (c *context) nextClientID() int64 {
	return atomic.AddInt64(&c.clientIDSequence, 1)
}

func (c *context) swapOpts(other *nsqd.Options) {
	c.nsqd.SwapOpts(other)
}

func (c *context) triggerOptsNotification() {
	c.nsqd.TriggerOptsNotification()
}

func (c *context) realHTTPAddr() *net.TCPAddr {
	return c.httpAddr
}

func (c *context) realTCPAddr() *net.TCPAddr {
	return c.tcpAddr
}

func (c *context) getStartTime() time.Time {
	return c.nsqd.GetStartTime()
}

func (c *context) getHealth() string {
	return c.nsqd.GetHealth()
}

func (c *context) isHealthy() bool {
	return c.nsqd.IsHealthy()
}

func (c *context) setHealth(err error) {
	c.nsqd.SetHealth(err)
}

func (c *context) getStats() []nsqd.TopicStats {
	return c.nsqd.GetStats()
}

func (c *context) GetTlsConfig() *tls.Config {
	return c.tlsConfig
}

func (c *context) getExistingTopic(name string) (*nsqd.Topic, error) {
	return c.nsqd.GetExistingTopic(name)
}

func (c *context) getTopic(name string, part int) *nsqd.Topic {
	return c.nsqd.GetTopic(name, part)
}

func (c *context) deleteExistingTopic(name string) error {
	return c.nsqd.DeleteExistingTopic(name)
}

func (c *context) persistMetadata() {
	c.nsqd.Lock()
	c.nsqd.PersistMetadata()
	c.nsqd.Unlock()
}

func (c *context) checkForMasterWrite(topic *nsqd.Topic) bool {
	return true
}

func (c *context) PutMessage(topic *nsqd.Topic, msg []byte) error {
	if c.nsqdCoord == nil {
		msg := nsqd.NewMessage(0, msg)
		_, _, err := topic.PutMessage(msg)
		return err
	}
	return c.nsqdCoord.PutMessageToCluster(topic, msg)
}

func (c *context) forwardPutMessage(topic string, port int, msg []byte) error {
	return nil
}

func (c *context) FinishMessage(ch *nsqd.Channel, clientID int64, msgID nsqd.MessageID) error {
	if c.nsqdCoord == nil {
		_, err := ch.FinishMessage(clientID, msgID)
		return err
	}
	return c.nsqdCoord.FinishMessageToCluster(ch, clientID, msgID)
}
