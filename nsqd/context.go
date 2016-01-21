package nsqd

import (
	"github.com/absolute8511/nsq/consistence"
	"net"
	"time"
)

type context struct {
	nsqd      *NSQD
	nsqdCoord *consistence.NsqdCoordinator
}

func (c *context) getOpts() *Options {
	return c.nsqd.getOpts()
}

func (c *context) isAuthEnabled() bool {
	return c.isAuthEnabled()
}

func (c *context) swapOpts(other *Options) {
	c.nsqd.swapOpts(other)
}

func (c *context) triggerOptsNotification() {
	c.nsqd.triggerOptsNotification()
}

func (c *context) realHTTPSAddr() *net.TCPAddr {
	return c.nsqd.RealHTTPSAddr()
}

func (c *context) realHTTPAddr() *net.TCPAddr {
	return c.nsqd.RealHTTPAddr()
}

func (c *context) realTCPAddr() *net.TCPAddr {
	return c.nsqd.RealTCPAddr()
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

func (c *context) getStats() []TopicStats {
	return c.nsqd.GetStats()
}

func (c *context) getExistingTopic(name string) (*Topic, error) {
	return c.nsqd.GetExistingTopic(name)
}

func (c *context) getTopic(name string, part int) *Topic {
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
