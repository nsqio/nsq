package nsqdserver

import (
	"crypto/tls"
	"errors"
	"github.com/absolute8511/nsq/consistence"
	"github.com/absolute8511/nsq/nsqd"
	"net"
	"sync/atomic"
	"time"
)

const (
	FailedOnNotLeader   = consistence.ErrFailedOnNotLeader
	FailedOnNotWritable = consistence.ErrFailedOnNotWritable
)

type context struct {
	clientIDSequence int64
	nsqd             *nsqd.NSQD
	nsqdCoord        *consistence.NsqdCoordinator
	tlsConfig        *tls.Config
	httpAddr         *net.TCPAddr
	tcpAddr          *net.TCPAddr
	reverseProxyPort string
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
	consistence.SetCoordLogLevel(other.LogLevel)
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

func (c *context) getDefaultPartition(topic string) int {
	if c.nsqdCoord != nil {
		pid, _, err := c.nsqdCoord.GetMasterTopicCoordData(topic)
		if err != nil {
			return -1
		}
		return pid
	}
	return c.nsqd.GetTopicDefaultPart(topic)
}

func (c *context) getPartitions(name string) map[int]*nsqd.Topic {
	return c.nsqd.GetTopicPartitions(name)
}

func (c *context) getExistingTopic(name string, part int) (*nsqd.Topic, error) {
	return c.nsqd.GetExistingTopic(name, part)
}

func (c *context) getTopic(name string, part int) *nsqd.Topic {
	return c.nsqd.GetTopic(name, part)
}

func (c *context) deleteExistingTopic(name string, part int) error {
	return c.nsqd.DeleteExistingTopic(name, part)
}

func (c *context) persistMetadata() {
	tmpMap := c.nsqd.GetTopicMapCopy()
	c.nsqd.PersistMetadata(tmpMap)
}

func (c *context) GetDistributedID() string {
	if c.nsqdCoord == nil {
		return ""
	}
	return c.nsqdCoord.GetMyID()
}

func (c *context) checkForMasterWrite(topic string, part int) bool {
	if c.nsqdCoord == nil {
		return true
	}
	return c.nsqdCoord.IsMineLeaderForTopic(topic, part)
}

func (c *context) PutMessage(topic *nsqd.Topic,
	msg []byte, traceID uint64) (nsqd.MessageID, nsqd.BackendOffset, int32, nsqd.BackendQueueEnd, error) {
	if c.nsqdCoord == nil {
		msg := nsqd.NewMessage(0, msg)
		msg.TraceID = traceID
		return topic.PutMessage(msg)
	}
	return c.nsqdCoord.PutMessageToCluster(topic, msg, traceID)
}

func (c *context) PutMessages(topic *nsqd.Topic, msgs []*nsqd.Message) (nsqd.MessageID, nsqd.BackendOffset, int32, error) {
	if c.nsqdCoord == nil {
		id, offset, rawSize, _, _, err := topic.PutMessages(msgs)
		return id, offset, rawSize, err
	}
	return c.nsqdCoord.PutMessagesToCluster(topic, msgs)
}

func (c *context) FinishMessage(ch *nsqd.Channel, clientID int64, msgID nsqd.MessageID) error {
	if c.nsqdCoord == nil {
		_, _, _, err := ch.FinishMessage(clientID, msgID)
		if err == nil {
			ch.ContinueConsumeForOrder()
		}
		return err
	}
	return c.nsqdCoord.FinishMessageToCluster(ch, clientID, msgID)
}

func (c *context) DeleteExistingChannel(topic *nsqd.Topic, channelName string) error {
	if c.nsqdCoord == nil {
		err := topic.DeleteExistingChannel(channelName)
		return err
	}
	return c.nsqdCoord.DeleteChannel(topic, channelName)
}

func (c *context) SetChannelOffset(ch *nsqd.Channel, startFrom *ConsumeOffset, force bool) (int64, int64, error) {
	var l *consistence.CommitLogData
	var queueOffset int64
	cnt := int64(0)
	var err error
	if startFrom.OffsetType == offsetTimestampType {
		if c.nsqdCoord != nil {
			l, queueOffset, cnt, err = c.nsqdCoord.SearchLogByMsgTimestamp(ch.GetTopicName(), ch.GetTopicPart(), startFrom.OffsetValue)
		} else {
			err = errors.New("Not supported while coordinator disabled")
		}
	} else if startFrom.OffsetType == offsetSpecialType {
		if startFrom.OffsetValue == -1 {
			e := ch.GetChannelEnd()
			queueOffset = int64(e.Offset())
			cnt = e.TotalMsgCnt()
		} else {
			nsqd.NsqLogger().Logf("not known special offset :%v", startFrom)
			err = errors.New("not supported offset type")
		}
	} else if startFrom.OffsetType == offsetVirtualQueueType {
		queueOffset = startFrom.OffsetValue
		cnt = 0
		if c.nsqdCoord != nil {
			l, queueOffset, cnt, err = c.nsqdCoord.SearchLogByMsgOffset(ch.GetTopicName(), ch.GetTopicPart(), queueOffset)
		} else {
			err = errors.New("Not supported while coordinator disabled")
		}
	} else {
		nsqd.NsqLogger().Logf("not supported offset type:%v", startFrom)
		err = errors.New("not supported offset type")
	}
	if err != nil {
		nsqd.NsqLogger().Logf("failed to search the consume offset: %v, err:%v", startFrom, err)
		return 0, 0, err
	}
	nsqd.NsqLogger().Logf("%v searched log : %v, offset: %v:%v", startFrom, l, queueOffset, cnt)
	if c.nsqdCoord == nil {
		err = ch.SetConsumeOffset(nsqd.BackendOffset(queueOffset), cnt, force)
		if err != nil {
			if err != nsqd.ErrSetConsumeOffsetNotFirstClient {
				nsqd.NsqLogger().Logf("failed to set the consume offset: %v, err:%v", startFrom, err)
				return 0, 0, err
			}
			nsqd.NsqLogger().Logf("the consume offset: %v can only be set by the first client", startFrom, err)
		}
	} else {
		err = c.nsqdCoord.SetChannelConsumeOffsetToCluster(ch, queueOffset, cnt, force)
		if err != nil {
			if coordErr, ok := err.(*consistence.CommonCoordErr); ok {
				if coordErr.IsEqual(consistence.ErrLocalSetChannelOffsetNotFirstClient) {
					nsqd.NsqLogger().Logf("the consume offset: %v can only be set by the first client", startFrom)
					return queueOffset, cnt, nil
				}
			}
			nsqd.NsqLogger().Logf("failed to set the consume offset: %v (%v:%v), err: %v ", startFrom, queueOffset, cnt, err)
			return 0, 0, err
		}
	}
	return queueOffset, cnt, nil
}
