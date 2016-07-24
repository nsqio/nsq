package nsqd

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mreiferson/wal"
	"github.com/nsqio/nsq/internal/lg"
	"github.com/nsqio/nsq/internal/quantile"
)

type Topic struct {
	// 64bit atomic vars need to be first for proper alignment on 32bit platforms
	messageCount uint64
	messageBytes uint64
	pauseIdx     uint64

	sync.RWMutex

	name       string
	channelMap map[string]*Channel
	wal        wal.WriteAheadLogger
	rs         RangeSet

	paused    int32
	ephemeral bool

	deleteCallback func(*Topic)
	deleter        sync.Once

	exitChan chan int
	exitFlag int32

	ctx *context
}

// Topic constructor
func NewTopic(topicName string, ctx *context, deleteCallback func(*Topic)) *Topic {
	t := &Topic{
		name:           topicName,
		channelMap:     make(map[string]*Channel),
		exitChan:       make(chan int),
		ctx:            ctx,
		deleteCallback: deleteCallback,
		ephemeral:      strings.HasSuffix(topicName, "#ephemeral"),
	}

	if t.ephemeral {
		t.wal = wal.NewEphemeral()
	} else {
		dqLogf := func(level lg.LogLevel, f string, args ...interface{}) {
			opts := ctx.nsqd.getOpts()
			lg.Logf(opts.Logger, opts.logLevel, lg.LogLevel(level), f, args...)
		}
		t.wal, _ = wal.New(topicName,
			ctx.nsqd.getOpts().DataPath,
			ctx.nsqd.getOpts().MaxBytesPerFile,
			ctx.nsqd.getOpts().SyncTimeout,
			dqLogf,
		)
	}

	fn := fmt.Sprintf(path.Join(ctx.nsqd.getOpts().DataPath, "meta.%s.dat"), t.name)
	data, err := ioutil.ReadFile(fn)
	if err != nil {
		if !os.IsNotExist(err) {
			t.ctx.nsqd.logf("ERROR: failed to read topic metadata from %s - %s", fn, err)
		}
	} else {
		err := json.Unmarshal(data, &t.rs)
		if err != nil {
			t.ctx.nsqd.logf("ERROR: failed to decode topic metadata - %s", err)
		}
	}

	t.ctx.nsqd.Notify(t)

	return t
}

// Exiting returns a boolean indicating if this topic is closed/exiting
func (t *Topic) Exiting() bool {
	return atomic.LoadInt32(&t.exitFlag) == 1
}

// GetChannel performs a thread safe operation
// to return a pointer to a Channel object (potentially new)
// for the given Topic
func (t *Topic) GetChannel(channelName string) *Channel {
	t.Lock()
	channel, _ := t.getOrCreateChannel(channelName)
	t.Unlock()
	return channel
}

// this expects the caller to handle locking
func (t *Topic) getOrCreateChannel(channelName string) (*Channel, bool) {
	channel, ok := t.channelMap[channelName]
	if !ok {
		var startIdx uint64
		if len(t.channelMap) > 0 {
			startIdx = t.wal.Index()
			if t.IsPaused() {
				startIdx = atomic.LoadUint64(&t.pauseIdx)
			}
		}
		channel = NewChannel(t, channelName, startIdx, t.ctx)
		t.channelMap[channelName] = channel
		t.ctx.nsqd.logf(LOG_INFO, "TOPIC(%s): new channel(%s)", t.name, channel.name)
		return channel, true
	}
	return channel, false
}

func (t *Topic) GetExistingChannel(channelName string) (*Channel, error) {
	t.RLock()
	defer t.RUnlock()
	channel, ok := t.channelMap[channelName]
	if !ok {
		return nil, errors.New("channel does not exist")
	}
	return channel, nil
}

// DeleteExistingChannel removes a channel from the topic only if it exists
func (t *Topic) DeleteExistingChannel(channelName string) error {
	t.Lock()
	channel, ok := t.channelMap[channelName]
	if !ok {
		t.Unlock()
		return errors.New("channel does not exist")
	}
	delete(t.channelMap, channelName)
	// not defered so that we can continue while the channel async closes
	numChannels := len(t.channelMap)
	t.Unlock()

	t.ctx.nsqd.logf(LOG_INFO, "TOPIC(%s): deleting channel %s", t.name, channel.name)

	// delete empties the channel before closing
	// (so that we dont leave any messages around)
	channel.Delete()

	if numChannels == 0 && t.ephemeral == true {
		go t.deleter.Do(func() { t.deleteCallback(t) })
	}

	return nil
}

func (t *Topic) Pub(entries []wal.EntryWriterTo) error {
	t.RLock()
	defer t.RUnlock()
	if atomic.LoadInt32(&t.exitFlag) == 1 {
		return errors.New("exiting")
	}
	startIdx, endIdx, err := t.wal.Append(entries)
	t.ctx.nsqd.SetHealth(err)
	if err != nil {
		return err
	}
	// TODO: (WAL) this is racey
	t.rs.AddRange(Range{Low: int64(startIdx), High: int64(endIdx)})
	atomic.AddUint64(&t.messageCount, uint64(len(entries)))
	var total uint64
	for _, e := range entries {
		total += uint64(len(e.Body))
	}
	atomic.AddUint64(&t.messageBytes, total)
	return nil
}

func (t *Topic) Depth() uint64 {
	t.RLock()
	defer t.RUnlock()
	var depth uint64
	if len(t.channelMap) > 0 {
		if t.IsPaused() {
			depth = t.wal.Index() - atomic.LoadUint64(&t.pauseIdx)
		}
	} else {
		depth = t.rs.Count()
	}
	return depth
}

// Delete empties the topic and all its channels and closes
func (t *Topic) Delete() error {
	return t.exit(true)
}

// Close persists all outstanding topic data and closes all its channels
func (t *Topic) Close() error {
	return t.exit(false)
}

func (t *Topic) exit(deleted bool) error {
	if !atomic.CompareAndSwapInt32(&t.exitFlag, 0, 1) {
		return errors.New("exiting")
	}

	if deleted {
		t.ctx.nsqd.logf(LOG_INFO, "TOPIC(%s): deleting", t.name)

		// since we are explicitly deleting a topic (not just at system exit time)
		// de-register this from the lookupd
		t.ctx.nsqd.Notify(t)
	} else {
		t.ctx.nsqd.logf(LOG_INFO, "TOPIC(%s): closing", t.name)
	}

	close(t.exitChan)

	if deleted {
		t.Lock()
		for _, channel := range t.channelMap {
			delete(t.channelMap, channel.name)
			channel.Delete()
		}
		t.Unlock()

		// empty the queue (deletes the backend files, too)
		return t.wal.Delete()
	}

	// close all the channels
	for _, channel := range t.channelMap {
		err := channel.Close()
		if err != nil {
			// we need to continue regardless of error to close all the channels
			t.ctx.nsqd.logf(LOG_ERROR, "channel(%s) close - %s", channel.name, err)
		}
	}

	// TODO: (WAL) this is racey
	data, err := json.Marshal(&t.rs)
	if err != nil {
		return err
	}

	fn := fmt.Sprintf(path.Join(t.ctx.nsqd.getOpts().DataPath, "meta.%s.dat"), t.name)
	tmpFn := fmt.Sprintf("%s.%d.tmp", fn, rand.Int())
	f, err := os.OpenFile(tmpFn, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}

	_, err = f.Write(data)
	if err != nil {
		f.Close()
		return err
	}
	f.Sync()
	f.Close()

	err = os.Rename(tmpFn, fn)
	if err != nil {
		return err
	}

	return t.wal.Close()
}

func (t *Topic) Empty() error {
	return t.wal.Empty()
}

func (t *Topic) AggregateChannelE2eProcessingLatency() *quantile.Quantile {
	var latencyStream *quantile.Quantile
	t.RLock()
	channels := make([]*Channel, 0, len(t.channelMap))
	for _, c := range t.channelMap {
		channels = append(channels, c)
	}
	t.RUnlock()
	for _, c := range channels {
		if c.e2eProcessingLatencyStream == nil {
			continue
		}
		if latencyStream == nil {
			latencyStream = quantile.New(
				t.ctx.nsqd.getOpts().E2EProcessingLatencyWindowTime,
				t.ctx.nsqd.getOpts().E2EProcessingLatencyPercentiles)
		}
		latencyStream.Merge(c.e2eProcessingLatencyStream)
	}
	return latencyStream
}

func (t *Topic) Pause() error {
	return t.doPause(true)
}

func (t *Topic) UnPause() error {
	return t.doPause(false)
}

func (t *Topic) doPause(pause bool) error {
	t.RLock()
	channels := make([]*Channel, 0, len(t.channelMap))
	for _, c := range t.channelMap {
		channels = append(channels, c)
	}
	t.RUnlock()

	if pause {
		atomic.StoreInt32(&t.paused, 1)
		for _, c := range channels {
			c.Pause()
		}
	} else {
		atomic.StoreInt32(&t.paused, 0)
		for _, c := range channels {
			c.UnPause()
		}
	}

	atomic.StoreUint64(&t.pauseIdx, t.wal.Index())

	return nil
}

func (t *Topic) IsPaused() bool {
	return atomic.LoadInt32(&t.paused) == 1
}

func (t *Topic) GenerateID() MessageID {
retry:
	id, err := t.idFactory.NewGUID()
	if err != nil {
		time.Sleep(time.Millisecond)
		goto retry
	}
	return id.Hex()
}
