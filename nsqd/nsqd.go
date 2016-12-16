package nsqd

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net"
	"os"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/absolute8511/nsq/internal/clusterinfo"
	"github.com/absolute8511/nsq/internal/dirlock"
	"github.com/absolute8511/nsq/internal/http_api"
	"github.com/absolute8511/nsq/internal/protocol"
	"github.com/absolute8511/nsq/internal/statsd"
	"github.com/absolute8511/nsq/internal/util"
	"github.com/absolute8511/nsq/internal/version"
	"github.com/bitly/go-simplejson"
)

const (
	TLSNotRequired = iota
	TLSRequiredExceptHTTP
	TLSRequired
)

type errStore struct {
	err error
}

var (
	ErrTopicPartitionMismatch = errors.New("topic partition mismatch")
	ErrTopicNotExist          = errors.New("topic does not exist")
)

var DEFAULT_RETENTION_DAYS = 7

const (
	FLUSH_DISTANCE = 4
)

type NSQD struct {
	sync.RWMutex

	opts atomic.Value

	dl        *dirlock.DirLock
	isLoading int32
	errValue  atomic.Value
	startTime time.Time

	topicMap       map[string]map[int]*Topic
	magicCodeMutex sync.Mutex

	poolSize int

	MetaNotifyChan       chan interface{}
	OptsNotificationChan chan struct{}
	exitChan             chan int
	waitGroup            util.WaitGroupWrapper

	ci          *clusterinfo.ClusterInfo
	exiting     bool
	persisting  int32
	pubLoopFunc func(t *Topic)
}

func New(opts *Options) *NSQD {
	dataPath := opts.DataPath
	if opts.DataPath == "" {
		cwd, _ := os.Getwd()
		dataPath = cwd
		opts.DataPath = dataPath
	}
	err := os.MkdirAll(dataPath, 0755)
	if err != nil {
		nsqLog.LogErrorf("failed to create directory: %v ", err)
		os.Exit(1)
	}
	DEFAULT_RETENTION_DAYS = int(opts.RetentionDays)

	nsqLog.Logger = opts.Logger
	SetRemoteMsgTracer(opts.RemoteTracer)

	n := &NSQD{
		startTime:            time.Now(),
		topicMap:             make(map[string]map[int]*Topic),
		exitChan:             make(chan int),
		MetaNotifyChan:       make(chan interface{}),
		OptsNotificationChan: make(chan struct{}, 1),
		ci:                   clusterinfo.New(opts.Logger, http_api.NewClient(nil)),
		dl:                   dirlock.New(dataPath),
	}
	n.SwapOpts(opts)

	n.errValue.Store(errStore{})

	err = n.dl.Lock()
	if err != nil {
		nsqLog.LogErrorf("FATAL: --data-path=%s in use (possibly by another instance of nsqd: %v", dataPath, err)
		os.Exit(1)
	}

	if opts.MaxDeflateLevel < 1 || opts.MaxDeflateLevel > 9 {
		nsqLog.LogErrorf("FATAL: --max-deflate-level must be [1,9]")
		os.Exit(1)
	}

	if opts.ID < 0 || opts.ID >= MAX_NODE_ID {
		nsqLog.LogErrorf("FATAL: --worker-id must be [0,%d)", MAX_NODE_ID)
		os.Exit(1)
	}
	nsqLog.Logf("broadcast option: %s, %s", opts.BroadcastAddress, opts.BroadcastInterface)

	if opts.StatsdPrefix != "" {
		var port string
		if opts.ReverseProxyPort != "" {
			port = opts.ReverseProxyPort
		} else {
			_, port, err = net.SplitHostPort(opts.HTTPAddress)
			if err != nil {
				nsqLog.LogErrorf("failed to parse HTTP address (%s) - %s", opts.HTTPAddress, err)
				os.Exit(1)
			}
		}
		statsdHostKey := statsd.HostKey(net.JoinHostPort(opts.BroadcastAddress, port))
		prefixWithHost := strings.Replace(opts.StatsdPrefix, "%s", statsdHostKey, -1)
		if prefixWithHost[len(prefixWithHost)-1] != '.' {
			prefixWithHost += "."
		}
		opts.StatsdPrefix = prefixWithHost
		nsqLog.Infof("using the stats prefix: %v", opts.StatsdPrefix)
	}

	if opts.TLSClientAuthPolicy != "" && opts.TLSRequired == TLSNotRequired {
		opts.TLSRequired = TLSRequired
	}

	return n
}

func (n *NSQD) SetPubLoop(loop func(t *Topic)) {
	n.Lock()
	n.pubLoopFunc = loop
	n.Unlock()
}

func (n *NSQD) GetOpts() *Options {
	return n.opts.Load().(*Options)
}

func (n *NSQD) SwapOpts(opts *Options) {
	nsqLog.SetLevel(opts.LogLevel)
	n.opts.Store(opts)
}

func (n *NSQD) TriggerOptsNotification() {
	select {
	case n.OptsNotificationChan <- struct{}{}:
	default:
	}
}

func (n *NSQD) SetHealth(err error) {
	n.errValue.Store(errStore{err: err})
}

func (n *NSQD) IsHealthy() bool {
	return n.GetError() == nil
}

func (n *NSQD) GetError() error {
	errValue := n.errValue.Load()
	return errValue.(errStore).err
}

func (n *NSQD) GetHealth() string {
	err := n.GetError()
	if err != nil {
		return fmt.Sprintf("NOK - %s", err)
	}
	return "OK"
}

func (n *NSQD) GetStartTime() time.Time {
	return n.startTime
}

// should be protected by read lock
func (n *NSQD) GetTopicMapRef() map[string]map[int]*Topic {
	return n.topicMap
}

func (n *NSQD) GetTopicPartitions(topicName string) map[int]*Topic {
	tmpMap := make(map[int]*Topic)
	n.RLock()
	parts, ok := n.topicMap[topicName]
	if ok {
		for p, t := range parts {
			tmpMap[p] = t
		}
	}
	n.RUnlock()
	return tmpMap
}

func (n *NSQD) GetTopicMapCopy() map[string]map[int]*Topic {
	tmpMap := make(map[string]map[int]*Topic)
	n.RLock()
	for k, topics := range n.topicMap {
		var tmpTopics map[int]*Topic
		var ok bool
		tmpTopics, ok = tmpMap[k]
		if !ok {
			tmpTopics = make(map[int]*Topic, len(topics))
			tmpMap[k] = tmpTopics
		}
		for p, t := range topics {
			tmpTopics[p] = t
		}
	}
	n.RUnlock()
	return tmpMap
}

func (n *NSQD) Start() {
	n.waitGroup.Wrap(func() { n.queueScanLoop() })
}

func (n *NSQD) LoadMetadata(disabled int32) {
	atomic.StoreInt32(&n.isLoading, 1)
	defer atomic.StoreInt32(&n.isLoading, 0)
	fn := fmt.Sprintf(path.Join(n.GetOpts().DataPath, "nsqd.%d.dat"), n.GetOpts().ID)
	data, err := ioutil.ReadFile(fn)
	if err != nil {
		if !os.IsNotExist(err) {
			nsqLog.LogErrorf("failed to read channel metadata from %s - %s", fn, err)
		}
		return
	}

	js, err := simplejson.NewJson(data)
	if err != nil {
		nsqLog.LogErrorf("failed to parse metadata - %s", err)
		return
	}

	topics, err := js.Get("topics").Array()
	if err != nil {
		nsqLog.LogErrorf("failed to parse metadata - %s", err)
		return
	}

	for ti := range topics {
		topicJs := js.Get("topics").GetIndex(ti)

		topicName, err := topicJs.Get("name").String()
		if err != nil {
			nsqLog.LogErrorf("failed to parse metadata - %s", err)
			return
		}
		if !protocol.IsValidTopicName(topicName) {
			nsqLog.LogWarningf("skipping creation of invalid topic %s", topicName)
			continue
		}
		part, err := topicJs.Get("partition").Int()
		if err != nil {
			nsqLog.LogErrorf("failed to parse metadata - %s", err)
			return
		}
		topic := n.internalGetTopic(topicName, part, disabled)

		// old meta should also be loaded
		channels, err := topicJs.Get("channels").Array()
		if err != nil {
			nsqLog.LogErrorf("failed to parse metadata - %s", err)
			return
		}

		for ci := range channels {
			channelJs := topicJs.Get("channels").GetIndex(ci)

			channelName, err := channelJs.Get("name").String()
			if err != nil {
				nsqLog.LogErrorf("failed to parse metadata - %s", err)
				return
			}
			if !protocol.IsValidChannelName(channelName) {
				nsqLog.LogWarningf("skipping creation of invalid channel %s", channelName)
				continue
			}
			channel := topic.GetChannel(channelName)

			paused, _ := channelJs.Get("paused").Bool()
			if paused {
				channel.Pause()
			}
		}
		// we load channels from the new meta file
		topic.LoadChannelMeta()
	}
}

func (n *NSQD) PersistMetadata(currentTopicMap map[string]map[int]*Topic) error {
	if !atomic.CompareAndSwapInt32(&n.persisting, 0, 1) {
		nsqLog.Logf("NSQ: persisting is already running")
		return nil
	}
	defer atomic.StoreInt32(&n.persisting, 0)
	// persist metadata about what topics/channels we have
	// so that upon restart we can get back to the same state
	fileName := fmt.Sprintf(path.Join(n.GetOpts().DataPath, "nsqd.%d.dat"), n.GetOpts().ID)
	nsqLog.Logf("NSQ: persisting topic/channel metadata to %s", fileName)

	js := make(map[string]interface{})
	topics := []interface{}{}
	for _, topicParts := range currentTopicMap {
		for _, topic := range topicParts {
			if topic.ephemeral {
				continue
			}
			topicData := make(map[string]interface{})
			topicData["name"] = topic.GetTopicName()
			topicData["partition"] = topic.GetTopicPart()
			// we save the channels to topic, but for compatible we need save empty channels to json
			channels := []interface{}{}
			err := topic.SaveChannelMeta()
			if err != nil {
				nsqLog.Warningf("save topic %v channel meta failed: %v", topic.GetFullName(), err)
			}
			topicData["channels"] = channels
			topics = append(topics, topicData)
		}
	}
	js["version"] = version.Binary
	js["topics"] = topics

	data, err := json.Marshal(&js)
	if err != nil {
		return err
	}

	tmpFileName := fmt.Sprintf("%s.%d.tmp", fileName, rand.Int())
	f, err := os.OpenFile(tmpFileName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
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

	err = util.AtomicRename(tmpFileName, fileName)
	if err != nil {
		return err
	}

	return nil
}

func (n *NSQD) Exit() {
	n.Lock()
	if n.exiting {
		n.Unlock()
		return
	}
	n.exiting = true
	n.Unlock()

	tmpMap := n.GetTopicMapCopy()
	err := n.PersistMetadata(tmpMap)
	if err != nil {
		nsqLog.LogErrorf(" failed to persist metadata - %s", err)
	}
	nsqLog.Logf("NSQ: closing topics")
	for _, topics := range tmpMap {
		for _, topic := range topics {
			topic.Close()
		}
	}

	// we want to do this last as it closes the idPump (if closed first it
	// could potentially starve items in process and deadlock)
	close(n.exitChan)
	n.waitGroup.Wait()

	n.dl.Unlock()
	nsqLog.Logf("NSQ: exited")
}

func (n *NSQD) GetTopicDefaultPart(topicName string) int {
	n.RLock()
	topics, ok := n.topicMap[topicName]
	if ok {
		if len(topics) > 0 {
			for _, t := range topics {
				n.RUnlock()
				return t.GetTopicPart()
			}
		}
	}
	n.RUnlock()
	return -1
}

func (n *NSQD) GetTopicIgnPart(topicName string) *Topic {
	n.RLock()
	topics, ok := n.topicMap[topicName]
	if ok {
		if len(topics) > 0 {
			for _, t := range topics {
				n.RUnlock()
				return t
			}
		}
	}
	n.RUnlock()

	return n.GetTopic(topicName, 0)
}

func (n *NSQD) GetTopicWithDisabled(topicName string, part int) *Topic {
	return n.internalGetTopic(topicName, part, 1)
}

// GetTopic performs a thread safe operation
// to return a pointer to a Topic object (potentially new)
func (n *NSQD) GetTopic(topicName string, part int) *Topic {
	return n.internalGetTopic(topicName, part, 0)
}

func (n *NSQD) internalGetTopic(topicName string, part int, disabled int32) *Topic {
	if part > MAX_TOPIC_PARTITION || part < 0 {
		return nil
	}
	if topicName == "" {
		nsqLog.Logf("TOPIC name is empty")
		return nil
	}
	// most likely, we already have this topic, so try read lock first.
	n.RLock()
	topics, ok := n.topicMap[topicName]
	if ok {
		t, ok2 := topics[part]
		if ok2 {
			n.RUnlock()
			return t
		}
	}
	n.RUnlock()

	n.Lock()

	topics, ok = n.topicMap[topicName]
	if ok {
		t, ok := topics[part]
		if ok {
			n.Unlock()
			return t
		}
	} else {
		topics = make(map[int]*Topic)
		n.topicMap[topicName] = topics
	}
	if part < 0 {
		part = 0
	}
	deleteCallback := func(t *Topic) {
		n.DeleteExistingTopic(t.GetTopicName(), t.GetTopicPart())
	}
	t := NewTopic(topicName, part, n.GetOpts(), deleteCallback, disabled, n.Notify, n.pubLoopFunc)
	if t == nil {
		nsqLog.Errorf("TOPIC(%s): create failed", topicName)
	} else {
		topics[part] = t
		nsqLog.Logf("TOPIC(%s): created", t.GetFullName())

	}
	n.Unlock()
	if t != nil {
		// update messagePump state
		t.NotifyReloadChannels()
	}
	return t
}

// GetExistingTopic gets a topic only if it exists
func (n *NSQD) GetExistingTopic(topicName string, part int) (*Topic, error) {
	n.RLock()
	defer n.RUnlock()
	topics, ok := n.topicMap[topicName]
	if !ok {
		return nil, ErrTopicNotExist
	}
	topic, ok := topics[part]
	if !ok {
		return nil, ErrTopicNotExist
	}
	return topic, nil
}

func (n *NSQD) deleteTopic(topicName string, part int) {
	n.Lock()
	defer n.Unlock()
	topics, ok := n.topicMap[topicName]
	if !ok {
		return
	}
	delete(topics, part)
	if len(topics) == 0 {
		delete(n.topicMap, topicName)
	}
}

// this just close the topic and remove from map, but keep the data for later.
func (n *NSQD) CloseExistingTopic(topicName string, partition int) error {
	topic, err := n.GetExistingTopic(topicName, partition)
	if err != nil {
		return err
	}
	// delete empties all channels and the topic itself before closing
	// (so that we dont leave any messages around)
	//
	// we do this before removing the topic from map below (with no lock)
	// so that any incoming writes will error and not create a new topic
	// to enforce ordering
	topic.Close()

	n.deleteTopic(topicName, partition)
	return nil
}

func (n *NSQD) ForceDeleteTopicData(name string, partition int) error {
	topic, err := n.GetExistingTopic(name, partition)
	if err != nil {
		// not exist, create temp for check
		n.Lock()
		loopFunc := n.pubLoopFunc
		n.Unlock()
		deleteCallback := func(t *Topic) {
			// do nothing
		}
		topic = NewTopic(name, partition, n.GetOpts(), deleteCallback, 1, n.Notify, loopFunc)
		if topic == nil {
			return errors.New("failed to init new topic")
		}
	}
	topic.Delete()
	n.deleteTopic(name, partition)
	return nil
}

func (n *NSQD) CheckMagicCode(name string, partition int, code int64, tryFix bool) (string, error) {
	localTopic, err := n.GetExistingTopic(name, partition)
	if err != nil {
		// not exist, create temp for check
		n.Lock()
		loopFunc := n.pubLoopFunc
		n.Unlock()
		deleteCallback := func(t *Topic) {
		}
		localTopic = NewTopic(name, partition, n.GetOpts(), deleteCallback, 1, n.Notify, loopFunc)
		if localTopic == nil {
			return "", errors.New("failed to init new topic")
		}
		defer localTopic.Close()
	}
	magicCodeWrong := false
	localMagicCode := localTopic.GetMagicCode()
	if localMagicCode != 0 && localMagicCode != code {
		nsqLog.Infof("local topic %v magic code is not matching with the current:%v-%v", localTopic.GetFullName(), localTopic.GetMagicCode(), code)
		magicCodeWrong = true
	}
	if magicCodeWrong {
		if !tryFix {
			return "", errors.New("magic code is wrong")
		} else {
			nsqLog.Warningf("local topic %v removed for wrong magic code: %v vs %v", localTopic.GetFullName(), localTopic.GetMagicCode(), code)
			n.deleteTopic(localTopic.GetTopicName(), localTopic.GetTopicPart())
			localTopic.Close()
			removedPath, err := localTopic.MarkAsRemoved()
			return removedPath, err
		}
	}
	return "", nil
}

func (n *NSQD) SetTopicMagicCode(t *Topic, code int64) error {
	n.magicCodeMutex.Lock()
	err := t.SetMagicCode(code)
	n.magicCodeMutex.Unlock()

	return err
}

// DeleteExistingTopic removes a topic only if it exists
func (n *NSQD) DeleteExistingTopic(topicName string, part int) error {
	topic, err := n.GetExistingTopic(topicName, part)
	if err != nil {
		return err
	}

	// delete empties all channels and the topic itself before closing
	// (so that we dont leave any messages around)
	//
	// we do this before removing the topic from map below (with no lock)
	// so that any incoming writes will error and not create a new topic
	// to enforce ordering
	topic.Delete()

	n.deleteTopic(topicName, part)
	return nil
}

func (n *NSQD) CleanClientPubStats(remote string, protocol string) {
	tmpMap := n.GetTopicMapCopy()
	for _, topics := range tmpMap {
		for _, t := range topics {
			t.detailStats.RemovePubStats(remote, protocol)
		}
	}
}

func (n *NSQD) flushAll(all bool, flushCnt int) {
	match := flushCnt % FLUSH_DISTANCE
	tmpMap := n.GetTopicMapCopy()
	for _, topics := range tmpMap {
		for _, t := range topics {
			if !all && t.IsWriteDisabled() {
				continue
			}
			if !all && (((t.GetTopicPart() + 1) % FLUSH_DISTANCE) != match) {
				continue
			}
			t.ForceFlush()
		}
	}
}

func (n *NSQD) Notify(v interface{}) {
	// since the in-memory metadata is incomplete,
	// should not persist metadata while loading it.
	// nsqd will call `PersistMetadata` it after loading
	persist := atomic.LoadInt32(&n.isLoading) == 0
	n.waitGroup.Wrap(func() {
		// by selecting on exitChan we guarantee that
		// we do not block exit, see issue #123
		select {
		case <-n.exitChan:
		case n.MetaNotifyChan <- v:
			if !persist {
				return
			}
			tmpMap := n.GetTopicMapCopy()
			err := n.PersistMetadata(tmpMap)
			if err != nil {
				nsqLog.LogErrorf("failed to persist metadata - %s", err)
			}
		}
	})
}

// channels returns a flat slice of all channels in all topics
func (n *NSQD) channels() []*Channel {
	var channels []*Channel
	tmpMap := n.GetTopicMapCopy()
	for _, topics := range tmpMap {
		for _, t := range topics {
			t.channelLock.RLock()
			for _, c := range t.channelMap {
				channels = append(channels, c)
			}
			t.channelLock.RUnlock()
		}
	}
	return channels
}

// resizePool adjusts the size of the pool of queueScanWorker goroutines
//
// 	1 <= pool <= min(num * 0.25, QueueScanWorkerPoolMax)
//
func (n *NSQD) resizePool(num int, workCh chan *Channel, responseCh chan bool, closeCh chan int) {
	idealPoolSize := int(float64(num) * 0.25)
	if idealPoolSize < 1 {
		idealPoolSize = 1
	} else if idealPoolSize > n.GetOpts().QueueScanWorkerPoolMax {
		idealPoolSize = n.GetOpts().QueueScanWorkerPoolMax
	}
	for {
		if idealPoolSize == n.poolSize {
			break
		} else if idealPoolSize < n.poolSize {
			// contract
			closeCh <- 1
			n.poolSize--
		} else {
			// expand
			n.waitGroup.Wrap(func() {
				n.queueScanWorker(workCh, responseCh, closeCh)
			})
			n.poolSize++
		}
	}
}

// queueScanWorker receives work (in the form of a channel) from queueScanLoop
// and processes the in-flight queues
func (n *NSQD) queueScanWorker(workCh chan *Channel, responseCh chan bool, closeCh chan int) {
	for {
		select {
		case c := <-workCh:
			now := time.Now().UnixNano()
			dirty := false
			if c.processInFlightQueue(now) {
				dirty = true
			}
			responseCh <- dirty
		case <-closeCh:
			return
		}
	}
}

// queueScanLoop runs in a single goroutine to process in-flight
// . It manages a pool of queueScanWorker (configurable max of
// QueueScanWorkerPoolMax (default: 4)) that process channels concurrently.
//
// It copies Redis's probabilistic expiration algorithm: it wakes up every
// QueueScanInterval (default: 100ms) to select a random QueueScanSelectionCount
// (default: 20) channels from a locally cached list (refreshed every
// QueueScanRefreshInterval (default: 5s)).
//
// If either of the queues had work to do the channel is considered "dirty".
//
// If QueueScanDirtyPercent (default: 25%) of the selected channels were dirty,
// the loop continues without sleep.
func (n *NSQD) queueScanLoop() {
	workCh := make(chan *Channel, n.GetOpts().QueueScanSelectionCount)
	responseCh := make(chan bool, n.GetOpts().QueueScanSelectionCount)
	closeCh := make(chan int)

	workTicker := time.NewTicker(n.GetOpts().QueueScanInterval)
	refreshTicker := time.NewTicker(n.GetOpts().QueueScanRefreshInterval)
	flushTicker := time.NewTicker(n.GetOpts().SyncTimeout)

	channels := n.channels()
	n.resizePool(len(channels), workCh, responseCh, closeCh)
	flushCnt := 0

	for {
		select {
		case <-workTicker.C:
			if len(channels) == 0 {
				continue
			}
		case <-refreshTicker.C:
			channels = n.channels()
			n.resizePool(len(channels), workCh, responseCh, closeCh)
			continue
		case <-flushTicker.C:
			n.flushAll(flushCnt%100 == 0, flushCnt)
			flushCnt++
			continue
		case <-n.exitChan:
			goto exit
		}

		num := n.GetOpts().QueueScanSelectionCount
		if num > len(channels) {
			num = len(channels)
		}

	loop:
		for _, i := range util.UniqRands(num, len(channels)) {
			workCh <- channels[i]
		}

		numDirty := 0
		for i := 0; i < num; i++ {
			if <-responseCh {
				numDirty++
			}
		}

		if float64(numDirty)/float64(num) > n.GetOpts().QueueScanDirtyPercent {
			goto loop
		}
	}

exit:
	nsqLog.Logf("QUEUESCAN: closing")
	close(closeCh)
	workTicker.Stop()
	refreshTicker.Stop()
}
func (n *NSQD) IsAuthEnabled() bool {
	return len(n.GetOpts().AuthHTTPAddresses) != 0
}
