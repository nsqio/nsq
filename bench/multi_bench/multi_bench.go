package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"flag"
	"log"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/absolute8511/glog"
	"github.com/absolute8511/go-nsq"
	"github.com/absolute8511/nsq/internal/app"
	"github.com/absolute8511/nsq/internal/clusterinfo"
	"github.com/absolute8511/nsq/internal/http_api"
	"github.com/absolute8511/nsq/internal/levellogger"
	"github.com/spaolacci/murmur3"
)

var (
	flagSet = flag.NewFlagSet("bench", flag.ExitOnError)

	runfor        = flagSet.Duration("runfor", 10*time.Second, "duration of time to run")
	sleepfor      = flagSet.Duration("sleepfor", 1*time.Second, " time to sleep between pub")
	lookupAddress = flagSet.String("lookupd-http-address", "127.0.0.1:4161", "<addr>:<port> to connect to nsqlookupd")
	topics        = app.StringArray{}
	size          = flagSet.Int("size", 100, "size of messages")
	batchSize     = flagSet.Int("batch-size", 10, "batch size of messages")
	deadline      = flagSet.String("deadline", "", "deadline to start the benchmark run")
	concurrency   = flagSet.Int("c", 100, "concurrency of goroutine")
	benchCase     = flagSet.String("bench-case", "simple", "which bench should run (simple/benchpub/benchsub/benchdelaysub/checkdata/benchlookup/benchreg/consumeoffset/checkdata2)")
	channelNum    = flagSet.Int("ch_num", 1, "the channel number under each topic")
	trace         = flagSet.Bool("trace", false, "enable the trace of pub and sub")
	ordered       = flagSet.Bool("ordered", false, "enable ordered sub")
	checkMsgSize  = flagSet.Bool("check-size", false, "enable check the body size of sub")
	topicListFile = flagSet.String("topic-list-file", "", "the file that contains one topic each line")
	maxDelaySecs  = flagSet.Int("max-delaysec", 30, "the max delayed message in second")
)

func getPartitionID(msgID nsq.NewMessageID) string {
	return strconv.Itoa(int(uint64(msgID) >> 50))
}

type pubResp struct {
	id      nsq.NewMessageID
	offset  uint64
	rawSize uint32
}

type dumpStat struct {
	msg     *nsq.Message
	recvCnt int64
}

var totalMsgCount int64
var totalSubMsgCount int64
var totalDumpCount int64
var currentMsgCount int64
var totalErrCount int64
var config *nsq.Config
var dumpCheck map[string]map[uint64]dumpStat
var orderCheck map[string]pubResp
var pubRespCheck map[string]map[uint64]pubResp
var mutex sync.Mutex
var topicMutex map[string]*sync.Mutex
var traceIDWaitingList map[string]map[uint64]*nsq.Message
var pubTraceFailedList map[string]map[uint64]int64
var maxDelayTs int64
var myRand = rand.New(rand.NewSource(time.Now().Unix()))

type ByMsgOffset []*nsq.Message

func (self ByMsgOffset) Len() int {
	return len(self)
}

func (self ByMsgOffset) Swap(i, j int) {
	self[i], self[j] = self[j], self[i]
}

func (self ByMsgOffset) Less(i, j int) bool {
	return self[i].Offset < self[j].Offset
}

type ByPubOffset []pubResp

func (self ByPubOffset) Len() int {
	return len(self)
}

func (self ByPubOffset) Swap(i, j int) {
	self[i], self[j] = self[j], self[i]
}

func (self ByPubOffset) Less(i, j int) bool {
	return self[i].offset < self[j].offset
}

func init() {
	flagSet.Var(&topics, "bench-topics", "the topic list for benchmark [t1, t2, t3]")
}

func startBenchPub(msg []byte, batch [][]byte) {
	var wg sync.WaitGroup
	config.EnableTrace = *trace
	config.WriteTimeout = 0
	config.ReadTimeout = 0
	pubMgr, err := nsq.NewTopicProducerMgr(topics, config)
	if err != nil {
		log.Printf("init error : %v", err)
		return
	}
	pubMgr.SetLogger(log.New(os.Stderr, "", log.LstdFlags), nsq.LogLevelInfo)
	err = pubMgr.ConnectToNSQLookupd(*lookupAddress)
	if err != nil {
		log.Printf("lookup connect error : %v", err)
		return
	}

	for _, t := range topics {
		if *trace {
			var id nsq.NewMessageID
			var offset uint64
			var rawSize uint32
			if *ordered {
				id, offset, rawSize, err = pubMgr.PublishOrdered(t, msg, msg)
			} else {
				id, offset, rawSize, err = pubMgr.PublishAndTrace(t, 0, msg)
			}
			log.Printf("topic %v pub trace : %v, %v, %v", t, id, offset, rawSize)
		} else {
			err = pubMgr.Publish(t, msg)
		}
		if err != nil {
			log.Printf("topic pub error : %v", err)
			return
		}
	}
	goChan := make(chan int)
	rdyChan := make(chan int)
	for j := 0; j < *concurrency; j++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			pubWorker(*runfor, pubMgr, topics[j%len(topics)], *batchSize, batch, rdyChan, goChan, false)
		}()
		<-rdyChan
	}

	if *deadline != "" {
		t, err := time.Parse("2006-01-02 15:04:05", *deadline)
		if err != nil {
			log.Fatal(err)
		}
		d := t.Sub(time.Now())
		log.Printf("sleeping until %s (%s)", t, d)
		time.Sleep(d)
	}

	start := time.Now()
	close(goChan)
	go func() {
		prevMsgCount := int64(0)
		prevStart := start
		for {
			time.Sleep(time.Second * 5)
			end := time.Now()
			duration := end.Sub(prevStart)
			currentTmc := atomic.LoadInt64(&currentMsgCount)
			tmc := currentTmc - prevMsgCount
			prevMsgCount = currentTmc
			prevStart = time.Now()
			log.Printf("duration: %s - %.03fmb/s - %.03fops/s - %.03fus/op\n",
				duration,
				float64(tmc*int64(*size))/duration.Seconds()/1024/1024,
				float64(tmc)/duration.Seconds(),
				float64(duration/time.Microsecond)/(float64(tmc)+0.01))

		}

	}()
	wg.Wait()
	end := time.Now()
	duration := end.Sub(start)
	tmc := atomic.LoadInt64(&totalMsgCount)
	log.Printf("duration: %s - %.03fmb/s - %.03fops/s - %.03fus/op\n",
		duration,
		float64(tmc*int64(*size))/duration.Seconds()/1024/1024,
		float64(tmc)/duration.Seconds(),
		float64(duration/time.Microsecond)/(float64(tmc)+0.01))

	log.Printf("total count: %v, total error : %v\n", tmc, atomic.LoadInt64(&totalErrCount))
}

func startBenchSub() {
	var wg sync.WaitGroup

	log.SetPrefix("[bench_reader] ")

	config.WriteTimeout = 0
	config.ReadTimeout = 0

	quitChan := make(chan int)
	goChan := make(chan int)
	rdyChan := make(chan int)
	for j := 0; j < *concurrency; j++ {
		for chIndex := 0; chIndex < *channelNum; chIndex++ {
			wg.Add(1)
			go func(id int, topic string, chSuffix string) {
				subWorker(quitChan, *runfor, *lookupAddress, topic, topic+"_ch"+chSuffix, rdyChan, goChan, id)
				wg.Done()
			}(j, topics[j%len(topics)], strconv.Itoa(chIndex))
			<-rdyChan
		}
	}

	if *deadline != "" {
		t, err := time.Parse("2006-01-02 15:04:05", *deadline)
		if err != nil {
			log.Fatal(err)
		}
		d := t.Sub(time.Now())
		log.Printf("sleeping until %s (%s)", t, d)
		time.Sleep(d)
	}

	start := time.Now()
	close(goChan)
	close(quitChan)
	go func() {
		prevMsgCount := int64(0)
		prevStart := start
		for {
			time.Sleep(time.Second * 5)
			end := time.Now()
			duration := end.Sub(prevStart)
			currentTmc := atomic.LoadInt64(&totalSubMsgCount)
			tmc := currentTmc - prevMsgCount
			prevMsgCount = currentTmc
			prevStart = time.Now()
			log.Printf("duration: %s - %.03fmb/s - %.03fops/s - %.03fus/op\n",
				duration,
				float64(tmc*int64(*size))/duration.Seconds()/1024/1024,
				float64(tmc)/duration.Seconds(),
				float64(duration/time.Microsecond)/(float64(tmc)+0.01))

		}

	}()

	wg.Wait()
	end := time.Now()
	duration := end.Sub(start)
	tmc := atomic.LoadInt64(&totalSubMsgCount)
	log.Printf("duration: %s - %.03fmb/s - %.03fops/s - %.03fus/op",
		duration,
		float64(tmc*int64(*size))/duration.Seconds()/1024/1024,
		float64(tmc)/duration.Seconds(),
		float64(duration/time.Microsecond)/(float64(tmc)+0.01))

	log.Printf("total count: %v\n", tmc)
}

func startSimpleTest(msg []byte, batch [][]byte) {
	// lookup operation
	cluster := clusterinfo.New(log.New(os.Stderr, "", log.LstdFlags), http_api.NewClient(nil))
	ver, err := cluster.GetVersion(*lookupAddress)
	if err != nil {
		log.Printf("failed to get lookup version: %v\n", err)
	} else {
		log.Printf("get info: %v\n", ver)
	}
	tmpList := make([]string, 0)
	tmpList = append(tmpList, *lookupAddress)
	currentTopics, err := cluster.GetLookupdTopics(tmpList)
	if err != nil {
		log.Printf("failed : %v\n", err)
	} else {
		log.Printf("return: %v\n", currentTopics)
	}
	if len(currentTopics) == 0 {
		return
	}
	chs, err := cluster.GetLookupdTopicChannels(currentTopics[0], tmpList)
	if err != nil {
		log.Printf("failed : %v\n", err)
	} else {
		log.Printf("return: %v\n", chs)
	}
	allNodes, err := cluster.GetLookupdProducers(tmpList)
	if err != nil {
		log.Printf("failed : %v\n", err)
	} else {
		log.Printf("return: %v\n", allNodes)
	}
	producers, partitionProducers, err := cluster.GetLookupdTopicProducers(currentTopics[0], tmpList)

	if err != nil {
		log.Printf("failed : %v\n", err)
	} else {
		log.Printf("return: %v, %v\n", producers, partitionProducers)
	}
	// nsqd basic tcp operation
}

func startCheckData2() {
	var wg sync.WaitGroup
	config.EnableTrace = *trace
	pubMgr, err := nsq.NewTopicProducerMgr(topics, config)
	if err != nil {
		log.Printf("init error : %v", err)
		return
	}
	pubMgr.SetLogger(log.New(os.Stderr, "", log.LstdFlags), nsq.LogLevelInfo)
	err = pubMgr.ConnectToNSQLookupd(*lookupAddress)
	if err != nil {
		log.Printf("lookup connect error : %v", err)
		return
	}

	pubIDList := make(map[string]*int64)
	// received max continuous trace id
	subReceivedMaxTraceIDList := make(map[string]*int64)
	for _, t := range topics {
		init := time.Now().UnixNano()
		pubIDList[t] = &init
		subInit := init
		subReceivedMaxTraceIDList[t] = &subInit
		topicMutex[t] = &sync.Mutex{}
	}

	for _, t := range topics {
		v := pubIDList[t]
		atomic.AddInt64(v, 1)
		data := make([]byte, 8)
		binary.BigEndian.PutUint64(data, uint64(*v))
		if *trace {
			var id nsq.NewMessageID
			var offset uint64
			var rawSize uint32
			if *ordered {
				id, offset, rawSize, err = pubMgr.PublishOrdered(t, data, data)
			} else {
				id, offset, rawSize, err = pubMgr.PublishAndTrace(t, uint64(*v), data)
			}
			log.Printf("topic %v pub trace : %v, %v, %v", t, id, offset, rawSize)
		} else {
			err = pubMgr.Publish(t, data)
		}
		if err != nil {
			log.Printf("topic pub error : %v", err)
			return
		}
		atomic.AddInt64(&totalMsgCount, 1)
		atomic.AddInt64(&currentMsgCount, 1)
	}

	goChan := make(chan int)
	rdyChan := make(chan int)
	for j := 0; j < *concurrency; j++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			mutex.Lock()
			curTopic := topics[j%len(topics)]
			counter := pubIDList[curTopic]
			mutex.Unlock()
			pubWorker2(*runfor, pubMgr, curTopic, counter, rdyChan, goChan)
		}()
		<-rdyChan
	}

	quitChan := make(chan int)
	for j := 0; j < *concurrency; j++ {
		for chIndex := 0; chIndex < *channelNum; chIndex++ {
			wg.Add(1)
			go func(id int, topic string, chSuffix string) {
				mutex.Lock()
				subCounter := subReceivedMaxTraceIDList[topic]
				subLocker := topicMutex[topic]
				subTraceWaiting, ok := traceIDWaitingList[topic]
				if !ok {
					subTraceWaiting = make(map[uint64]*nsq.Message)
					traceIDWaitingList[topic] = subTraceWaiting
				}
				mutex.Unlock()
				subWorker2(quitChan, *runfor, *lookupAddress, topic, topic+"_ch"+chSuffix,
					subCounter, subTraceWaiting, subLocker, rdyChan, goChan, id)
				wg.Done()
			}(j, topics[j%len(topics)], strconv.Itoa(chIndex))
			<-rdyChan
		}
	}

	close(goChan)
	startTime := time.Now()

	go func() {
		prev := int64(0)
		prevSub := int64(0)
		equalTimes := 0
		for {
			time.Sleep(time.Second * 5)
			currentTmc := atomic.LoadInt64(&currentMsgCount)
			totalSub := atomic.LoadInt64(&totalSubMsgCount)
			log.Printf("pub total %v - sub total %v\n",
				currentTmc,
				totalSub)
			if prev == currentTmc && prevSub == totalSub && time.Since(startTime) > *runfor {
				equalTimes++
				if totalSub >= currentTmc && equalTimes > 3 {
					close(quitChan)
					return
				} else if equalTimes > 10 {
					close(quitChan)
					return
				}
			} else {
				equalTimes = 0
			}
			prev = currentTmc
			prevSub = totalSub
		}
	}()

	wg.Wait()

	log.Printf("pub total %v - sub total %v , pub err: %v\n",
		atomic.LoadInt64(&totalMsgCount),
		atomic.LoadInt64(&totalSubMsgCount),
		atomic.LoadInt64(&totalErrCount))

	for topicName, counter := range pubIDList {
		log.Printf("topic %v pub count to : %v \n", topicName, *counter)
		subCounter := subReceivedMaxTraceIDList[topicName]
		if *subCounter == *counter {
			log.Printf("sub max trace id is equal with pub\n")
		} else {
			log.Printf("!!! topic: %v sub max trace id is not equal: %v\n", topicName, *subCounter)
		}
	}

	for topicName, waitingList := range traceIDWaitingList {
		if len(waitingList) == 0 {
			continue
		}
		log.Printf("topic: %v sub waiting list: %v\n", topicName, len(waitingList))
		for traceID, _ := range waitingList {
			log.Printf("%v, ", traceID)
		}
		log.Printf("\n")
	}
}

// check the pub data and sub data is the same at any time.
func startCheckData(msg []byte, batch [][]byte, testDelay bool) {
	var wg sync.WaitGroup
	config.EnableTrace = *trace
	pubMgr, err := nsq.NewTopicProducerMgr(topics, config)
	if err != nil {
		log.Printf("init error : %v", err)
		return
	}
	pubMgr.SetLogger(log.New(os.Stderr, "", log.LstdFlags), nsq.LogLevelInfo)
	err = pubMgr.ConnectToNSQLookupd(*lookupAddress)
	if err != nil {
		log.Printf("lookup connect error : %v", err)
		return
	}

	for _, t := range topics {
		if *trace {
			var id nsq.NewMessageID
			var offset uint64
			var rawSize uint32
			if *ordered {
				id, offset, rawSize, err = pubMgr.PublishOrdered(t, msg, msg)
			} else {
				id, offset, rawSize, err = pubMgr.PublishAndTrace(t, 0, msg)
			}
			log.Printf("topic %v pub trace : %v, %v, %v", t, id, offset, rawSize)
		} else {
			err = pubMgr.Publish(t, msg)
		}
		if err != nil {
			log.Printf("topic pub error : %v", err)
			return
		}
		atomic.AddInt64(&totalMsgCount, 1)
		atomic.AddInt64(&currentMsgCount, 1)
	}

	goChan := make(chan int)
	rdyChan := make(chan int)
	for j := 0; j < *concurrency; j++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			pubWorker(*runfor, pubMgr, topics[j%len(topics)], *batchSize, batch, rdyChan, goChan, testDelay)
		}()
		<-rdyChan
	}

	quitChan := make(chan int)
	for j := 0; j < *concurrency; j++ {
		for chIndex := 0; chIndex < *channelNum; chIndex++ {
			wg.Add(1)
			go func(id int, topic string, chSuffix string) {
				subWorker(quitChan, *runfor, *lookupAddress, topic, topic+"_ch"+chSuffix, rdyChan, goChan, id)
				wg.Done()
			}(j, topics[j%len(topics)], strconv.Itoa(chIndex))
			<-rdyChan
		}
	}

	close(goChan)
	startTime := time.Now()

	go func() {
		prev := int64(0)
		prevSub := int64(0)
		equalTimes := 0
		for {
			time.Sleep(time.Second * 5)
			currentTmc := atomic.LoadInt64(&currentMsgCount)
			totalSub := atomic.LoadInt64(&totalSubMsgCount)
			log.Printf("pub total %v - sub total %v, dump: %v \n",
				currentTmc,
				totalSub, atomic.LoadInt64(&totalDumpCount))
			if prev == currentTmc && prevSub == totalSub && time.Since(startTime) > *runfor {
				equalTimes++
				if *benchCase == "benchdelaysub" {
					if totalSub >= atomic.LoadInt64(&totalMsgCount) && totalSub >= currentTmc && equalTimes > 3 {
						close(quitChan)
						return
					}
					if time.Now().Unix() <= atomic.LoadInt64(&maxDelayTs)+10 {
						if totalSub < atomic.LoadInt64(&totalMsgCount) {
							equalTimes = 0
						}
						continue
					}
				}
				if totalSub >= currentTmc && equalTimes > 3 {
					close(quitChan)
					return
				} else if equalTimes > 10 {
					close(quitChan)
					return
				}
			} else {
				equalTimes = 0
			}
			prev = currentTmc
			prevSub = totalSub
		}
	}()

	wg.Wait()

	log.Printf("pub total %v - sub total %v, dump: %v, err: %v \n",
		atomic.LoadInt64(&totalMsgCount),
		atomic.LoadInt64(&totalSubMsgCount),
		atomic.LoadInt64(&totalDumpCount),
		atomic.LoadInt64(&totalErrCount))

	for topicName, tdump := range dumpCheck {
		log.Printf("topic %v count: %v \n", topicName, len(tdump))
		topicMsgs := make([]*nsq.Message, 0, len(tdump))
		dump2List := make([]dumpStat, 0, 10)
		dump3List := make([]dumpStat, 0, 10)
		dump4MoreList := make([]dumpStat, 0, 10)
		for _, d := range tdump {
			topicMsgs = append(topicMsgs, d.msg)
			if d.recvCnt == 2 {
				if len(dump2List) < 10 {
					dump2List = append(dump2List, d)
				}
			} else if d.recvCnt == 3 {
				if len(dump3List) < 10 {
					dump3List = append(dump3List, d)
				}
			} else if d.recvCnt > 3 {
				if len(dump4MoreList) < 10 {
					dump4MoreList = append(dump4MoreList, d)
				}
			}
		}
		log.Printf("topic %v dump 2 msgs: ", topicName)
		for _, d := range dump2List {
			log.Printf("%v (%v, %v), ", d.msg.ID, d.msg.Offset, d.msg.Attempts)
		}
		log.Printf("\n")
		log.Printf("topic %v dump 3 msgs: ", topicName)
		for _, d := range dump3List {
			log.Printf("%v (%v, %v), ", d.msg.ID, d.msg.Offset, d.msg.Attempts)
		}
		log.Printf("\n")
		log.Printf("topic %v dump 4 and more msgs: ", topicName)
		for _, d := range dump4MoreList {
			log.Printf("%v (%v, %v vs %v), ", d.msg.ID, d.msg.Offset, d.msg.Attempts, d.recvCnt)
		}
		log.Printf("\n")

		sort.Sort(ByMsgOffset(topicMsgs))
		receivedOffsets := make([]uint64, 0)
		fragmentNum := 1
		for _, msg := range topicMsgs {
			if fragmentNum > 10 {
				break
			}
			if len(receivedOffsets) == 0 {
				log.Printf("a new fragment of queue: begin from %v, %v ", msg.Offset, msg.ID)
				receivedOffsets = append(receivedOffsets, msg.Offset)
				receivedOffsets = append(receivedOffsets, msg.Offset+uint64(msg.RawSize))
			} else if receivedOffsets[len(receivedOffsets)-1] == msg.Offset {
				receivedOffsets[len(receivedOffsets)-1] = msg.Offset + uint64(msg.RawSize)
			} else {
				log.Printf("current fragment of queue end at %v ", receivedOffsets[len(receivedOffsets)-1])
				receivedOffsets = append(receivedOffsets, msg.Offset)
				receivedOffsets = append(receivedOffsets, msg.Offset+uint64(msg.RawSize))
				log.Printf("a new fragment of queue: begin from %v, %v ", msg.Offset, msg.ID)
				fragmentNum++
			}
		}
		if len(receivedOffsets) != 0 {
			log.Printf("last fragment of queue end at %v ", receivedOffsets[len(receivedOffsets)-1])
		}
	}
	for topicName, tpubs := range pubRespCheck {
		log.Printf("topic: %v pub count: %v", topicName, len(tpubs))
		pubSortList := make([]pubResp, 0, len(tpubs))
		for _, r := range tpubs {
			pubSortList = append(pubSortList, r)
		}
		sort.Sort(ByPubOffset(pubSortList))
		pubOffsets := make([]uint64, 0)
		fragmentNum := 1
		for _, r := range pubSortList {
			if fragmentNum > 10 {
				break
			}
			if len(pubOffsets) == 0 {
				log.Printf("a new pub fragment of queue: begin from %v, %v ", r.offset, r.id)
				pubOffsets = append(pubOffsets, r.offset)
				pubOffsets = append(pubOffsets, r.offset+uint64(r.rawSize))
			} else if pubOffsets[len(pubOffsets)-1] == r.offset {
				pubOffsets[len(pubOffsets)-1] = r.offset + uint64(r.rawSize)
			} else {
				log.Printf("current pub fragment of queue end at %v ", pubOffsets[len(pubOffsets)-1])
				pubOffsets = append(pubOffsets, r.offset)
				pubOffsets = append(pubOffsets, r.offset+uint64(r.rawSize))
				log.Printf("a new pub fragment of queue: begin from %v, %v ", r.offset, r.id)
				fragmentNum++
			}
		}
		if len(pubOffsets) != 0 {
			log.Printf("last pub fragment of queue end at %v ", pubOffsets[len(pubOffsets)-1])
		}
	}
}

func startBenchLookup() {
	// lookup operation
	var wg sync.WaitGroup
	start := time.Now()
	eachCnt := *size * 10
	for j := 0; j < *concurrency; j++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cluster := clusterinfo.New(nil, http_api.NewClient(nil))
			tmpList := make([]string, 0)
			tmpList = append(tmpList, *lookupAddress)
			currentTopics, err := cluster.GetLookupdTopics(tmpList)
			if err != nil {
				log.Printf("failed : %v\n", err)
				return
			} else {
				log.Printf("return: %v\n", currentTopics)
			}
			cnt := eachCnt
			for cnt > 0 {
				cnt--
				for _, t := range currentTopics {
					_, _, err := cluster.GetLookupdTopicProducers(t, tmpList)
					if err != nil {
						log.Printf("failed : %v\n", err)
					}
				}
				time.Sleep(time.Millisecond)
			}
		}()
	}
	wg.Wait()
	runSec := time.Now().Sub(start).Seconds() + 1
	log.Printf(" %v request done in %v seconds, qps: %v\n", *concurrency*eachCnt, runSec,
		float64(*concurrency*eachCnt)/runSec)
}

func connectCallback(id string, hostname string) func(*clusterinfo.LookupPeer) {
	return func(lp *clusterinfo.LookupPeer) {
		ci := make(map[string]interface{})
		ci["id"] = id
		ci["version"] = "test.ver"
		ci["tcp_port"] = 1111
		ci["http_port"] = 1112
		ci["hostname"] = hostname
		ci["broadcast_address"] = "127.0.0.1"

		cmd, _ := nsq.Identify(ci)
		lp.Command(cmd)
	}
}

func startBenchLookupRegUnreg() {
	var wg sync.WaitGroup
	start := time.Now()
	eachCnt := *size * 10
	hostname, _ := os.Hostname()
	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			lookupPeer := clusterinfo.NewLookupPeer(*lookupAddress, 1024*1024*10, &levellogger.GLogger{},
				connectCallback("bench_reg_"+strconv.Itoa(i), hostname))
			lookupPeer.Command(nil) // start the connection

			cmd := nsq.Ping()
			resp, err := lookupPeer.Command(cmd)
			if err != nil {
				log.Printf("ping lookup error : %v\n", err)
				return
			} else {
				log.Printf("ping lookup  : %v\n", resp)
			}
			cnt := eachCnt
			for cnt > 0 {
				cnt--
				for _, t := range topics {
					cmd = nsq.UnRegister(t,
						strconv.Itoa(i), "")
					lookupPeer.Command(cmd)
					cmd = nsq.Register(t,
						strconv.Itoa(i), "")
					lookupPeer.Command(cmd)
					for ch := 0; ch < 10; ch++ {
						cmd = nsq.UnRegister(t,
							strconv.Itoa(i), "ch"+strconv.Itoa(ch))
						lookupPeer.Command(cmd)
						cmd = nsq.Register(t,
							strconv.Itoa(i), "ch"+strconv.Itoa(ch))
						lookupPeer.Command(cmd)
					}
				}
			}
		}()
	}
	wg.Wait()
	runSec := time.Now().Sub(start).Seconds() + 1
	log.Printf(" %v request done in %v seconds, qps: %v\n", *concurrency*eachCnt, runSec,
		float64(*concurrency*eachCnt)/runSec)
}

func startCheckSetConsumerOffset() {
	config.EnableTrace = true
	// offset count, timestamp
	pubMgr, err := nsq.NewTopicProducerMgr(topics, config)
	if err != nil {
		log.Printf("init error : %v", err)
		return
	}
	pubMgr.SetLogger(log.New(os.Stderr, "", log.LstdFlags), nsq.LogLevelInfo)
	err = pubMgr.ConnectToNSQLookupd(*lookupAddress)
	if err != nil {
		log.Printf("lookup connect error : %v", err)
		return
	}

	type partOffset struct {
		offsetValue int64
		pid         string
	}
	topicOffsets := make(map[string]partOffset)
	topicTsOffsets := make(map[string]partOffset)
	for _, t := range topics {
		for i := 0; i < 10; i++ {
			msg := []byte(strconv.Itoa(int(time.Now().UnixNano())))
			var id nsq.NewMessageID
			var offset uint64
			var rawSize uint32
			if *ordered {
				id, offset, rawSize, err = pubMgr.PublishOrdered(t, msg, msg)
			} else {
				id, offset, rawSize, err = pubMgr.PublishAndTrace(t, 0, msg)
			}
			if err != nil {
				log.Printf("topic pub error : %v", err)
				return
			}
			mid := uint64(id)
			pidStr := getPartitionID(nsq.NewMessageID(mid))
			if i == 6 {
				p := &partOffset{
					offsetValue: int64(offset),
					pid:         pidStr,
				}
				topicOffsets[t] = *p
				log.Printf("check consume offset at : %v, %v", mid, p)
				p.offsetValue = time.Now().Unix()
				topicTsOffsets[t] = *p
				log.Printf("check consume timestamp at : %v, %v", mid, p)
			}
			log.Printf("topic %v pub to partition %v trace : %v, %v, %v", t, pidStr, id, offset, rawSize)
			time.Sleep(time.Second)
		}
	}
	for t, queueOffset := range topicOffsets {
		var offset nsq.ConsumeOffset
		offset.SetVirtualQueueOffset(queueOffset.offsetValue)
		consumer, err := nsq.NewConsumer(t, "offset_ch", config)
		if err != nil {
			log.Printf("init consumer error: %v", err)
			return
		}
		pid, _ := strconv.Atoi(queueOffset.pid)
		consumer.SetConsumeOffset(pid, offset)
		consumer.SetLogger(log.New(os.Stderr, "", log.LstdFlags), nsq.LogLevelInfo)
		consumer.AddHandler(&consumeOffsetHandler{t, false, offset, queueOffset.pid, 0})
		consumer.ConnectToNSQLookupd(*lookupAddress)
		time.Sleep(time.Second * 5)
		consumer.Stop()
		<-consumer.StopChan
		time.Sleep(time.Second * 5)
	}
	for t, tsOffset := range topicTsOffsets {
		var offset nsq.ConsumeOffset
		offset.SetTime(tsOffset.offsetValue)
		consumer, err := nsq.NewConsumer(t, "offset_ch", config)
		if err != nil {
			log.Printf("init consumer error: %v", err)
			return
		}
		pid, _ := strconv.Atoi(tsOffset.pid)
		consumer.SetConsumeOffset(pid, offset)
		consumer.SetLogger(log.New(os.Stderr, "", log.LstdFlags), nsq.LogLevelInfo)
		consumer.AddHandler(&consumeOffsetHandler{t, false, offset, tsOffset.pid, 0})
		consumer.ConnectToNSQLookupd(*lookupAddress)
		time.Sleep(time.Second * 5)
		log.Printf("stopping consumer")
		consumer.Stop()
		<-consumer.StopChan
		time.Sleep(time.Second * 5)
	}
	log.Printf("exiting")
}

type consumeOffsetHandler struct {
	topic          string
	firstReceived  bool
	expectedOffset nsq.ConsumeOffset
	expectedPidStr string
	received       int
}

func (c *consumeOffsetHandler) HandleMessage(message *nsq.Message) error {
	mid := uint64(nsq.GetNewMessageID(message.ID[:8]))
	pidStr := getPartitionID(nsq.NewMessageID(mid))
	if len(message.Body) <= 0 {
		log.Printf("got empty message body: %v", message)
	}
	c.received++
	if !c.firstReceived && pidStr == c.expectedPidStr {
		if nsq.OffsetVirtualQueueType == c.expectedOffset.OffsetType {
			if int64(message.Offset) != c.expectedOffset.OffsetValue {
				log.Printf("not expected queue offset: %v, %v", message.Offset, c.expectedOffset)
			}
		} else if nsq.OffsetTimestampType == c.expectedOffset.OffsetType {
			diff := message.Timestamp - c.expectedOffset.OffsetValue*1e9
			if diff > 1*1e9 || diff < 0 {
				log.Printf("not expected timestamp: %v, %v", message.Timestamp, c.expectedOffset)
			}
		}
		log.Printf("got the first message : %v", message)
		c.firstReceived = true
	} else {
		if c.received < 10 {
			log.Printf("got later message : %v", message)
		}
	}
	return nil
}

func main() {
	glog.InitWithFlag(flagSet)
	flagSet.Parse(os.Args[1:])
	glog.StartWorker(time.Second)
	if *ordered {
		*trace = true
		*batchSize = 1
	}
	config = nsq.NewConfig()
	config.MsgTimeout = time.Second * time.Duration(10*(*channelNum))
	if config.MsgTimeout >= time.Second*200 {
		config.MsgTimeout = time.Second * 200
	}
	config.DefaultRequeueDelay = time.Second * 30
	config.MaxRequeueDelay = time.Minute * 60
	config.MaxBackoffDuration = time.Second * 3
	config.MaxAttempts = 65534
	config.MaxInFlight = 20
	config.EnableTrace = *trace
	config.EnableOrdered = *ordered
	config.PubStrategy = nsq.PubRR
	if config.EnableOrdered {
		config.PubStrategy = nsq.PubIDHash
		config.Hasher = murmur3.New32()
	}
	log.Printf("check test flag: order: %v, pub strategy: %v, config: %v", *ordered, config.PubStrategy, config)

	log.SetPrefix("[bench_writer] ")
	dumpCheck = make(map[string]map[uint64]dumpStat, 5)
	pubRespCheck = make(map[string]map[uint64]pubResp, 5)
	orderCheck = make(map[string]pubResp)
	traceIDWaitingList = make(map[string]map[uint64]*nsq.Message, 5)
	pubTraceFailedList = make(map[string]map[uint64]int64)

	topicMutex = make(map[string]*sync.Mutex)
	if *topicListFile != "" {
		f, err := os.Open(*topicListFile)
		if err != nil {
			log.Printf("load topic list file error: %v", err)
		} else {
			scanner := bufio.NewScanner(f)
			for scanner.Scan() {
				line := scanner.Text()
				line = strings.TrimSpace(line)
				topics = append(topics, line)
			}
		}
	}
	log.Printf("testing topic list: %v", topics)

	msg := make([]byte, *size)
	batch := make([][]byte, *batchSize)
	for i := range batch {
		batch[i] = msg
	}

	if *benchCase == "simple" {
		startSimpleTest(msg, batch)
	} else if *benchCase == "benchpub" {
		startBenchPub(msg, batch)
	} else if *benchCase == "benchsub" {
		startBenchSub()
	} else if *benchCase == "checkdata" {
		startCheckData(msg, batch, false)
	} else if *benchCase == "benchlookup" {
		startBenchLookup()
	} else if *benchCase == "benchreg" {
		startBenchLookupRegUnreg()
	} else if *benchCase == "consumeoffset" {
		startCheckSetConsumerOffset()
	} else if *benchCase == "checkdata2" {
		startCheckData2()
	} else if *benchCase == "benchdelaysub" {
		startCheckData(msg, batch, true)
	}
}

func pubWorker(td time.Duration, globalPubMgr *nsq.TopicProducerMgr, topicName string, batchSize int,
	batch [][]byte, rdyChan chan int, goChan chan int, testDelay bool) {
	pubMgr, err := nsq.NewTopicProducerMgr(topics, config)
	if err != nil {
		log.Printf("init error : %v", err)
		close(rdyChan)
		return
	}
	pubMgr.SetLogger(log.New(os.Stderr, "", log.LstdFlags), nsq.LogLevelInfo)
	err = pubMgr.ConnectToNSQLookupd(*lookupAddress)
	if err != nil {
		log.Printf("lookup connect error : %v", err)
		close(rdyChan)
		return
	}

	rdyChan <- 1
	<-goChan
	var msgCount int64
	endTime := time.Now().Add(td)
	traceIDs := make([]uint64, len(batch))
	var traceResp pubResp
	for {
		if time.Now().After(endTime) {
			break
		}
		if (*sleepfor).Nanoseconds() > int64(10000) {
			time.Sleep(*sleepfor)
		}

		singleMsg := batch[0]
		if testDelay && atomic.LoadInt64(&currentMsgCount)%171 == 0 {
			delayDuration := time.Second * time.Duration(1+myRand.Intn(*maxDelaySecs))
			delayTs := int(time.Now().Add(delayDuration).Unix())
			if int64(delayTs) > atomic.LoadInt64(&maxDelayTs) {
				atomic.StoreInt64(&maxDelayTs, int64(delayTs))
			}
			singleMsg = []byte("delay-" + strconv.Itoa(delayTs))
			batch[0] = singleMsg
		}
		if *trace {
			if batchSize == 1 {
				if *ordered {
					traceResp.id, traceResp.offset, traceResp.rawSize, err = pubMgr.PublishOrdered(topicName, batch[0], singleMsg)
				} else {
					traceResp.id, traceResp.offset, traceResp.rawSize, err = pubMgr.PublishAndTrace(topicName, traceIDs[0], singleMsg)
				}
			} else {
				traceResp.id, traceResp.offset, traceResp.rawSize, err = pubMgr.MultiPublishAndTrace(topicName, traceIDs, batch)
			}
			if err != nil {
				log.Println("pub error :" + err.Error())
				atomic.AddInt64(&totalErrCount, 1)
				time.Sleep(time.Second)
				continue
			}

			pidStr := getPartitionID(traceResp.id)
			mutex.Lock()
			topicResp, ok := pubRespCheck[topicName+pidStr]
			if !ok {
				topicResp = make(map[uint64]pubResp)
				pubRespCheck[topicName+pidStr] = topicResp
			}
			oldResp, ok := topicResp[uint64(traceResp.id)]
			if ok {
				log.Printf("got the same id with mpub: %v\n", traceResp.id)
				if oldResp != traceResp {
					log.Printf("response not the same old %v, new:%v\n", oldResp, traceResp)
				}
			} else {
				topicResp[uint64(traceResp.id)] = traceResp
			}
			mutex.Unlock()
		} else {
			var err error
			if batchSize == 1 {
				err = pubMgr.Publish(topicName, singleMsg)
			} else {
				err = pubMgr.MultiPublish(topicName, batch)
			}
			if err != nil {
				log.Println("pub error :" + err.Error())
				atomic.AddInt64(&totalErrCount, 1)
				time.Sleep(time.Second)
				continue
			}
		}
		msgCount += int64(len(batch))
		atomic.AddInt64(&currentMsgCount, int64(len(batch)))
		if time.Now().After(endTime) {
			break
		}
	}
	atomic.AddInt64(&totalMsgCount, msgCount)
}

type consumeHandler struct {
	topic      string
	check      bool
	checkDelay bool
}

func (c *consumeHandler) HandleMessage(message *nsq.Message) error {
	mid := uint64(nsq.GetNewMessageID(message.ID[:8]))
	// get partition id from msgid to avoid multi partitions to dump check.
	pidStr := getPartitionID(nsq.NewMessageID(mid))
	if c.check {
		mutex.Lock()
		defer mutex.Unlock()
		topicCheck, ok := dumpCheck[c.topic+pidStr]
		if !ok {
			topicCheck = make(map[uint64]dumpStat)
			dumpCheck[c.topic+pidStr] = topicCheck
		}
		if c.checkDelay {
			if len(message.Body) >= len("delay-") && string(message.Body[:len("delay-")]) == "delay-" {
				delayTs, err := strconv.Atoi(string(message.Body[len("delay-"):]))
				if err == nil {
					now := int(time.Now().Unix())
					if delayTs > now {
						if message.Attempts > 1 {
							log.Printf("got delayed message early: %v (id %v), now: %v", string(message.Body), message.ID, now)
						}
						message.DisableAutoResponse()
						message.RequeueWithoutBackoff(time.Duration(delayTs-now) * time.Second)
						return nil
					} else if now-delayTs > 3 {
						log.Printf("got delayed message too late: %v (id %v), now: %v", string(message.Body), message.ID, now)
					}
				} else {
					log.Printf("got delayed message invalid delay: %v, %v", string(message.Body), err)
				}
			}
		}
		if *ordered {
			lastResp, ok := orderCheck[c.topic+pidStr]
			if ok {
				if mid < uint64(lastResp.id) {
					log.Printf("got message id out of order: %v, %v, %v\n", lastResp, mid, message)
				}
				if mid != uint64(lastResp.id) && message.Offset < lastResp.offset+uint64(lastResp.rawSize) {
					log.Printf("got message offset out of order: %v, %v\n", lastResp, message)
				}
			}
			lastResp.id = nsq.NewMessageID(mid)
			lastResp.offset = message.Offset
			lastResp.rawSize = message.RawSize
			orderCheck[c.topic+pidStr] = lastResp
		}
		if dup, ok := topicCheck[mid]; ok {
			atomic.AddInt64(&totalDumpCount, 1)
			if dup.msg.Offset != message.Offset || dup.msg.RawSize != message.RawSize {
				log.Printf("got dump message with mismatch data: %v, %v\n", dup.msg, message)
			}
			atomic.AddInt64(&dup.recvCnt, 1)
			dup.msg = message
			topicCheck[mid] = dup
			return nil
		}
		topicCheck[mid] = dumpStat{msg: message, recvCnt: 1}
	}
	if len(message.Body) <= 0 {
		log.Printf("got empty message %v\n", message)
	}
	if *checkMsgSize && len(message.Body) != *size {
		log.Printf("got message body size mismatch : %v \n", len(message.Body))
	}
	newCount := atomic.AddInt64(&totalSubMsgCount, 1)
	if newCount < 2 {
		return errors.New("failed by need.")
	}
	return nil
}

func subWorker(quitChan chan int, td time.Duration, lookupAddr string, topic string, channel string,
	rdyChan chan int, goChan chan int, id int) {
	consumer, err := nsq.NewConsumer(topic, channel, config)
	if err != nil {
		panic(err.Error())
	}
	consumer.SetLogger(log.New(os.Stderr, "", log.LstdFlags), nsq.LogLevelInfo)
	if *benchCase == "checkdata" {
		consumer.AddHandler(&consumeHandler{topic, true, false})
	} else if *benchCase == "benchdelaysub" {
		consumer.AddHandler(&consumeHandler{topic, true, true})
	} else {
		consumer.AddHandler(&consumeHandler{topic, false, false})
	}
	rdyChan <- 1
	<-goChan
	done := make(chan struct{})
	go func() {
		time.Sleep(td)
		<-quitChan
		consumer.Stop()
		<-consumer.StopChan
		close(done)
	}()
	consumer.ConnectToNSQLookupd(lookupAddr)
	<-done
}

type consumeTraceIDHandler struct {
	topic           string
	locker          *sync.Mutex
	subIDCounter    *int64
	subTraceWaiting map[uint64]*nsq.Message
	failedList      map[uint64]int64
}

func (c *consumeTraceIDHandler) HandleMessage(message *nsq.Message) error {
	traceID := binary.BigEndian.Uint64(message.ID[8:16])
	c.locker.Lock()
	defer c.locker.Unlock()
	if traceID <= 0 {
		if len(message.Body) != 8 {
			return nil
		}
		traceID = binary.BigEndian.Uint64(message.Body[:8])
		if traceID <= 0 {
			return nil
		}
	} else {
		if !bytes.Equal(message.Body, message.ID[8:16]) {
			log.Printf("the trace id should be equal to body: %v\n", message)
			// ignore this message
			return nil
		}
	}
	c.subTraceWaiting[traceID] = message
	newMaxTraceID := atomic.LoadInt64(c.subIDCounter)
	for fid, _ := range c.failedList {
		// we treat as pub failed id already
		if fid > uint64(newMaxTraceID) {
			c.subTraceWaiting[fid] = nil
		}
	}

	for {
		if _, ok := c.subTraceWaiting[uint64(newMaxTraceID+1)]; ok {
			newMaxTraceID++
			delete(c.subTraceWaiting, uint64(newMaxTraceID))
		} else {
			if _, ok := c.failedList[uint64(newMaxTraceID+1)]; ok {
				newMaxTraceID++
				continue
			}
			break
		}
	}
	for traceID, _ := range c.subTraceWaiting {
		if traceID <= uint64(newMaxTraceID) {
			delete(c.subTraceWaiting, traceID)
		}
	}
	atomic.AddInt64(&totalSubMsgCount, 1)
	atomic.StoreInt64(c.subIDCounter, newMaxTraceID)
	return nil
}

func subWorker2(quitChan chan int, td time.Duration, lookupAddr string, topic string, channel string,
	subIDCounter *int64, subTraceWaiting map[uint64]*nsq.Message, locker *sync.Mutex, rdyChan chan int, goChan chan int, id int) {
	consumer, err := nsq.NewConsumer(topic, channel, config)
	if err != nil {
		panic(err.Error())
	}
	mutex.Lock()
	failedList, ok := pubTraceFailedList[topic]
	if !ok {
		failedList = make(map[uint64]int64)
		pubTraceFailedList[topic] = failedList
	}
	mutex.Unlock()

	consumer.SetLogger(log.New(os.Stderr, "", log.LstdFlags), nsq.LogLevelInfo)
	consumer.AddHandler(&consumeTraceIDHandler{topic, locker, subIDCounter, subTraceWaiting, failedList})
	rdyChan <- 1
	<-goChan
	done := make(chan struct{})
	go func() {
		time.Sleep(td)
		<-quitChan
		consumer.Stop()
		<-consumer.StopChan
		close(done)
	}()
	consumer.ConnectToNSQLookupd(lookupAddr)
	<-done
}

func pubWorker2(td time.Duration, globalPubMgr *nsq.TopicProducerMgr, topicName string, pubIDCounter *int64, rdyChan chan int, goChan chan int) {
	pubMgr, err := nsq.NewTopicProducerMgr(topics, config)
	if err != nil {
		log.Printf("init pub mgr error : %v", err)
		close(rdyChan)
		return
	}
	pubMgr.SetLogger(log.New(os.Stderr, "", log.LstdFlags), nsq.LogLevelInfo)
	err = pubMgr.ConnectToNSQLookupd(*lookupAddress)
	if err != nil {
		log.Printf("lookup connect error : %v", err)
		close(rdyChan)
		return
	}

	rdyChan <- 1
	<-goChan
	var msgCount int64
	endTime := time.Now().Add(td)
	var traceResp pubResp
	mutex.Lock()
	failedList, ok := pubTraceFailedList[topicName]
	if !ok {
		failedList = make(map[uint64]int64)
		pubTraceFailedList[topicName] = failedList
	}
	failedLocker := topicMutex[topicName]
	mutex.Unlock()

	for {
		if time.Now().After(endTime) {
			break
		}
		if (*sleepfor).Nanoseconds() > int64(10000) {
			time.Sleep(*sleepfor)
		}

		traceID := atomic.AddInt64(pubIDCounter, 1)
		data := make([]byte, 8)
		binary.BigEndian.PutUint64(data, uint64(traceID))
		if *trace {
			if *ordered {
				traceResp.id, traceResp.offset, traceResp.rawSize, err = pubMgr.PublishOrdered(topicName, data, data)
			} else {
				traceResp.id, traceResp.offset, traceResp.rawSize, err = pubMgr.PublishAndTrace(topicName, uint64(traceID), data)
			}
			if err != nil {
				log.Printf("pub id : %v error :%v\n", traceID, err)
				failedLocker.Lock()
				failedList[uint64(traceID)] = 1
				failedLocker.Unlock()
				atomic.AddInt64(&totalErrCount, 1)
				time.Sleep(time.Second)
				continue
			}
		} else {
			var err error
			err = pubMgr.Publish(topicName, data)
			if err != nil {
				log.Printf("pub id : %v error :%v\n", traceID, err)
				failedLocker.Lock()
				failedList[uint64(traceID)] = 1
				failedLocker.Unlock()

				atomic.AddInt64(&totalErrCount, 1)
				time.Sleep(time.Second)
				continue
			}
		}
		msgCount += 1
		atomic.AddInt64(&currentMsgCount, 1)
		if time.Now().After(endTime) {
			break
		}
	}
	atomic.AddInt64(&totalMsgCount, msgCount)
}
