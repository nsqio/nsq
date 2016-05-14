package main

import (
	"errors"
	"flag"
	"log"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/absolute8511/go-nsq"
	"github.com/absolute8511/nsq/internal/app"
	"github.com/absolute8511/nsq/internal/clusterinfo"
	"github.com/absolute8511/nsq/internal/http_api"
)

var (
	flagSet = flag.NewFlagSet("bench", flag.ExitOnError)

	runfor        = flagSet.Duration("runfor", 10*time.Second, "duration of time to run")
	sleepfor      = flagSet.Duration("sleepfor", 1*time.Second, " time to sleep between pub")
	lookupAddress = flag.String("lookup-http-address", "127.0.0.1:4161", "<addr>:<port> to connect to nsqd")
	topics        = app.StringArray{}
	size          = flagSet.Int("size", 200, "size of messages")
	batchSize     = flagSet.Int("batch-size", 20, "batch size of messages")
	deadline      = flagSet.String("deadline", "", "deadline to start the benchmark run")
	concurrency   = flagSet.Int("c", 100, "concurrency of goroutine")
	benchCase     = flagSet.String("bench-case", "simple", "which bench should run (simple/benchpub/benchsub)")
)

var totalMsgCount int64
var currentMsgCount int64
var totalErrCount int64
var config *nsq.Config

func init() {
	flagSet.Var(&topics, "bench-topics", "the topic list for benchmark [t1, t2, t3]")
}

func startBenchPub(msg []byte, batch [][]byte) {
	var wg sync.WaitGroup
	pubMgr, err := nsq.NewTopicProducerMgr(topics, nsq.PubRR, config)
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
		err = pubMgr.Publish(t, msg)
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
			pubWorker(*runfor, pubMgr, *batchSize, batch, topics, rdyChan, goChan)
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

	log.Printf("total error : %v\n", atomic.LoadInt64(&totalErrCount))
}

func startBenchSub() {
	var wg sync.WaitGroup

	log.SetPrefix("[bench_reader] ")

	goChan := make(chan int)
	rdyChan := make(chan int)
	for j := 0; j < *concurrency; j++ {
		wg.Add(1)
		go func(id int, topic string) {
			subWorker(*runfor, *lookupAddress, topic, topic+"_ch", rdyChan, goChan, id)
			wg.Done()
		}(j, topics[j%len(topics)])
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
	wg.Wait()
	end := time.Now()
	duration := end.Sub(start)
	tmc := atomic.LoadInt64(&totalMsgCount)
	log.Printf("duration: %s - %.03fmb/s - %.03fops/s - %.03fus/op",
		duration,
		float64(tmc*int64(*size))/duration.Seconds()/1024/1024,
		float64(tmc)/duration.Seconds(),
		float64(duration/time.Microsecond)/float64(tmc))
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
	for _, t := range topics {
		err := cluster.CreateTopicChannel(t, "", tmpList)
		if err != nil {
			log.Printf("failed to create topic: %v, err: %v", t, err)
		}
	}
	currentTopics, err := cluster.GetLookupdTopics(tmpList)
	if err != nil {
		log.Printf("failed : %v\n", err)
	} else {
		log.Printf("return: %v\n", topics)
	}
	chs, err := cluster.GetLookupdTopicChannels(currentTopics[0], 0, tmpList)
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

func main() {
	flagSet.Parse(os.Args[1:])
	config = nsq.NewConfig()

	log.SetPrefix("[bench_writer] ")

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
	}
}

func pubWorker(td time.Duration, pubMgr *nsq.TopicProducerMgr, batchSize int, batch [][]byte, topics app.StringArray, rdyChan chan int, goChan chan int) {
	rdyChan <- 1
	<-goChan
	var msgCount int64
	endTime := time.Now().Add(td)
	r := rand.New(rand.NewSource(time.Now().Unix()))
	for {
		if time.Now().After(endTime) {
			break
		}
		if (*sleepfor).Nanoseconds() > int64(10000) {
			time.Sleep(*sleepfor)
		}

		i := r.Intn(len(topics))
		err := pubMgr.MultiPublish(topics[i], batch)
		if err != nil {
			log.Println("pub error :" + err.Error())
			atomic.AddInt64(&totalErrCount, 1)
			time.Sleep(time.Second)
			continue
		}
		msgCount += int64(len(batch))
		if time.Now().After(endTime) {
			break
		}
		atomic.AddInt64(&currentMsgCount, int64(len(batch)))
	}
	atomic.AddInt64(&totalMsgCount, msgCount)
}

type consumeHandler struct {
}

func (c *consumeHandler) HandleMessage(message *nsq.Message) error {
	newCount := atomic.AddInt64(&totalMsgCount, 1)
	if newCount < 2 {
		return errors.New("failed by need.")
	}
	return nil
}

func subWorker(td time.Duration, lookupAddr string, topic string, channel string, rdyChan chan int, goChan chan int, id int) {
	consumer, err := nsq.NewConsumer(topic, channel, config)
	if err != nil {
		panic(err.Error())
	}
	consumer.SetLogger(log.New(os.Stderr, "", log.LstdFlags), nsq.LogLevelInfo)
	consumer.AddHandler(&consumeHandler{})
	rdyChan <- 1
	<-goChan
	done := make(chan struct{})
	go func() {
		time.Sleep(td)
		consumer.Stop()
		close(done)
	}()
	consumer.ConnectToNSQLookupd(lookupAddr)
	<-done
}
