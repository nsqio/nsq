package main

import (
	"bufio"
	"flag"
	"log"
	"math"
	"net"
	"runtime"
	"sync"
	"time"

	"github.com/bitly/go-nsq"
)

var (
	num        = flag.Int("num", 1000000, "num messages")
	tcpAddress = flag.String("nsqd-tcp-address", "127.0.0.1:4150", "<addr>:<port> to connect to nsqd")
	size       = flag.Int("size", 200, "size of messages")
	topic      = flag.String("topic", "sub_bench", "topic to receive messages on")
	channel    = flag.String("channel", "ch", "channel to receive messages on")
)

func main() {
	flag.Parse()
	var wg sync.WaitGroup

	goChan := make(chan int)
	rdyChan := make(chan int)
	workers := runtime.GOMAXPROCS(0)
	for j := 0; j < workers; j++ {
		wg.Add(1)
		go func(id int) {
			subWorker(*num, workers, *tcpAddress, *topic, *channel, rdyChan, goChan, id)
			wg.Done()
		}(j)
		<-rdyChan
	}

	start := time.Now()
	close(goChan)
	wg.Wait()
	end := time.Now()
	duration := end.Sub(start)
	log.Printf("duration: %s - %.03fmb/s - %.03fops/s - %.03fus/op",
		duration,
		float64(*num*(*size))/duration.Seconds()/1024/1024,
		float64(*num)/duration.Seconds(),
		float64(duration/time.Microsecond)/float64(*num))
}

func subWorker(n int, workers int, tcpAddr string, topic string, channel string, rdyChan chan int, goChan chan int, id int) {
	conn, err := net.DialTimeout("tcp", tcpAddr, time.Second)
	if err != nil {
		panic(err.Error())
	}
	conn.Write(nsq.MagicV2)
	rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
	ci := make(map[string]interface{})
	ci["short_id"] = "test"
	ci["long_id"] = "test"
	cmd, _ := nsq.Identify(ci)
	cmd.WriteTo(rw)
	nsq.Subscribe(topic, channel).WriteTo(rw)
	rdyCount := int(math.Min(math.Max(float64(n/workers), 1), 2500))
	rdyChan <- 1
	<-goChan
	nsq.Ready(rdyCount).WriteTo(rw)
	rw.Flush()
	nsq.ReadResponse(rw)
	nsq.ReadResponse(rw)
	num := n / workers
	numRdy := num/rdyCount - 1
	rdy := rdyCount
	for i := 0; i < num; i += 1 {
		resp, err := nsq.ReadResponse(rw)
		if err != nil {
			panic(err.Error())
		}
		frameType, data, err := nsq.UnpackResponse(resp)
		if err != nil {
			panic(err.Error())
		}
		if frameType == nsq.FrameTypeError {
			panic(string(data))
		} else if frameType == nsq.FrameTypeResponse {
			continue
		}
		msg, err := nsq.DecodeMessage(data)
		if err != nil {
			panic(err.Error())
		}
		nsq.Finish(msg.ID).WriteTo(rw)
		rdy--
		if rdy == 0 && numRdy > 0 {
			nsq.Ready(rdyCount).WriteTo(rw)
			rdy = rdyCount
			numRdy--
			rw.Flush()
		}
	}
}
