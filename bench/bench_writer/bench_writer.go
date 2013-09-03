package main

import (
	"bufio"
	"bytes"
	"flag"
	"github.com/bitly/go-nsq"
	"log"
	"net"
	"runtime"
	"sync"
	"time"
)

var (
	num        = flag.Int("num", 1000000, "num messages")
	tcpAddress = flag.String("nsqd-tcp-address", "127.0.0.1:4150", "<addr>:<port> to connect to nsqd")
	topic      = flag.String("topic", "sub_bench", "topic to receive messages on")
	size       = flag.Int("size", 200, "size of messages")
	batchSize  = flag.Int("batch-size", 200, "batch size of messages")
)

func main() {
	flag.Parse()
	var wg sync.WaitGroup

	msg := make([]byte, *size)
	batch := make([][]byte, 0)
	for i := 0; i < *batchSize; i++ {
		batch = append(batch, msg)
	}

	start := time.Now()
	for j := 0; j < runtime.GOMAXPROCS(0); j++ {
		wg.Add(1)
		go func() {
			pubWorker(*num, *tcpAddress, *batchSize, batch, *topic)
			wg.Done()
		}()
	}

	wg.Wait()
	end := time.Now()
	duration := end.Sub(start)
	log.Printf("duration: %s - %.03fmb/s - %.03fops/s - %.03fus/op",
		duration,
		float64(*num*200)/duration.Seconds()/1024/1024,
		float64(*num)/duration.Seconds(),
		float64(duration/time.Microsecond)/float64(*num))
}

func pubWorker(n int, tcpAddr string, batchSize int, batch [][]byte, topic string) {
	conn, err := net.DialTimeout("tcp", tcpAddr, time.Second)
	if err != nil {
		panic(err.Error())
	}
	conn.Write(nsq.MagicV2)
	rw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))

	num := n / runtime.GOMAXPROCS(0) / batchSize
	for i := 0; i < num; i += 1 {
		cmd, _ := nsq.MultiPublish(topic, batch)
		err := cmd.Write(rw)
		if err != nil {
			panic(err.Error())
		}
		err = rw.Flush()
		if err != nil {
			panic(err.Error())
		}
		resp, err := nsq.ReadResponse(rw)
		if err != nil {
			panic(err.Error())
		}
		_, data, _ := nsq.UnpackResponse(resp)
		if !bytes.Equal(data, []byte("OK")) {
			panic("invalid response")
		}
	}
}
