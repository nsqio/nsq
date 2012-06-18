// This is a client that writes out to a file, and optionally rolls the file

package main

import (
	"../nsqreader"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"time"
)

var (
	nsqAddress      = flag.String("nsq-address", "127.0.0.1:5150", "<addr>:<port> to listen on for TCP clients")
	filenamePattern = flag.String("filename-pattern", "/tmp/output-%d-%02d-%02d_%02d.log", "file pattern (with a strftime format)")
	topic           = flag.String("topic-name", "", "nsq toppic")
	channel         = flag.String("channel-name", "", "nsq channel")
	buffer          = flag.Int("buffer", 1000, "number of messages to buffer in channel and disk before sync/ack")
)

type FileLogger struct {
	out      *os.File
	filename string
	logChan  chan *Message
}

type Message struct {
	id            []byte
	body          []byte
	returnChannel chan *nsqreader.FinishedMessage
}

type SyncMsg struct {
	m             *nsqreader.FinishedMessage
	returnChannel chan *nsqreader.FinishedMessage
}

func (l *FileLogger) HandleMessage(msgid []byte, data []byte, responseChannel chan *nsqreader.FinishedMessage) {
	l.logChan <- &Message{msgid, data, responseChannel}
}

func main() {
	flag.Parse()

	if *topic == "" || *channel == "" {
		log.Fatalf("--topic-name and --channel-name are required")
	}

	if *buffer < 0 {
		log.Fatalf("--buffer must be > 0")
	}

	f := &FileLogger{
		logChan: make(chan *Message, *buffer),
	}
	addr, _ := net.ResolveTCPAddr("tcp", *nsqAddress)
	r, _ := nsqreader.NewNSQReader(*topic, *channel)
	r.BufferSize = *buffer

	r.AddAsyncHandler(f)
	go func() {
		var pos = 0
		var output = make([]*SyncMsg, *buffer)
		var sync = false
		var ticker = time.Tick(time.Duration(30) * time.Second)
		for {
			select {
			case <-ticker:
				if pos != 0 || f.out != nil {
					updateFile(f)
					sync = true
				}
			case m := <-f.logChan:
				if updateFile(f) {
					sync = true
				}
				f.out.Write(m.body)
				f.out.WriteString("\n")
				x := &nsqreader.FinishedMessage{m.id, true}
				output[pos] = &SyncMsg{x, m.returnChannel}
				pos++
			}

			if sync || pos >= *buffer {
				log.Printf("syncing %d records to disk", pos)
				for pos > 0 {
					pos--
					m := output[pos]
					m.returnChannel <- m.m
					output[pos] = nil
				}
				sync = false
			}
		}
	}()

	err := r.ConnectToNSQ(addr)
	if err != nil {
		log.Fatalf(err.Error())
	}

	<-r.ExitChan

}

func updateFile(f *FileLogger) bool {
	t := time.Now()

	filename := fmt.Sprintf(*filenamePattern, t.Year(), t.Month(), t.Day(), t.Hour())
	if filename != f.filename || f.out == nil {
		log.Printf("old %s new %s", f.filename, filename)
		// roll it
		if f.out != nil {
			f.out.Close()
		}
		// os.MkdirAll(filename, 777)
		log.Printf("opening %s", filename)
		newfile, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
		f.out = newfile
		if err != nil {
			log.Fatal(err)
		}
		f.filename = filename
		return true
	}
	return false
}
