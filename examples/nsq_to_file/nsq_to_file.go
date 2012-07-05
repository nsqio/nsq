// This is a client that writes out to a file, and optionally rolls the file

package main

import (
	"../../nsq"
	"../../util"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"time"
)

var (
	filenamePattern  = "%s.%s.%d-%02d-%02d_%02d.log" // topic.host.YYY-MM-DD_HH.log
	outputDir        = flag.String("output-dir", "/tmp", "directory to write output files to")
	topic            = flag.String("topic-name", "", "nsq topic")
	channel          = flag.String("channel-name", "nsq_to_file", "nsq channel")
	buffer           = flag.Int("buffer", 1000, "number of messages to buffer in channel and disk before sync/ack")
	nsqAddresses     = util.StringArray{}
	lookupdAddresses = util.StringArray{}
)

func init() {
	flag.Var(&nsqAddresses, "nsq-address", "nsq address (may be given multiple times)")
	flag.Var(&lookupdAddresses, "lookupd-address", "lookupd address (may be given multiple times)")
}

type FileLogger struct {
	out      *os.File
	filename string
	logChan  chan *Message
}

type Message struct {
	*nsq.Message
	returnChannel chan *nsq.FinishedMessage
}

type SyncMsg struct {
	m             *nsq.FinishedMessage
	returnChannel chan *nsq.FinishedMessage
}

func (l *FileLogger) HandleMessage(m *nsq.Message, responseChannel chan *nsq.FinishedMessage) {
	l.logChan <- &Message{m, responseChannel}
}

func main() {
	flag.Parse()

	if *topic == "" || *channel == "" {
		log.Fatalf("--topic-name and --channel-name are required")
	}

	if *buffer < 0 {
		log.Fatalf("--buffer must be > 0")
	}

	if len(nsqAddresses) == 0 && len(lookupdAddresses) == 0 {
		log.Fatalf("--nsq-address or --lookupd-address required.")
	}
	if len(nsqAddresses) != 0 && len(lookupdAddresses) != 0 {
		log.Fatalf("use --nsq-address or --lookupd-address not both")
	}

	f := &FileLogger{
		logChan: make(chan *Message, *buffer),
	}

	r, _ := nsq.NewReader(*topic, *channel)
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
				f.out.Write(m.Body)
				f.out.WriteString("\n")
				x := &nsq.FinishedMessage{m.Id, 0, true}
				output[pos] = &SyncMsg{x, m.returnChannel}
				pos++
			}

			if sync || pos >= *buffer {
				if pos > 0 {
					log.Printf("syncing %d records to disk", pos)
					f.out.Sync()
					for pos > 0 {
						pos--
						m := output[pos]
						m.returnChannel <- m.m
						output[pos] = nil
					}
				}
				sync = false
			}
		}
	}()

	for _, addrString := range nsqAddresses {
		addr, _ := net.ResolveTCPAddr("tcp", addrString)
		err := r.ConnectToNSQ(addr)
		if err != nil {
			log.Fatalf(err.Error())
		}
	}

	for _, addrString := range lookupdAddresses {
		log.Printf("lookupd addr %s", addrString)
		addr, _ := net.ResolveTCPAddr("tcp", addrString)
		err := r.ConnectToLookupd(addr)
		if err != nil {
			log.Fatalf(err.Error())
		}
	}

	<-r.ExitChan

}

func updateFile(f *FileLogger) bool {
	t := time.Now()

	hostname, _ := os.Hostname()
	shortHostname := strings.Split(hostname, ".")
	filename := fmt.Sprintf(filenamePattern, *topic, shortHostname[0], t.Year(), t.Month(), t.Day(), t.Hour())
	if filename != f.filename || f.out == nil {
		log.Printf("old %s new %s", f.filename, filename)
		// roll it
		if f.out != nil {
			f.out.Close()
		}
		os.MkdirAll(*outputDir, 777)
		log.Printf("opening %s/%s", *outputDir, filename)
		newfile, err := os.OpenFile(fmt.Sprintf("%s/%s", *outputDir, filename), os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
		f.out = newfile
		if err != nil {
			log.Fatal(err)
		}
		f.filename = filename
		return true
	}
	return false
}
