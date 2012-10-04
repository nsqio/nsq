// This is a client that writes out to a file, and optionally rolls the file

package main

import (
	"../../nsq"
	"../../util"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

var (
	filenamePattern  = "%s.%s.%d-%02d-%02d_%02d.log" // topic.host.YYY-MM-DD_HH.log
	hostIdentifier   = flag.String("host-identifier", "", "value to output in log filename in place of hostname. <SHORT_HOST> and <HOSTNAME> are valid replacement tokens")
	outputDir        = flag.String("output-dir", "/tmp", "directory to write output files to")
	topic            = flag.String("topic", "", "nsq topic")
	channel          = flag.String("channel", "nsq_to_file", "nsq channel")
	maxInFlight      = flag.Int("max-in-flight", 1000, "max number of messages to allow in flight")
	verbose          = flag.Bool("verbose", false, "verbose logging")
	nsqdAddresses    = util.StringArray{}
	lookupdAddresses = util.StringArray{}
)

func init() {
	flag.Var(&nsqdAddresses, "nsqd-tcp-address", "nsqd TCP address (may be given multiple times)")
	flag.Var(&lookupdAddresses, "lookupd-http-address", "lookupd HTTP address (may be given multiple times)")
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

func router(r *nsq.Reader, f *FileLogger, termChan chan os.Signal, hupChan chan os.Signal) {
	pos := 0
	output := make([]*Message, *maxInFlight)
	sync := false
	ticker := time.NewTicker(time.Duration(30) * time.Second)
	closing := false

	for {
		select {
		case <-termChan:
			ticker.Stop()
			r.Stop()
			// ensures that we keep flushing whatever is left in the channels
			closing = true
		case <-hupChan:
			f.out.Close()
			f.out = nil
			updateFile(f)
			sync = true
		case <-ticker.C:
			updateFile(f)
			sync = true
		case m := <-f.logChan:
			if updateFile(f) {
				sync = true
			}
			_, err := f.out.Write(m.Body)
			if err != nil {
				log.Fatalf("ERROR: writing message to disk - %s", err.Error())
			}
			_, err = f.out.WriteString("\n")
			if err != nil {
				log.Fatalf("ERROR: writing newline to disk - %s", err.Error())
			}
			output[pos] = m
			pos++
		}

		if closing || sync || pos >= r.ConnectionMaxInFlight() {
			if pos > 0 {
				log.Printf("syncing %d records to disk", pos)
				err := f.out.Sync()
				if err != nil {
					log.Fatalf("ERROR: failed syncing messages - %s", err.Error())
				}
				for pos > 0 {
					pos--
					m := output[pos]
					m.returnChannel <- &nsq.FinishedMessage{m.Id, 0, true}
					output[pos] = nil
				}
			}
			sync = false
		}
	}
}

func updateFile(f *FileLogger) bool {
	t := time.Now()
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatalf("ERROR: unable to get hostname %s", err.Error())
	}
	shortHostname := strings.Split(hostname, ".")[0]
	identifier := shortHostname
	if len(*hostIdentifier) != 0 {
		identifier = strings.Replace(*hostIdentifier, "<SHORT_HOST>", shortHostname, -1)
		identifier = strings.Replace(identifier, "<HOSTNAME>", hostname, -1)
	}
	filename := fmt.Sprintf(filenamePattern, *topic, identifier, t.Year(), t.Month(), t.Day(), t.Hour())

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

func main() {
	flag.Parse()

	if *topic == "" || *channel == "" {
		log.Fatalf("--topic and --channel are required")
	}

	if *maxInFlight < 0 {
		log.Fatalf("--max-in-flight must be > 0")
	}

	if len(nsqdAddresses) == 0 && len(lookupdAddresses) == 0 {
		log.Fatalf("--nsqd-tcp-address or --lookupd-http-address required.")
	}
	if len(nsqdAddresses) != 0 && len(lookupdAddresses) != 0 {
		log.Fatalf("use --nsqd-tcp-address or --lookupd-http-address not both")
	}

	hupChan := make(chan os.Signal, 1)
	termChan := make(chan os.Signal, 1)
	signal.Notify(hupChan, syscall.SIGHUP)
	signal.Notify(termChan, syscall.SIGINT, syscall.SIGTERM)

	f := &FileLogger{
		logChan: make(chan *Message, 1),
	}

	r, err := nsq.NewReader(*topic, *channel)
	if err != nil {
		log.Fatalf(err.Error())
	}
	r.SetMaxInFlight(*maxInFlight)
	r.VerboseLogging = *verbose

	r.AddAsyncHandler(f)
	go router(r, f, termChan, hupChan)

	for _, addrString := range nsqdAddresses {
		err := r.ConnectToNSQ(addrString)
		if err != nil {
			log.Fatalf(err.Error())
		}
	}

	for _, addrString := range lookupdAddresses {
		log.Printf("lookupd addr %s", addrString)
		err := r.ConnectToLookupd(addrString)
		if err != nil {
			log.Fatalf(err.Error())
		}
	}

	<-r.ExitChan
}
