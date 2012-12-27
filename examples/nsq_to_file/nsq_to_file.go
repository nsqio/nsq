// This is a client that writes out to a file, and optionally rolls the file

package main

import (
	"../../nsq"
	"../../util"
	"compress/gzip"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path"
	"strings"
	"syscall"
	"time"
)

var (
	datetimeFormat   = flag.String("datetime-format", "%Y-%m-%d_%H", "strftime compatible format for <DATETIME> in filename format")
	filenameFormat   = flag.String("filename-format", "<TOPIC>.<HOST>.<DATETIME>.log", "output filename format (<TOPIC>, <HOST>, <DATETIME> are replaced)")
	showVersion      = flag.Bool("version", false, "print version string")
	hostIdentifier   = flag.String("host-identifier", "", "value to output in log filename in place of hostname. <SHORT_HOST> and <HOSTNAME> are valid replacement tokens")
	outputDir        = flag.String("output-dir", "/tmp", "directory to write output files to")
	topic            = flag.String("topic", "", "nsq topic")
	channel          = flag.String("channel", "nsq_to_file", "nsq channel")
	maxInFlight      = flag.Int("max-in-flight", 1000, "max number of messages to allow in flight")
	gzipFlag         = flag.Bool("gzip", false, "gzip output files.")
	verbose          = flag.Bool("verbose", false, "verbose logging")
	nsqdTCPAddrs     = util.StringArray{}
	lookupdHTTPAddrs = util.StringArray{}
)

func init() {
	flag.Var(&nsqdTCPAddrs, "nsqd-tcp-address", "nsqd TCP address (may be given multiple times)")
	flag.Var(&lookupdHTTPAddrs, "lookupd-http-address", "lookupd HTTP address (may be given multiple times)")
}

type FileLogger struct {
	out        *os.File
	gzipWriter *gzip.Writer
	filename   string
	logChan    chan *Message
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
			f.Close()
			f.updateFile()
			sync = true
		case <-ticker.C:
			f.updateFile()
			sync = true
		case m := <-f.logChan:
			if f.updateFile() {
				sync = true
			}
			_, err := f.Write(m.Body)
			if err != nil {
				log.Fatalf("ERROR: writing message to disk - %s", err.Error())
			}
			_, err = f.Write([]byte("\n"))
			if err != nil {
				log.Fatalf("ERROR: writing newline to disk - %s", err.Error())
			}
			output[pos] = m
			pos++
			if pos == *maxInFlight {
				sync = true
			}
		}

		if closing || sync || r.IsStarved() {
			if pos > 0 {
				log.Printf("syncing %d records to disk", pos)
				err := f.Sync()
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

func (f *FileLogger) Close() {
	if f.out != nil {
		if f.gzipWriter != nil {
			f.gzipWriter.Close()
		}
		f.out.Close()
		f.out = nil
	}
}
func (f *FileLogger) Write(p []byte) (n int, err error) {
	if f.gzipWriter != nil {
		return f.gzipWriter.Write(p)
	}
	return f.out.Write(p)
}
func (f *FileLogger) Sync() error {
	var err error
	if f.gzipWriter != nil {
		f.gzipWriter.Close()
		err = f.out.Sync()
		f.gzipWriter, _ = gzip.NewWriterLevel(f.out, gzip.BestSpeed)
	} else {
		err = f.out.Sync()
	}
	return err
}

func (f *FileLogger) updateFile() bool {
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

	filename := strings.Replace(*filenameFormat, "<TOPIC>", *topic, -1)
	filename = strings.Replace(filename, "<HOST>", identifier, -1)
	datetime := strftime(*datetimeFormat, t)
	filename = strings.Replace(filename, "<DATETIME>", datetime, -1)
	if *gzipFlag {
		filename = filename + ".gz"
	}

	if filename != f.filename || f.out == nil {
		log.Printf("old %s new %s", f.filename, filename)
		f.Close()
		os.MkdirAll(*outputDir, 777)
		log.Printf("opening %s/%s", *outputDir, filename)
		newfile, err := os.OpenFile(path.Join(*outputDir, filename), os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
		f.out = newfile
		if err != nil {
			log.Fatal(err)
		}
		f.filename = filename
		if *gzipFlag {
			f.gzipWriter, _ = gzip.NewWriterLevel(newfile, gzip.BestSpeed)
		}
		return true
	}

	return false
}

func main() {
	flag.Parse()

	if *showVersion {
		fmt.Printf("nsq_to_file v%s\n", util.BINARY_VERSION)
		return
	}

	if *topic == "" || *channel == "" {
		log.Fatalf("--topic and --channel are required")
	}

	if *maxInFlight < 0 {
		log.Fatalf("--max-in-flight must be > 0")
	}

	if len(nsqdTCPAddrs) == 0 && len(lookupdHTTPAddrs) == 0 {
		log.Fatalf("--nsqd-tcp-address or --lookupd-http-address required.")
	}
	if len(nsqdTCPAddrs) != 0 && len(lookupdHTTPAddrs) != 0 {
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

	for _, addrString := range nsqdTCPAddrs {
		err := r.ConnectToNSQ(addrString)
		if err != nil {
			log.Fatalf(err.Error())
		}
	}

	for _, addrString := range lookupdHTTPAddrs {
		log.Printf("lookupd addr %s", addrString)
		err := r.ConnectToLookupd(addrString)
		if err != nil {
			log.Fatalf(err.Error())
		}
	}

	<-r.ExitChan
}
