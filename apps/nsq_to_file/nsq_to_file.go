// This is a client that writes out to a file, and optionally rolls the file

package main

import (
	"compress/gzip"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/bitly/go-nsq"
	"github.com/bitly/nsq/util"
	"github.com/bitly/nsq/util/lookupd"
)

var (
	showVersion = flag.Bool("version", false, "print version string")

	channel     = flag.String("channel", "nsq_to_file", "nsq channel")
	maxInFlight = flag.Int("max-in-flight", 200, "max number of messages to allow in flight")

	outputDir      = flag.String("output-dir", "/tmp", "directory to write output files to")
	datetimeFormat = flag.String("datetime-format", "%Y-%m-%d_%H", "strftime compatible format for <DATETIME> in filename format")
	filenameFormat = flag.String("filename-format", "<TOPIC>.<HOST><GZIPREV>.<DATETIME>.log", "output filename format (<TOPIC>, <HOST>, <DATETIME>, <GZIPREV> are replaced. <GZIPREV> is a suffix when an existing gzip file already exists)")
	hostIdentifier = flag.String("host-identifier", "", "value to output in log filename in place of hostname. <SHORT_HOST> and <HOSTNAME> are valid replacement tokens")
	gzipLevel      = flag.Int("gzip-level", 6, "gzip compression level (1-9, 1=BestSpeed, 9=BestCompression)")
	gzipEnabled    = flag.Bool("gzip", false, "gzip output files.")
	skipEmptyFiles = flag.Bool("skip-empty-files", false, "Skip writting empty files")
	topicPollRate  = flag.Duration("topic-refresh", time.Minute, "how frequently the topic list should be refreshed")

	readerOpts       = util.StringArray{}
	nsqdTCPAddrs     = util.StringArray{}
	lookupdHTTPAddrs = util.StringArray{}
	topics           = util.StringArray{}

	// TODO: remove, deprecated
	gzipCompression = flag.Int("gzip-compression", 3, "(deprecated) use --gzip-level, gzip compression level (1 = BestSpeed, 2 = BestCompression, 3 = DefaultCompression)")
)

func init() {
	flag.Var(&readerOpts, "reader-opt", "option to passthrough to nsq.Consumer (may be given multiple times)")
	flag.Var(&nsqdTCPAddrs, "nsqd-tcp-address", "nsqd TCP address (may be given multiple times)")
	flag.Var(&lookupdHTTPAddrs, "lookupd-http-address", "lookupd HTTP address (may be given multiple times)")
	flag.Var(&topics, "topic", "nsq topic (may be given multiple times)")
}

type FileLogger struct {
	out              *os.File
	gzipWriter       *gzip.Writer
	lastFilename     string
	logChan          chan *nsq.Message
	compressionLevel int
	gzipEnabled      bool
	filenameFormat   string

	ExitChan chan int
	termChan chan bool
	hupChan  chan bool
}

type ReaderFileLogger struct {
	F *FileLogger
	R *nsq.Consumer
}

type TopicDiscoverer struct {
	topics   map[string]*ReaderFileLogger
	termChan chan os.Signal
	hupChan  chan os.Signal
	wg       sync.WaitGroup
}

func newTopicDiscoverer() *TopicDiscoverer {
	return &TopicDiscoverer{
		topics:   make(map[string]*ReaderFileLogger),
		termChan: make(chan os.Signal),
		hupChan:  make(chan os.Signal),
	}
}

func (l *FileLogger) HandleMessage(m *nsq.Message) error {
	m.DisableAutoResponse()
	l.logChan <- m
	return nil
}

func (f *FileLogger) router(r *nsq.Consumer) {
	pos := 0
	output := make([]*nsq.Message, *maxInFlight)
	sync := false
	ticker := time.NewTicker(time.Duration(30) * time.Second)
	closing := false
	closeFile := false
	exit := false

	for {
		select {
		case <-r.StopChan:
			sync = true
			closeFile = true
			exit = true
		case <-f.termChan:
			ticker.Stop()
			r.Stop()
			sync = true
			closing = true
		case <-f.hupChan:
			sync = true
			closeFile = true
		case <-ticker.C:
			if f.needsFileRotate() {
				if *skipEmptyFiles {
					closeFile = true
				} else {
					f.updateFile()
				}
			}
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
			if pos == cap(output) {
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
					m.Finish()
					output[pos] = nil
				}
			}
			sync = false
		}

		if closeFile {
			f.Close()
			closeFile = false
		}
		if exit {
			close(f.ExitChan)
			break
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
		f.gzipWriter, _ = gzip.NewWriterLevel(f.out, f.compressionLevel)
	} else {
		err = f.out.Sync()
	}
	return err
}

func (f *FileLogger) calculateCurrentFilename() string {
	t := time.Now()

	datetime := strftime(*datetimeFormat, t)
	filename := strings.Replace(f.filenameFormat, "<DATETIME>", datetime, -1)
	if !f.gzipEnabled {
		filename = strings.Replace(filename, "<GZIPREV>", "", -1)
	}
	return filename

}

func (f *FileLogger) needsFileRotate() bool {
	filename := f.calculateCurrentFilename()
	return filename != f.lastFilename
}

func (f *FileLogger) updateFile() bool {
	filename := f.calculateCurrentFilename()
	maxGzipRevisions := 1000
	if filename != f.lastFilename || f.out == nil {
		f.Close()
		os.MkdirAll(*outputDir, 0770)
		var newFile *os.File
		var err error
		if f.gzipEnabled {
			// for gzip files, we never append to an existing file
			// we try to create different revisions, replacing <GZIPREV> in the filename
			for gzipRevision := 0; gzipRevision < maxGzipRevisions; gzipRevision += 1 {
				var revisionSuffix string
				if gzipRevision > 0 {
					revisionSuffix = fmt.Sprintf("-%d", gzipRevision)
				}
				tempFilename := strings.Replace(filename, "<GZIPREV>", revisionSuffix, -1)
				fullPath := path.Join(*outputDir, tempFilename)
				dir, _ := filepath.Split(fullPath)
				if dir != "" {
					err = os.MkdirAll(dir, 0770)
					if err != nil {
						log.Fatalf("ERROR: %s Unable to create %s", err, dir)
					}
				}
				newFile, err = os.OpenFile(fullPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0666)
				if err != nil && os.IsExist(err) {
					log.Printf("INFO: file already exists: %s", fullPath)
					continue
				}
				if err != nil {
					log.Fatalf("ERROR: %s Unable to open %s", err, fullPath)
				}
				log.Printf("opening %s", fullPath)
				break
			}
			if newFile == nil {
				log.Fatalf("ERROR: Unable to open a new gzip file after %d tries", maxGzipRevisions)
			}
		} else {
			log.Printf("opening %s/%s", *outputDir, filename)
			fullPath := path.Join(*outputDir, filename)
			dir, _ := filepath.Split(fullPath)
			if dir != "" {
				err = os.MkdirAll(dir, 0770)
				if err != nil {
					log.Fatalf("ERROR: %s Unable to create %s", err, dir)
				}
			}
			newFile, err = os.OpenFile(fullPath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
			if err != nil {
				log.Fatal(err)
			}
		}

		f.out = newFile
		f.lastFilename = filename
		if f.gzipEnabled {
			f.gzipWriter, _ = gzip.NewWriterLevel(newFile, f.compressionLevel)
		}
		return true
	}

	return false
}

func NewFileLogger(gzipEnabled bool, compressionLevel int, filenameFormat, topic string) (*FileLogger, error) {
	if gzipEnabled && strings.Index(filenameFormat, "<GZIPREV>") == -1 {
		return nil, errors.New("missing <GZIPREV> in filenameFormat")
	}

	hostname, err := os.Hostname()
	if err != nil {
		return nil, err
	}
	shortHostname := strings.Split(hostname, ".")[0]
	identifier := shortHostname
	if len(*hostIdentifier) != 0 {
		identifier = strings.Replace(*hostIdentifier, "<SHORT_HOST>", shortHostname, -1)
		identifier = strings.Replace(identifier, "<HOSTNAME>", hostname, -1)
	}
	filenameFormat = strings.Replace(filenameFormat, "<TOPIC>", topic, -1)
	filenameFormat = strings.Replace(filenameFormat, "<HOST>", identifier, -1)
	if gzipEnabled && !strings.HasSuffix(filenameFormat, ".gz") {
		filenameFormat = filenameFormat + ".gz"
	}

	f := &FileLogger{
		logChan:          make(chan *nsq.Message, 1),
		compressionLevel: compressionLevel,
		filenameFormat:   filenameFormat,
		gzipEnabled:      gzipEnabled,
		ExitChan:         make(chan int),
		termChan:         make(chan bool),
		hupChan:          make(chan bool),
	}
	return f, nil
}

func hasArg(s string) bool {
	for _, arg := range os.Args {
		if strings.Contains(arg, s) {
			return true
		}
	}
	return false
}

func newReaderFileLogger(topic string) (*ReaderFileLogger, error) {
	f, err := NewFileLogger(*gzipEnabled, *gzipLevel, *filenameFormat, topic)
	if err != nil {
		return nil, err
	}

	cfg := nsq.NewConfig()
	cfg.UserAgent = fmt.Sprintf("nsq_to_file/%s go-nsq/%s", util.BINARY_VERSION, nsq.VERSION)
	err = util.ParseReaderOpts(cfg, readerOpts)
	if err != nil {
		return nil, err
	}
	cfg.MaxInFlight = *maxInFlight

	r, err := nsq.NewConsumer(topic, *channel, cfg)
	if err != nil {
		return nil, err
	}
	r.SetLogger(log.New(os.Stderr, "", log.LstdFlags), nsq.LogLevelInfo)

	r.AddHandler(f)

	for _, addrString := range nsqdTCPAddrs {
		err := r.ConnectToNSQD(addrString)
		if err != nil {
			log.Fatalf(err.Error())
		}
	}

	for _, addrString := range lookupdHTTPAddrs {
		log.Printf("lookupd addr %s", addrString)
		err := r.ConnectToNSQLookupd(addrString)
		if err != nil {
			log.Fatalf(err.Error())
		}
	}

	return &ReaderFileLogger{
		R: r,
		F: f,
	}, nil
}

func (t *TopicDiscoverer) startTopicRouter(logger *ReaderFileLogger) {
	t.wg.Add(1)
	defer t.wg.Done()
	go logger.F.router(logger.R)
	<-logger.F.ExitChan
}

func (t *TopicDiscoverer) syncTopics(addrs []string) {
	newTopics, err := lookupd.GetLookupdTopics(addrs)
	if err != nil {
		log.Printf("ERROR: could not retrieve topic list: %s", err)
	}
	for _, topic := range newTopics {
		if _, ok := t.topics[topic]; !ok {
			logger, err := newReaderFileLogger(topic)
			if err != nil {
				log.Printf("ERROR: couldn't create logger for new topic %s: %s", topic, err.Error())
				continue
			}
			t.topics[topic] = logger
			go t.startTopicRouter(logger)
		}
	}
}

func (t *TopicDiscoverer) stopReaderFileLoggers() {
	for _, topic := range t.topics {
		topic.F.termChan <- true
	}
}

func (t *TopicDiscoverer) hupReaderFileLoggers() {
	for _, topic := range t.topics {
		topic.F.hupChan <- true
	}
}

func (t *TopicDiscoverer) watch(addrs []string, sync bool) {
	ticker := time.Tick(*topicPollRate)
	for {
		select {
		case <-ticker:
			if sync {
				t.syncTopics(addrs)
			}
		case <-t.termChan:
			t.stopReaderFileLoggers()
			t.wg.Wait()
			return
		case <-t.hupChan:
			t.hupReaderFileLoggers()
		}
	}
}

func main() {
	flag.Parse()

	if *showVersion {
		fmt.Printf("nsq_to_file v%s\n", util.BINARY_VERSION)
		return
	}

	if *channel == "" {
		log.Fatalf("--channel is required")
	}

	var topicsFromNSQLookupd bool

	if len(nsqdTCPAddrs) == 0 && len(lookupdHTTPAddrs) == 0 {
		log.Fatalf("--nsqd-tcp-address or --lookupd-http-address required.")
	}
	if len(nsqdTCPAddrs) != 0 && len(lookupdHTTPAddrs) != 0 {
		log.Fatalf("use --nsqd-tcp-address or --lookupd-http-address not both")
	}

	if *gzipLevel < 1 || *gzipLevel > 9 {
		log.Fatalf("invalid --gzip-level value (%d), should be 1-9", *gzipLevel)
	}

	// TODO: remove, deprecated
	if hasArg("gzip-compression") {
		log.Printf("WARNING: --gzip-compression is deprecated in favor of --gzip-level")
		switch *gzipCompression {
		case 1:
			*gzipLevel = gzip.BestSpeed
		case 2:
			*gzipLevel = gzip.BestCompression
		case 3:
			*gzipLevel = gzip.DefaultCompression
		default:
			log.Fatalf("invalid --gzip-compression value (%d), should be 1,2,3", *gzipCompression)
		}
	}

	discoverer := newTopicDiscoverer()

	signal.Notify(discoverer.hupChan, syscall.SIGHUP)
	signal.Notify(discoverer.termChan, syscall.SIGINT, syscall.SIGTERM)

	if len(topics) < 1 {
		if len(lookupdHTTPAddrs) < 1 {
			log.Fatalf("use --topic to list at least one topic to subscribe to or specify at least one --lookupd-http-address to subscribe to all its topics")
		}
		topicsFromNSQLookupd = true
		var err error
		topics, err = lookupd.GetLookupdTopics(lookupdHTTPAddrs)
		if err != nil {
			log.Fatalf("ERROR: could not retrieve topic list: %s", err)
		}
	}

	for _, topic := range topics {
		logger, err := newReaderFileLogger(topic)
		if err != nil {
			log.Fatalf("ERROR: couldn't create logger for topic %s: %s", topic, err.Error())
		}
		discoverer.topics[topic] = logger
		go discoverer.startTopicRouter(logger)
	}

	discoverer.watch(lookupdHTTPAddrs, topicsFromNSQLookupd)
}
