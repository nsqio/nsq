// This is a client that writes out to a file, and optionally rolls the file

package main

import (
	"compress/gzip"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/nsqio/go-nsq"
	"github.com/nsqio/nsq/internal/app"
	"github.com/nsqio/nsq/internal/clusterinfo"
	"github.com/nsqio/nsq/internal/http_api"
	"github.com/nsqio/nsq/internal/version"
)

var (
	showVersion = flag.Bool("version", false, "print version string")

	channel     = flag.String("channel", "nsq_to_file", "nsq channel")
	maxInFlight = flag.Int("max-in-flight", 200, "max number of messages to allow in flight")

	outputDir      = flag.String("output-dir", "/tmp", "directory to write output files to")
	datetimeFormat = flag.String("datetime-format", "%Y-%m-%d_%H", "strftime compatible format for <DATETIME> in filename format")
	filenameFormat = flag.String("filename-format", "<TOPIC>.<HOST><REV>.<DATETIME>.log", "output filename format (<TOPIC>, <HOST>, <PID>, <DATETIME>, <REV> are replaced. <REV> is increased when file already exists)")
	hostIdentifier = flag.String("host-identifier", "", "value to output in log filename in place of hostname. <SHORT_HOST> and <HOSTNAME> are valid replacement tokens")
	gzipLevel      = flag.Int("gzip-level", 6, "gzip compression level (1-9, 1=BestSpeed, 9=BestCompression)")
	gzipEnabled    = flag.Bool("gzip", false, "gzip output files.")
	skipEmptyFiles = flag.Bool("skip-empty-files", false, "Skip writing empty files")
	topicPollRate  = flag.Duration("topic-refresh", time.Minute, "how frequently the topic list should be refreshed")
	topicPattern   = flag.String("topic-pattern", ".*", "Only log topics matching the following pattern")

	rotateSize     = flag.Int64("rotate-size", 0, "rotate the file when it grows bigger than `rotate-size` bytes")
	rotateInterval = flag.Duration("rotate-interval", 0*time.Second, "rotate the file every duration")

	httpConnectTimeout = flag.Duration("http-client-connect-timeout", 2*time.Second, "timeout for HTTP connect")
	httpRequestTimeout = flag.Duration("http-client-request-timeout", 5*time.Second, "timeout for HTTP request")

	nsqdTCPAddrs     = app.StringArray{}
	lookupdHTTPAddrs = app.StringArray{}
	topics           = app.StringArray{}

	// TODO: remove, deprecated
	gzipCompression = flag.Int("gzip-compression", 3, "(deprecated) use --gzip-level, gzip compression level (1 = BestSpeed, 2 = BestCompression, 3 = DefaultCompression)")
)

func init() {
	flag.Var(&nsqdTCPAddrs, "nsqd-tcp-address", "nsqd TCP address (may be given multiple times)")
	flag.Var(&lookupdHTTPAddrs, "lookupd-http-address", "lookupd HTTP address (may be given multiple times)")
	flag.Var(&topics, "topic", "nsq topic (may be given multiple times)")
}

type FileLogger struct {
	out              *os.File
	writer           io.Writer
	gzipWriter       *gzip.Writer
	logChan          chan *nsq.Message
	compressionLevel int
	gzipEnabled      bool
	filenameFormat   string

	ExitChan chan int
	termChan chan bool
	hupChan  chan bool

	// for rotation
	lastFilename string
	lastOpenTime time.Time
	filesize     int64
	rev          uint
}

type ConsumerFileLogger struct {
	F *FileLogger
	C *nsq.Consumer
}

type TopicDiscoverer struct {
	topics   map[string]*ConsumerFileLogger
	termChan chan os.Signal
	hupChan  chan os.Signal
	wg       sync.WaitGroup
	cfg      *nsq.Config
}

func newTopicDiscoverer(cfg *nsq.Config) *TopicDiscoverer {
	return &TopicDiscoverer{
		topics:   make(map[string]*ConsumerFileLogger),
		termChan: make(chan os.Signal),
		hupChan:  make(chan os.Signal),
		cfg:      cfg,
	}
}

func (f *FileLogger) HandleMessage(m *nsq.Message) error {
	m.DisableAutoResponse()
	f.logChan <- m
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
			if f.needsFileRotate() {
				f.updateFile()
				sync = true
			}
			_, err := f.writer.Write(m.Body)
			if err != nil {
				log.Fatalf("ERROR: writing message to disk - %s", err)
			}
			_, err = f.writer.Write([]byte("\n"))
			if err != nil {
				log.Fatalf("ERROR: writing newline to disk - %s", err)
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
					log.Fatalf("ERROR: failed syncing messages - %s", err)
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
		f.out.Sync()
		if f.gzipWriter != nil {
			f.gzipWriter.Close()
		}
		f.out.Close()
		f.out = nil
	}
}

func (f *FileLogger) Write(p []byte) (n int, err error) {
	f.filesize += int64(len(p))
	return f.out.Write(p)
}

func (f *FileLogger) Sync() error {
	var err error
	if f.gzipWriter != nil {
		f.gzipWriter.Close()
		err = f.out.Sync()
		f.gzipWriter, _ = gzip.NewWriterLevel(f, f.compressionLevel)
		f.writer = f.gzipWriter
	} else {
		err = f.out.Sync()
	}
	return err
}

func (f *FileLogger) calculateCurrentFilename() string {
	t := time.Now()
	datetime := strftime(*datetimeFormat, t)
	return strings.Replace(f.filenameFormat, "<DATETIME>", datetime, -1)
}

func (f *FileLogger) needsFileRotate() bool {
	if f.out == nil {
		return true
	}

	filename := f.calculateCurrentFilename()
	if filename != f.lastFilename {
		log.Printf("INFO: new filename %s, need rotate", filename)
		return true // rotate by filename
	}

	if *rotateInterval > 0 {
		if s := time.Since(f.lastOpenTime); s > *rotateInterval {
			log.Printf("INFO: %s since last open, need rotate", s)
			return true // rotate by interval
		}
	}

	if *rotateSize > 0 && f.filesize > *rotateSize {
		log.Printf("INFO: %s current %d bytes, need rotate", f.out.Name(), f.filesize)
		return true // rotate by size
	}
	return false
}

func (f *FileLogger) updateFile() {
	filename := f.calculateCurrentFilename()
	if filename != f.lastFilename {
		f.rev = 0 // reset revsion to 0 if it is a new filename
	} else {
		f.rev++
	}
	f.lastFilename = filename
	f.lastOpenTime = time.Now()

	fullPath := path.Join(*outputDir, filename)
	dir, _ := filepath.Split(fullPath)
	if dir != "" {
		err := os.MkdirAll(dir, 0770)
		if err != nil {
			log.Fatalf("ERROR: %s Unable to create %s", err, dir)
		}
	}

	f.Close()

	var err error
	var fi os.FileInfo
	for ; ; f.rev++ {
		absFilename := strings.Replace(fullPath, "<REV>", fmt.Sprintf("-%06d", f.rev), -1)
		openFlag := os.O_WRONLY | os.O_CREATE
		if f.gzipEnabled {
			openFlag |= os.O_EXCL
		} else {
			openFlag |= os.O_APPEND
		}
		f.out, err = os.OpenFile(absFilename, openFlag, 0666)
		if err != nil {
			if os.IsExist(err) {
				log.Printf("INFO: file already exists: %s", absFilename)
				continue
			}
			log.Fatalf("ERROR: %s Unable to open %s", err, absFilename)
		}
		log.Printf("INFO: opening %s", absFilename)
		fi, err = f.out.Stat()
		if err != nil {
			log.Fatalf("ERROR: %s Unable to stat file %s", err, f.out.Name())
		}
		f.filesize = fi.Size()
		if f.filesize == 0 {
			break // ok, new file
		}
		if f.needsFileRotate() {
			continue // next rev
		}
		break // ok, don't need rotate
	}

	if f.gzipEnabled {
		f.gzipWriter, _ = gzip.NewWriterLevel(f, f.compressionLevel)
		f.writer = f.gzipWriter
	} else {
		f.writer = f
	}
}

func NewFileLogger(gzipEnabled bool, compressionLevel int, filenameFormat, topic string) (*FileLogger, error) {
	// TODO: remove, deprecated, for compat <GZIPREV>
	filenameFormat = strings.Replace(filenameFormat, "<GZIPREV>", "<REV>", -1)
	if gzipEnabled || *rotateSize > 0 || *rotateInterval > 0 {
		if strings.Index(filenameFormat, "<REV>") == -1 {
			return nil, errors.New("missing <REV> in --filename-format when gzip or rotation enabled")
		}
	} else { // remove <REV> as we don't need it
		filenameFormat = strings.Replace(filenameFormat, "<REV>", "", -1)
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
	filenameFormat = strings.Replace(filenameFormat, "<PID>", fmt.Sprintf("%d", os.Getpid()), -1)
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
	argExist := false
	flag.Visit(func(f *flag.Flag) {
		if f.Name == s {
			argExist = true
		}
	})
	return argExist
}

func newConsumerFileLogger(topic string, cfg *nsq.Config) (*ConsumerFileLogger, error) {
	f, err := NewFileLogger(*gzipEnabled, *gzipLevel, *filenameFormat, topic)
	if err != nil {
		return nil, err
	}

	consumer, err := nsq.NewConsumer(topic, *channel, cfg)
	if err != nil {
		return nil, err
	}

	consumer.AddHandler(f)

	err = consumer.ConnectToNSQDs(nsqdTCPAddrs)
	if err != nil {
		log.Fatal(err)
	}

	err = consumer.ConnectToNSQLookupds(lookupdHTTPAddrs)
	if err != nil {
		log.Fatal(err)
	}

	return &ConsumerFileLogger{
		C: consumer,
		F: f,
	}, nil
}

func (t *TopicDiscoverer) startTopicRouter(logger *ConsumerFileLogger) {
	t.wg.Add(1)
	defer t.wg.Done()
	go logger.F.router(logger.C)
	<-logger.F.ExitChan
}

func (t *TopicDiscoverer) allowTopicName(pattern string, name string) bool {
	match, err := regexp.MatchString(pattern, name)
	if err != nil {
		return false
	}

	return match
}

func (t *TopicDiscoverer) syncTopics(addrs []string, pattern string,
	connectTimeout time.Duration, requestTimeout time.Duration) {
	newTopics, err := clusterinfo.New(nil, http_api.NewClient(nil, connectTimeout, requestTimeout)).GetLookupdTopics(addrs)
	if err != nil {
		log.Printf("ERROR: could not retrieve topic list: %s", err)
	}
	for _, topic := range newTopics {
		if _, ok := t.topics[topic]; !ok {
			if !t.allowTopicName(pattern, topic) {
				log.Println("Skipping topic ", topic, "as it didn't match required pattern:", pattern)
				continue
			}
			logger, err := newConsumerFileLogger(topic, t.cfg)
			if err != nil {
				log.Printf("ERROR: couldn't create logger for new topic %s: %s", topic, err)
				continue
			}
			t.topics[topic] = logger
			go t.startTopicRouter(logger)
		}
	}
}

func (t *TopicDiscoverer) stop() {
	for _, topic := range t.topics {
		topic.F.termChan <- true
	}
}

func (t *TopicDiscoverer) hup() {
	for _, topic := range t.topics {
		topic.F.hupChan <- true
	}
}

func (t *TopicDiscoverer) watch(addrs []string, sync bool, pattern string,
	connectTimeout time.Duration, requestTimeout time.Duration) {
	ticker := time.Tick(*topicPollRate)
	for {
		select {
		case <-ticker:
			if sync {
				t.syncTopics(addrs, pattern, connectTimeout, requestTimeout)
			}
		case <-t.termChan:
			t.stop()
			t.wg.Wait()
			return
		case <-t.hupChan:
			t.hup()
		}
	}
}

func main() {
	cfg := nsq.NewConfig()

	// TODO: remove, deprecated
	flag.Var(&nsq.ConfigFlag{cfg}, "reader-opt", "(deprecated) use --consumer-opt")
	flag.Var(&nsq.ConfigFlag{cfg}, "consumer-opt", "option to passthrough to nsq.Consumer (may be given multiple times, http://godoc.org/github.com/nsqio/go-nsq#Config)")

	flag.Parse()

	if *showVersion {
		fmt.Printf("nsq_to_file v%s\n", version.Binary)
		return
	}

	if *channel == "" {
		log.Fatal("--channel is required")
	}

	connectTimeout := *httpConnectTimeout
	if int64(connectTimeout) <= 0 {
		log.Fatal("--http-client-connect-timeout should be positive")
	}

	requestTimeout := *httpRequestTimeout
	if int64(requestTimeout) <= 0 {
		log.Fatal("--http-client-request-timeout should be positive")
	}

	var topicsFromNSQLookupd bool

	if len(nsqdTCPAddrs) == 0 && len(lookupdHTTPAddrs) == 0 {
		log.Fatal("--nsqd-tcp-address or --lookupd-http-address required.")
	}
	if len(nsqdTCPAddrs) != 0 && len(lookupdHTTPAddrs) != 0 {
		log.Fatal("use --nsqd-tcp-address or --lookupd-http-address not both")
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

	cfg.UserAgent = fmt.Sprintf("nsq_to_file/%s go-nsq/%s", version.Binary, nsq.VERSION)
	cfg.MaxInFlight = *maxInFlight

	discoverer := newTopicDiscoverer(cfg)

	signal.Notify(discoverer.hupChan, syscall.SIGHUP)
	signal.Notify(discoverer.termChan, syscall.SIGINT, syscall.SIGTERM)

	if len(topics) < 1 {
		if len(lookupdHTTPAddrs) < 1 {
			log.Fatal("use --topic to list at least one topic to subscribe to or specify at least one --lookupd-http-address to subscribe to all its topics")
		}
		topicsFromNSQLookupd = true
		var err error
		topics, err = clusterinfo.New(nil, http_api.NewClient(nil, connectTimeout, requestTimeout)).GetLookupdTopics(lookupdHTTPAddrs)
		if err != nil {
			log.Fatalf("ERROR: could not retrieve topic list: %s", err)
		}
	}

	for _, topic := range topics {
		if !discoverer.allowTopicName(*topicPattern, topic) {
			log.Println("Skipping topic", topic, "as it didn't match required pattern:", *topicPattern)
			continue
		}

		logger, err := newConsumerFileLogger(topic, cfg)
		if err != nil {
			log.Fatalf("ERROR: couldn't create logger for topic %s: %s", topic, err)
		}
		discoverer.topics[topic] = logger
		go discoverer.startTopicRouter(logger)
	}

	discoverer.watch(lookupdHTTPAddrs, topicsFromNSQLookupd, *topicPattern, connectTimeout, requestTimeout)
}
