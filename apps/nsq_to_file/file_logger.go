package main

import (
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/nsqio/go-nsq"
)

type FileLogger struct {
	opts     *Options
	consumer *nsq.Consumer

	out            *os.File
	writer         io.Writer
	gzipWriter     *gzip.Writer
	logChan        chan *nsq.Message
	filenameFormat string

	termChan chan bool
	hupChan  chan bool

	// for rotation
	filename string
	openTime time.Time
	filesize int64
	rev      uint
}

func NewFileLogger(opts *Options, topic string, cfg *nsq.Config) (*FileLogger, error) {
	computedFilenameFormat, err := computeFilenameFormat(opts, topic)
	if err != nil {
		return nil, err
	}

	consumer, err := nsq.NewConsumer(topic, opts.Channel, cfg)
	if err != nil {
		return nil, err
	}

	f := &FileLogger{
		opts:           opts,
		consumer:       consumer,
		logChan:        make(chan *nsq.Message, 1),
		filenameFormat: computedFilenameFormat,
		termChan:       make(chan bool),
		hupChan:        make(chan bool),
	}
	consumer.AddHandler(f)

	err = consumer.ConnectToNSQDs(opts.NSQDTCPAddrs)
	if err != nil {
		return nil, err
	}

	err = consumer.ConnectToNSQLookupds(opts.NSQLookupdHTTPAddrs)
	if err != nil {
		return nil, err
	}

	return f, nil
}

func (f *FileLogger) HandleMessage(m *nsq.Message) error {
	m.DisableAutoResponse()
	f.logChan <- m
	return nil
}

func (f *FileLogger) router() {
	pos := 0
	output := make([]*nsq.Message, f.opts.MaxInFlight)
	sync := false
	ticker := time.NewTicker(f.opts.SyncInterval)
	closeFile := false
	exit := false

	for {
		select {
		case <-f.consumer.StopChan:
			sync = true
			closeFile = true
			exit = true
		case <-f.termChan:
			ticker.Stop()
			f.consumer.Stop()
			sync = true
		case <-f.hupChan:
			sync = true
			closeFile = true
		case <-ticker.C:
			if f.needsRotation() {
				if f.opts.SkipEmptyFiles {
					closeFile = true
				} else {
					f.updateFile()
				}
			}
			sync = true
		case m := <-f.logChan:
			if f.needsRotation() {
				f.updateFile()
				sync = true
			}
			_, err := f.Write(m.Body)
			if err != nil {
				log.Fatalf("ERROR: writing message to disk - %s", err)
			}
			_, err = f.Write([]byte("\n"))
			if err != nil {
				log.Fatalf("ERROR: writing newline to disk - %s", err)
			}
			output[pos] = m
			pos++
			if pos == cap(output) {
				sync = true
			}
		}

		if sync || f.consumer.IsStarved() {
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
			break
		}
	}
}

func (f *FileLogger) Close() {
	if f.out == nil {
		return
	}

	f.out.Sync()
	if f.gzipWriter != nil {
		f.gzipWriter.Close()
	}
	f.out.Close()

	// Move file from work dir to output dir if necessary, taking care not
	// to overwrite existing files
	if f.opts.WorkDir != f.opts.OutputDir {
		src := f.out.Name()
		dst := filepath.Join(f.opts.OutputDir, strings.TrimPrefix(src, f.opts.WorkDir))

		// Optimistic rename
		log.Printf("INFO: moving finished file %s to %s", src, dst)
		err := exclusiveRename(src, dst)
		if err == nil {
			return
		} else if !os.IsExist(err) {
			log.Fatalf("ERROR: unable to move file from %s to %s: %s", src, dst, err)
			return
		}

		// Optimistic rename failed, so we need to generate a new
		// destination file name by bumping the revision number.
		_, filenameTmpl := filepath.Split(f.filename)
		dstDir, _ := filepath.Split(dst)
		dstTmpl := filepath.Join(dstDir, filenameTmpl)

		for i := f.rev + 1; ; i++ {
			log.Printf("INFO: destination file already exists: %s", dst)
			dst := strings.Replace(dstTmpl, "<REV>", fmt.Sprintf("-%06d", i), -1)
			err := exclusiveRename(src, dst)
			if err != nil {
				if os.IsExist(err) {
					continue // next rev
				}
				log.Fatalf("ERROR: unable to rename file from %s to %s: %s", src, dst, err)
				return
			}
			log.Printf("INFO: renamed finished file %s to %s to avoid overwrite", src, dst)
			break
		}
	}

	f.out = nil
}

func (f *FileLogger) Write(p []byte) (int, error) {
	n, err := f.writer.Write(p)
	f.filesize += int64(n)
	return n, err
}

func (f *FileLogger) Sync() error {
	var err error
	if f.gzipWriter != nil {
		f.gzipWriter.Close()
		err = f.out.Sync()
		f.gzipWriter, _ = gzip.NewWriterLevel(f, f.opts.GZIPLevel)
		f.writer = f.gzipWriter
	} else {
		err = f.out.Sync()
	}
	return err
}

func (f *FileLogger) currentFilename() string {
	t := time.Now()
	datetime := strftime(f.opts.DatetimeFormat, t)
	return strings.Replace(f.filenameFormat, "<DATETIME>", datetime, -1)
}

func (f *FileLogger) needsRotation() bool {
	if f.out == nil {
		return true
	}

	filename := f.currentFilename()
	if filename != f.filename {
		log.Printf("INFO: new filename %s, rotating...", filename)
		return true // rotate by filename
	}

	if f.opts.RotateInterval > 0 {
		if s := time.Since(f.openTime); s > f.opts.RotateInterval {
			log.Printf("INFO: %s since last open, rotating...", s)
			return true // rotate by interval
		}
	}

	if f.opts.RotateSize > 0 && f.filesize > f.opts.RotateSize {
		log.Printf("INFO: %s currently %d bytes (> %d), rotating...",
			f.out.Name(), f.filesize, f.opts.RotateSize)
		return true // rotate by size
	}

	return false
}

func (f *FileLogger) updateFile() {
	filename := f.currentFilename()
	if filename != f.filename {
		f.rev = 0 // reset revision to 0 if it is a new filename
	} else {
		f.rev++
	}
	f.filename = filename
	f.openTime = time.Now()

	fullPath := path.Join(f.opts.WorkDir, filename)
	makeDirFromPath(fullPath)

	f.Close()

	var err error
	var fi os.FileInfo
	for ; ; f.rev++ {
		absFilename := strings.Replace(fullPath, "<REV>", fmt.Sprintf("-%06d", f.rev), -1)

		// If we're using a working directory for in-progress files,
		// proactively check for duplicate file names in the output dir to
		// prevent conflicts on rename in the normal case
		if f.opts.WorkDir != f.opts.OutputDir {
			outputFileName := filepath.Join(f.opts.OutputDir, strings.TrimPrefix(absFilename, f.opts.WorkDir))
			makeDirFromPath(outputFileName)

			_, err := os.Stat(outputFileName)
			if err == nil {
				log.Printf("INFO: output file already exists: %s", outputFileName)
				continue // next rev
			} else if !os.IsNotExist(err) {
				log.Fatalf("ERROR: unable to stat output file %s: %s", outputFileName, err)
			}
		}

		openFlag := os.O_WRONLY | os.O_CREATE
		if f.opts.GZIP {
			openFlag |= os.O_EXCL
		} else {
			openFlag |= os.O_APPEND
		}
		f.out, err = os.OpenFile(absFilename, openFlag, 0666)
		if err != nil {
			if os.IsExist(err) {
				log.Printf("INFO: working file already exists: %s", absFilename)
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

		if f.needsRotation() {
			continue // next rev
		}

		break // ok, don't need rotate
	}

	if f.opts.GZIP {
		f.gzipWriter, _ = gzip.NewWriterLevel(f.out, f.opts.GZIPLevel)
		f.writer = f.gzipWriter
	} else {
		f.writer = f.out
	}
}

func makeDirFromPath(path string) {
	dir, _ := filepath.Split(path)
	if dir != "" {
		err := os.MkdirAll(dir, 0770)
		if err != nil {
			log.Fatalf("ERROR: %s Unable to create %s", err, dir)
		}
	}
}

func exclusiveRename(src, dst string) error {
	err := os.Link(src, dst)
	if err != nil {
		return err
	}

	err = os.Remove(src)
	if err != nil {
		return err
	}

	return nil
}

func computeFilenameFormat(opts *Options, topic string) (string, error) {
	hostname, err := os.Hostname()
	if err != nil {
		return "", err
	}
	shortHostname := strings.Split(hostname, ".")[0]

	identifier := shortHostname
	if len(opts.HostIdentifier) != 0 {
		identifier = strings.Replace(opts.HostIdentifier, "<SHORT_HOST>", shortHostname, -1)
		identifier = strings.Replace(identifier, "<HOSTNAME>", hostname, -1)
	}

	cff := opts.FilenameFormat
	if opts.GZIP || opts.RotateSize > 0 || opts.RotateInterval > 0 || opts.WorkDir != opts.OutputDir {
		if strings.Index(cff, "<REV>") == -1 {
			return "", errors.New("missing <REV> in --filename-format when gzip, rotation, or work dir enabled")
		}
	} else {
		// remove <REV> as we don't need it
		cff = strings.Replace(cff, "<REV>", "", -1)
	}
	cff = strings.Replace(cff, "<TOPIC>", topic, -1)
	cff = strings.Replace(cff, "<HOST>", identifier, -1)
	cff = strings.Replace(cff, "<PID>", fmt.Sprintf("%d", os.Getpid()), -1)
	if opts.GZIP && !strings.HasSuffix(cff, ".gz") {
		cff = cff + ".gz"
	}

	return cff, nil
}
