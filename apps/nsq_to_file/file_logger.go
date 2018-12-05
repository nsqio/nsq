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
	out              *os.File
	writer           io.Writer
	gzipWriter       *gzip.Writer
	logChan          chan *nsq.Message
	compressionLevel int
	gzipEnabled      bool
	filenameFormat   string

	termChan chan bool
	hupChan  chan bool

	// for rotation
	lastFilename string
	lastOpenTime time.Time
	filesize     int64
	rev          uint
}

func NewFileLogger(gzipEnabled bool, compressionLevel int, filenameFormat, topic string) (*FileLogger, error) {
	if gzipEnabled || *rotateSize > 0 || *rotateInterval > 0 || *workDir != *outputDir {
		if strings.Index(filenameFormat, "<REV>") == -1 {
			return nil, errors.New("missing <REV> in --filename-format when gzip, rotation, or work dir enabled")
		}
	} else {
		// remove <REV> as we don't need it
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
		termChan:         make(chan bool),
		hupChan:          make(chan bool),
	}
	return f, nil
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
	ticker := time.NewTicker(*syncInterval)
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

		// Move file from work dir to output dir if necessary, taking care not
		// to overwrite existing files
		if *workDir != *outputDir {
			src := f.out.Name()
			dst := makeOutputPath(src, *workDir, *outputDir)

			// Optimistic rename
			log.Printf("INFO: moving finished file %s to %s", src, dst)
			err := atomicRename(src, dst)
			if err == nil {
				return
			} else if !os.IsExist(err) {
				log.Fatalf("ERROR: unable to move file from %s to %s: %s", src, dst, err)
				return
			}

			// Optimistic rename failed, so we need to generate a new
			// destination file name by bumping the revision number.
			_, filenameTmpl := filepath.Split(f.lastFilename)
			dstDir, _ := filepath.Split(dst)
			dstTmpl := filepath.Join(dstDir, filenameTmpl)

			for i := f.rev + 1; ; i++ {
				log.Printf("INFO: destination file already exists: %s", dst)
				dst := strings.Replace(dstTmpl, "<REV>", fmt.Sprintf("-%06d", i), -1)
				err := atomicRename(src, dst)
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

	fullPath := path.Join(*workDir, filename)
	makeOutputDir(fullPath)

	f.Close()

	var err error
	var fi os.FileInfo
	for ; ; f.rev++ {
		absFilename := strings.Replace(fullPath, "<REV>", fmt.Sprintf("-%06d", f.rev), -1)

		// If we're using a working directory for in-progress files,
		// proactively check for duplicate file names in the output dir to
		// prevent conflicts on rename in the normal case
		if *workDir != *outputDir {
			outputFileName := makeOutputPath(absFilename, *workDir, *outputDir)
			makeOutputDir(outputFileName)

			_, err := os.Stat(outputFileName)
			if err == nil {
				log.Printf("INFO: output file already exists: %s", outputFileName)
				continue // next rev
			} else if !os.IsNotExist(err) {
				log.Fatalf("ERROR: unable to stat output file %s: %s", outputFileName, err)
				return
			}
		}

		openFlag := os.O_WRONLY | os.O_CREATE
		if f.gzipEnabled {
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

// makeOutputFilename translates a file path from work dir to output dir
func makeOutputPath(workPath string, workDir string, outputDir string) string {
	if workDir == outputDir {
		return workPath
	}
	return filepath.Join(outputDir, strings.TrimPrefix(workPath, workDir))
}

func makeOutputDir(path string) {
	dir, _ := filepath.Split(path)
	if dir != "" {
		err := os.MkdirAll(dir, 0770)
		if err != nil {
			log.Fatalf("ERROR: %s Unable to create %s", err, dir)
		}
	}
}

func atomicRename(src, dst string) error {
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
