package consistence

import (
	"fmt"
	"log"
	"os"
	"strings"

	etcdlock "github.com/reechou/xlock"
)

const (
	LOG_DEFAULT = iota
	LOG_ERROR
	LOG_INFO
	LOG_DEBUG
)

var logger *EtcdLogger

func SetEtcdMgrLogger(l *log.Logger) {
	level := LOG_INFO
	logger = &EtcdLogger{log: l, level: level}
	etcdlock.SetLogger(l, level)
}

type EtcdLogger struct {
	log   *log.Logger
	level int
}

func (p *EtcdLogger) Debugf(f string, args ...interface{}) {
	if p.level >= LOG_DEBUG {
		msg := "DEBUG: " + fmt.Sprintf(f, args...)
		// Append newline if necessary
		if !strings.HasSuffix(msg, "\n") {
			msg = msg + "\n"
		}
		p.log.Print(msg)
	}
}

func (p *EtcdLogger) Infof(f string, args ...interface{}) {
	if p.level >= LOG_INFO {
		msg := "Info: " + fmt.Sprintf(f, args...)
		// Append newline if necessary
		if !strings.HasSuffix(msg, "\n") {
			msg = msg + "\n"
		}
		p.log.Print(msg)
	}
}

func (p *EtcdLogger) Errorf(f string, args ...interface{}) {
	if p.level >= LOG_ERROR {
		msg := "Error: " + fmt.Sprintf(f, args...)
		// Append newline if necessary
		if !strings.HasSuffix(msg, "\n") {
			msg = msg + "\n"
		}
		p.log.Print(msg)
	}
}

func init() {
	// Default logger uses the go default log.
	SetEtcdMgrLogger(log.New(os.Stderr, "etcd-leadership", log.LstdFlags))
}
