// +build windows

package main

import (
	"log"
	"os"
	"path/filepath"

	"github.com/nsqio/nsq/internal/winservice"
	"github.com/nsqio/nsq/nsqd"
)

type program struct {
	nsqd *nsqd.NSQD
}

func main() {
	prg := &program{}
	if err := winservice.Run(prg); err != nil {
		log.Fatal(err)
	}
}

func (p *program) Start() error {
	p.nsqd = start()
	if p.nsqd == nil {
		// --version
		os.Exit(0)
	}
	return nil
}

func (p *program) Stop() error {
	if p.nsqd != nil {
		p.nsqd.Exit()
	}
	return nil
}

func (p *program) BeforeStart(e winservice.Environment) error {
	if !e.IsAnInteractiveSession() {
		dir := filepath.Dir(os.Args[0])
		return os.Chdir(dir)
	}
	return nil
}
