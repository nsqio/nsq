// +build windows

package main

import (
	"log"
	"os"
	"path/filepath"

	"github.com/nsqio/nsq/internal/winservice"
	"github.com/nsqio/nsq/nsqlookupd"
)

type program struct {
	nsqlookupd *nsqlookupd.NSQLookupd
}

func main() {
	prg := &program{}
	if err := winservice.Run(prg); err != nil {
		log.Fatal(err)
	}
}

func (p *program) Start() error {
	p.nsqlookupd = start()
	if p.nsqlookupd == nil {
		// --version
		os.Exit(0)
	}
	return nil
}

func (p *program) Stop() error {
	if p.nsqlookupd != nil {
		p.nsqlookupd.Exit()
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
