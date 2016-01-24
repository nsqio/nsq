// +build !windows

package main

import (
	"os"
	"os/signal"
	"syscall"
)

func main() {
	nsqlookupd := start()
	if nsqlookupd == nil {
		return
	}

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)
	<-signalChan

	nsqlookupd.Exit()
}
