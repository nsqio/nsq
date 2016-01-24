// +build !windows

package main

import (
	"os"
	"os/signal"
	"syscall"
)

func main() {
	nsqd := start()
	if nsqd == nil {
		return
	}

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)
	<-signalChan

	nsqd.Exit()
}
