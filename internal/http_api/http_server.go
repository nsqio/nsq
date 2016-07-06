package http_api

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/absolute8511/nsq/internal/levellogger"
)

type logWriter struct {
	levellogger.Logger
}

func (l logWriter) Write(p []byte) (int, error) {
	l.Logger.Output(2, string(p))
	return len(p), nil
}

func Serve(listener net.Listener, handler http.Handler, proto string, l levellogger.Logger) {
	l.Output(2, fmt.Sprintf("%s: listening on %s", proto, listener.Addr()))

	server := &http.Server{
		Handler:      handler,
		ErrorLog:     log.New(logWriter{l}, "", 0),
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 60 * time.Second,
	}
	err := server.Serve(listener)
	// theres no direct way to detect this error because it is not exposed
	if err != nil && !strings.Contains(err.Error(), "use of closed network connection") {
		l.Output(2, fmt.Sprintf("ERROR: http.Serve() - %s", err))
	}

	l.Output(2, fmt.Sprintf("%s: closing %s", proto, listener.Addr()))
}
