package util

import (
	"log"
	"net"
	"net/http"
	"strings"
)

func HTTPServer(listener net.Listener, handler http.Handler) {
	log.Printf("HTTP: listening on %s", listener.Addr().String())

	server := &http.Server{
		Handler: handler,
	}
	err := server.Serve(listener)
	// theres no direct way to detect this error because it is not exposed
	if err != nil && !strings.Contains(err.Error(), "use of closed network connection") {
		log.Printf("ERROR: http.Serve() - %s", err.Error())
	}

	log.Printf("HTTP: closing %s", listener.Addr().String())
}
