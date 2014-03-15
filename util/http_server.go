package util

import (
	"log"
	"net"
	"net/http"
	"strings"
)

func HTTPServer(listener net.Listener, handler http.Handler, proto_name string) {
	log.Printf("%s: listening on %s", proto_name, listener.Addr().String())

	server := &http.Server{
		Handler: handler,
	}
	err := server.Serve(listener)
	// theres no direct way to detect this error because it is not exposed
	if err != nil && !strings.Contains(err.Error(), "use of closed network connection") {
		log.Printf("ERROR: http.Serve() - %s", err.Error())
	}

	log.Printf("%s: closing %s", proto_name, listener.Addr().String())
}
