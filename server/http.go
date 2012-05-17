package server

import (
	"../protocol"
	"errors"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"strconv"
)

import _ "net/http/pprof"

type ReqParams struct {
	params url.Values
	body   []byte
}

func NewReqParams(req *http.Request) (*ReqParams, error) {
	reqParams, err := url.ParseQuery(req.URL.RawQuery)
	if err != nil {
		return nil, err
	}

	data, err := ioutil.ReadAll(req.Body)
	if err != nil {
		return nil, err
	}

	return &ReqParams{reqParams, data}, nil
}

func (r *ReqParams) Query(key string) (string, error) {
	keyData := r.params[key]
	if len(keyData) == 0 {
		return "", errors.New("key not in query params")
	}
	return keyData[0], nil
}

func HttpServer(address string, port string, endChan chan int) {
	http.HandleFunc("/ping", pingHandler)
	http.HandleFunc("/put", putHandler)
	http.HandleFunc("/stats", statsHandler)
	go func() {
		fqAddress := address + ":" + port
		log.Printf("listening for http requests on %s", fqAddress)
		err := http.ListenAndServe(fqAddress, nil)
		if err != nil {
			log.Fatal("http.ListenAndServe:", err)
		}
	}()
	<-endChan
}

func pingHandler(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Length", "2")
	io.WriteString(w, "OK")
}

func putHandler(w http.ResponseWriter, req *http.Request) {
	reqParams, err := NewReqParams(req)
	if err != nil {
		log.Printf("HTTP: error - %s", err.Error())
		return
	}

	topicName, err := reqParams.Query("topic")
	if err != nil {
		log.Printf("HTTP: error - %s", err.Error())
		return
	}

	conn := &FakeConn{}
	client := NewClient(conn, "HTTP")
	prot := protocol.Protocols[538990129] // v1
	response, err := prot.Execute(client, "PUB", topicName, string(reqParams.body))
	if err != nil {
		log.Printf("HTTP: error - %s", err.Error())
		return
	}

	w.Header().Set("Content-Length", strconv.Itoa(len(response)))
	w.Write(response)
}
