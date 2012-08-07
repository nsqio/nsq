package main

import (
	"../util"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"time"
)

func httpServer(listener net.Listener, exitSyncChan chan int) {
	log.Printf("HTTP: listening on %s", listener.Addr().String())

	handler := http.NewServeMux()
	handler.HandleFunc("/ping", pingHandler)
	handler.HandleFunc("/lookup", lookupHandler)

	server := &http.Server{
		Handler:      handler,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	}
	err := server.Serve(listener)
	// theres no direct way to detect this error because it is not exposed
	if err != nil && !strings.Contains(err.Error(), "use of closed network connection") {
		log.Printf("ERROR: http.Serve() - %s", err.Error())
	}

	log.Printf("HTTP: closing %s", listener.Addr().String())
	exitSyncChan <- 1
}

func pingHandler(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Length", "2")
	io.WriteString(w, "OK")
}

func lookupHandler(w http.ResponseWriter, req *http.Request) {
	reqParams, err := util.NewReqParams(req)
	if err != nil {
		log.Printf("ERROR: failed to parse request params - %s", err.Error())
		w.Write([]byte(`{"status_code":500, "status_txt":"INVALID_REQUEST", "data":null}`))
		return
	}

	topicName, err := reqParams.Query("topic")
	if err != nil {
		w.Write([]byte(`{"status_code":500, "status_txt":"MISSING_ARG_TOPIC", "data":null}`))
		return
	}

	lookupDataInterface, ok := sm.Get("topic." + topicName)
	if !ok {
		w.Write([]byte(`{"status_code":500, "status_txt":"INVALID_ARG_TOPIC", "data":null}`))
		return
	}
	lookupData := lookupDataInterface.(map[string]interface{})

	// hijack the request so we can access the connection directly (in order to get the local addr)
	hj, ok := w.(http.Hijacker)
	if !ok {
		w.Write([]byte(`{"status_code":500, "status_txt":"INTERNAL_ERROR", "data":null}`))
		return
	}

	conn, bufrw, err := hj.Hijack()
	if err != nil {
		w.Write([]byte(`{"status_code":500, "status_txt":"INTERNAL_ERROR", "data":null}`))
		return
	}
	defer conn.Close()

	data := make(map[string]interface{})
	data["channels"] = lookupData["channels"]

	// for each producer try to identify the optimal address based on the ones announced
	// to lookupd.  if none are optimal send the hostname (last entry)
	producers := make([]map[string]interface{}, 0)
	for _, entry := range lookupData["producers"].([]map[string]interface{}) {
		preferLocal, preferV6, preferredNetwork, err := identifyNetworkPreferences(conn, entry["id"].(string))
		if err != nil {
			w.Write([]byte(`{"status_code":500, "status_txt":"INTERNAL_ERROR", "data":null}`))
			return
		}

		chosen, err := identifyBestAddress(entry["ips"].([]string), preferLocal, preferV6, preferredNetwork)
		if err != nil {
			w.Write([]byte(`{"status_code":500, "status_txt":"INTERNAL_ERROR", "data":null}`))
			return
		}

		producer := make(map[string]interface{})
		producer["address"] = chosen
		producer["port"] = entry["port"]
		producers = append(producers, producer)
	}
	data["producers"] = producers

	output := make(map[string]interface{})
	output["data"] = data
	output["status_code"] = 200
	output["status_txt"] = "OK"
	response, err := json.Marshal(&output)
	if err != nil {
		response = []byte(`{"status_code":500, "status_txt":"INVALID_JSON", "data":null}`)
	}

	resp := fmt.Sprintf("HTTP/1.1 200 OK\r\nContent-Length: %d\r\nConnection: close\r\nContent-Type: application/json; charset=utf-8\r\n\r\n%s", len(response), response)
	bufrw.WriteString(resp)
	bufrw.Flush()
}

func identifyNetworkPreferences(conn net.Conn, address string) (bool, bool, *net.IPNet, error) {
	var preferredNetwork *net.IPNet

	remoteIP := conn.RemoteAddr().(*net.TCPAddr).IP
	connLocalIP := conn.LocalAddr().(*net.TCPAddr).IP

	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return false, false, nil, err
	}
	ip := net.ParseIP(host)

	log.Printf("remoteIP: %s", remoteIP)
	log.Printf("connLocalIP: %s", connLocalIP)
	log.Printf("ip: %s", ip)

	interfaceAddrs, err := net.InterfaceAddrs()
	if err != nil {
		return false, false, nil, err
	}

	// try to find the interface that both the client and nsqd connected on
	for _, intAddr := range interfaceAddrs {
		addrIp, netMask, err := net.ParseCIDR(intAddr.String())
		if err != nil {
			return false, false, nil, err
		}
		if !addrIp.IsLinkLocalMulticast() && !addrIp.IsLinkLocalUnicast() {
			log.Printf("interface: %s - netmask - %s", intAddr, netMask)
			if netMask.Contains(ip) && netMask.Contains(connLocalIP) {
				preferredNetwork = netMask
			}
		}
	}

	preferLocal := remoteIP.IsLoopback()
	preferV6 := strings.Contains(remoteIP.String(), ":")

	log.Printf("preferLocal: %v", preferLocal)
	log.Printf("preferV6: %v", preferV6)
	log.Printf("preferredNetwork: %v", preferredNetwork)

	return preferLocal, preferV6, preferredNetwork, nil
}

func identifyBestAddress(ips []string, preferLocal bool, preferV6 bool, preferredNetwork *net.IPNet) (string, error) {
	for i, address := range ips {
		if i == len(ips)-1 {
			// last entry is always hostname
			return address, nil
		}

		ip := net.ParseIP(address)
		if preferLocal && ip.IsLoopback() {
			return ip.String(), nil
		}

		if preferV6 && strings.Contains(ip.String(), ":") {
			return ip.String(), nil
		}

		if preferredNetwork != nil && preferredNetwork.Contains(ip) {
			return ip.String(), nil
		}
	}

	// should be impossible?
	return "", errors.New("no address available")
}
