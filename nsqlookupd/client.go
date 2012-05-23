package main

import (
	"bufio"
	"io"
	"log"
	"net"
	"strings"
)

type Client struct {
	conn io.ReadWriteCloser
}

func NewClient(conn net.Conn) *Client {
	return &Client{conn}
}

func (c *Client) Handle() {
	reader := bufio.NewReader(c.conn)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			break
		}

		line = strings.Replace(line, "\n", "", -1)
		line = strings.Replace(line, "\r", "", -1)
		params := strings.Split(line, " ")

		log.Printf("%#v", params)
	}
}
