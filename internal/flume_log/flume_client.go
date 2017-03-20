package flume_log

import (
	"bufio"
	"errors"
	"log"
	"net"
	"time"
)

type FlumeClient struct {
	remoteAddr string
	conn       net.Conn
	bw         *bufio.Writer
	stopChan   chan bool
	bufList    chan []byte
	loopDone   chan bool
}

func NewFlumeClient(agentIP string) *FlumeClient {
	client := &FlumeClient{
		bufList:    make(chan []byte, 1000),
		stopChan:   make(chan bool),
		remoteAddr: agentIP,
		loopDone:   make(chan bool),
	}

	go client.writeLoop()
	return client
}

func readLoop(conn net.Conn) {
	buf := make([]byte, 1024)
	for {
		_, err := conn.Read(buf)
		if err != nil {
			conn.Close()
			return
		}
	}
}

func (c *FlumeClient) writeLoop() {
	defer func() {
		select {
		case buf := <-c.bufList:
			_, err := c.bw.Write(buf)
			if err != nil {
				log.Printf("write log %v failed: %v, left data: %v", string(buf), err, len(c.bufList))
				break
			}
		default:
		}
		if c.conn != nil {
			c.bw.Flush()
			c.conn.Close()
			c.conn = nil
		}
		close(c.loopDone)
	}()
	c.reconnect()
	ticker := time.NewTicker(time.Second * 3)
	for {
		if c.conn == nil {
			err := c.reconnect()
			if err != nil {
				select {
				case <-time.After(time.Second):
				case <-c.stopChan:
					return
				}
				continue
			}
		}
		select {
		case buf := <-c.bufList:
			_, err := c.bw.Write(buf)
			if err != nil {
				log.Printf("write log %v failed: %v", string(buf), err)
				c.reconnect()
			}
		case <-ticker.C:
			err := c.bw.Flush()
			if err != nil {
				log.Printf("flush write log failed: %v", err)
				c.reconnect()
			}
		case <-c.stopChan:
			return
		}
	}
}

func (c *FlumeClient) SendLog(d []byte) error {
	select {
	case <-c.stopChan:
		return errors.New("flume client stopped")
	case c.bufList <- d:
	default:
		return errors.New("flume client buffer overflowed")
	}
	return nil
}

func (c *FlumeClient) reconnect() (err error) {
	if c.conn != nil {
		c.bw.Flush()
		c.conn.Close()
		c.conn = nil
		c.bw = nil
	}
	log.Printf("reconnect flumelogger to %v ", c.remoteAddr)
	conn, err := net.DialTimeout("tcp", c.remoteAddr, time.Second*5)
	if err != nil {
		log.Printf("connect to %v failed: %v", c.remoteAddr, err)
		return err
	} else {
		c.conn = conn
		c.bw = bufio.NewWriterSize(conn, 1024*8)
		go readLoop(conn)
	}
	return nil
}

func (c *FlumeClient) Stop() {
	if c.stopChan != nil {
		close(c.stopChan)
	}
	<-c.loopDone
}
