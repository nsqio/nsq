package flume_log

import (
	"errors"
	"log"
	"net"
	"time"
)

type FlumeClient struct {
	remoteAddr string
	conn       net.Conn
	stopChan   chan bool
	bufList    chan []byte
	loopDone   chan bool
}

func NewFlumeClient(agentIP string) *FlumeClient {
	client := &FlumeClient{
		bufList:    make(chan []byte, 100),
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
			_, err := c.conn.Write(buf)
			if err != nil {
				log.Printf("write log %v failed: %v, left data: %v", string(buf), err, len(c.bufList))
				break
			}
		default:
		}
		if c.conn != nil {
			c.conn.Close()
		}
		close(c.loopDone)
	}()
	c.reconnect()
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
			_, err := c.conn.Write(buf)
			if err != nil {
				log.Printf("write log %v failed: %v", string(buf), err)
				c.reconnect()
			}
		case <-c.stopChan:
			return
		}
	}
}

func (c *FlumeClient) SendLog(log_info *LogInfo) error {
	d := log_info.Serialize()
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
		c.conn.Close()
		c.conn = nil
	}
	conn, err := net.DialTimeout("tcp", c.remoteAddr, time.Second*5)
	if err != nil {
		log.Printf("connect to %v failed: %v", c.remoteAddr, err)
		return err
	} else {
		c.conn = conn
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
