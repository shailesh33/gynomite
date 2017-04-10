package server

import (
	"github.com/shailesh33/gynomite/common"
	"github.com/shailesh33/gynomite/datastore"
	"bufio"
	"fmt"
	"log"
	"net"
)

type ClientConn struct {
	conn   net.Conn
	Writer *bufio.Writer
	//forwardChan  chan common.Message
	outQueue     chan common.Request
	quit         chan int
	msgForwarder common.MsgForwarder
}

func (c ClientConn) String() string {
	return fmt.Sprintf("<CLIENT from %s>", c.conn.RemoteAddr())
}

func NewClientConnHandler(conn net.Conn, msgForwarder common.MsgForwarder) (common.Conn, error) {
	c := ClientConn{
		conn:   conn,
		Writer: bufio.NewWriter(conn),
		//forwardChan: make(chan common.Message, 20000),
		outQueue: make(chan common.Request, 20000),
		quit:     make(chan int), msgForwarder: msgForwarder}
	log.Printf("New client connection %s", c)
	return c, nil
}

func (c *ClientConn) responder() {
	for {
		select {
		case m := <-c.outQueue:
			// Wait for this request to be done
			req := m.(common.Request)
			rsp := req.Done()
			//log.Printf("Received Response for request %s", req)
			rsp.Write(c.Writer)
		case <-c.quit:
			log.Println("Client loop exiting", c)
			return
		}
	}
}

func (c ClientConn) Run() error {
	defer c.conn.Close()

	defer func(c ClientConn) {
		log.Println("Closing client connection", c)
		close(c.quit)
		//TODO: wait for responder to finish here
	}(c)
	parser := datastore.NewRequestParser(bufio.NewReader(c.conn), c)

	go c.responder()

	for {
		req, err := parser.GetNextRequest()
		if err != nil {
			log.Println("Received Error ", err)
			return err
		}
		c.outQueue <- req
		c.msgForwarder.MsgForward(req)
		//log.Printf("%s Forwarded %s outqueue:%d", c, req, len(c.outQueue))
	}
	return nil
}

func (c ClientConn) MsgForward(m common.Message) error {
	log.Panicf("%s does not implement MsgForward", c)
	return nil
}
