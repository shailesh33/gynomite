package server

import (
	"bitbucket.org/shailesh33/dynomite/common"
	"bitbucket.org/shailesh33/dynomite/datastore"
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
	return fmt.Sprintf("<Client connection from %s>", c.conn.RemoteAddr())
}

func NewClientConnHandler(conn net.Conn, msgForwarder common.MsgForwarder) (ClientConn, error) {
	log.Println("Creating new client conn")
	return ClientConn{
		conn:   conn,
		Writer: bufio.NewWriter(conn),
		//forwardChan: make(chan common.Message, 20000),
		outQueue: make(chan common.Request, 20000),
		quit:     make(chan int), msgForwarder: msgForwarder}, nil
}

func (c *ClientConn) responder() {
	for {
		select {
		case m := <-c.outQueue:
			// Wait for this request to be done
			req := m.(common.Request)
			// TODO: There should be timeout in Done
			rsp := req.Done()
			//log.Printf("Received Response for request %s", req)
			rsp.Write(c.Writer)
		case <-c.quit:
			log.Println("Client loop exiting", c)
		}
	}
}

func (c ClientConn) Run() error {
	defer func(c ClientConn) {
		log.Println("Closing client connection", c)
		c.quit <- 1
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
	}
	return nil
}

//func (c ClientConn) Forward(msg common.Message) error {
//	c.forwardChan <- msg
//	return nil
//}
