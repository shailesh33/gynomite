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
	//common.Conn
	conn         net.Conn
	Writer       *bufio.Writer
	forwardChan  chan common.Message
	outQueue     chan common.Message
	quit         chan int
	msgForwarder common.MsgForwarder
}

func (c ClientConn) String() string {
	return fmt.Sprintf("<Client connection from %s>", c.conn.RemoteAddr())
}

func NewClientConnHandler(conn net.Conn, msgForwarder common.MsgForwarder) (ClientConn, error) {
	log.Println("Creating new client conn")
	return ClientConn{
		conn:        conn,
		Writer:      bufio.NewWriter(conn),
		forwardChan: make(chan common.Message, 20000),
		outQueue:    make(chan common.Message, 20000),
		quit:        make(chan int), msgForwarder: msgForwarder}, nil
}

// handle the request read from the client
func (c ClientConn) Handle(r common.Message) error {
	req := r.(common.Request)
	c.outQueue <- req
	c.msgForwarder.MsgForward(req)

	//dataStoreConn := datastore.GetDatastoreConn()
	//log.Printf("Client Received %s", req)
	//
	//dataStoreConn.Forward(req)
	return nil
}

func (c *ClientConn) forwardedResponseHandle() error {
	for {
		select {
		case m := <-c.forwardChan:
			rsp := m.(common.Response)
			req := <-c.outQueue
			log.Printf("Received Response for request %s", req)
			rsp.Write(c.Writer)
		case <-c.quit:
			log.Println("Client loop exiting", c)
		}
	}
	return nil
}

func (c ClientConn) Loop() error {
	parser := datastore.NewRequestParser(bufio.NewReader(c.conn), c)

	go c.forwardedResponseHandle()
	for {
		var r common.Message
		r, err := parser.GetNextMessage()
		if err != nil {
			log.Println("Received Error ", err)
			c.quit <- 1
			return err
		}
		c.Handle(r)
	}
	return nil
}

func (c ClientConn) Forward(msg common.Message) error {
	c.forwardChan <- msg
	return nil
}
