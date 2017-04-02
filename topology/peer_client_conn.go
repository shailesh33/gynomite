package topology

import (
	"github.com/shailesh33/gynomite/common"
	"bufio"
	"fmt"
	"log"
	"net"
)

type PeerClientConn struct {
	conn         net.Conn
	writer       *bufio.Writer
	outQueue     chan PeerMessage
	quit         chan int
	msgForwarder common.MsgForwarder
}

func (c PeerClientConn) String() string {
	return fmt.Sprintf("<Peer Client connection from %s>", c.conn.RemoteAddr())
}

func newPeerClientConnHandler(conn net.Conn, msgForwarder common.MsgForwarder) (common.Conn, error) {
	log.Println("Creating new Peer client conn")
	return PeerClientConn{
		conn:     conn,
		writer:   bufio.NewWriter(conn),
		outQueue: make(chan PeerMessage, 20000),
		quit:     make(chan int), msgForwarder: msgForwarder}, nil
}

func (c *PeerClientConn) responder() {
	for {
		select {
		case m := <-c.outQueue:
			// TODO: There should be timeout in Done
			rsp := m.Done()
			rsp.Write(c.writer)
		case <-c.quit:
			log.Println("Peer Client loop exiting", c)
			return
		}
	}
}

func (c PeerClientConn) Run() error {
	defer c.conn.Close()

	defer func(c PeerClientConn) {
		log.Println("Closing client connection", c)
		close(c.quit)
		//TODO: wait for responder to finish here
	}(c)
	log.Printf("Running Loop for %s", c)

	parser := newPeerMessageParser(bufio.NewReader(c.conn), c)

	go c.responder()
	for {
		req, err := parser.GetNextPeerMessage()
		if err != nil {
			log.Println("Received Error ", err)
			return err
		}
		log.Println("Getting next request to ", c.msgForwarder)

		c.outQueue <- req
		c.msgForwarder.MsgForward(req.M)
	}
	return nil
}

func (c PeerClientConn) MsgForward(m common.Message) error {
	log.Panicf("%s does not implement MsgForward")
	return nil
}

