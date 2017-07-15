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
	placement	common.NodePlacement
	writer       *bufio.Writer
	outQueue     chan *PeerMessage
	quit         chan int
	msgForwarder common.MsgForwarder
}

func (c PeerClientConn) String() string {
	return fmt.Sprintf("<Peer Client connection from %s>", c.conn.RemoteAddr())
}

func newPeerClientConn(conn net.Conn, placement common.NodePlacement, msgForwarder common.MsgForwarder) (common.Conn, error) {
	log.Println("Creating new Peer client conn")
	return PeerClientConn{
		conn:     conn,
		placement:	  placement,
		writer:   bufio.NewWriter(conn),
		outQueue: make(chan *PeerMessage, 20000),
		quit:     make(chan int), msgForwarder: msgForwarder}, nil
}

func (c *PeerClientConn) responder() {
	for {
		select {
		case req := <-c.outQueue:
			// TODO: There should be timeout in Done
			//log.Printf("Waiting for response of %s", req)
			rsp := req.Done()
			//log.Printf("Got response of %s", req)
			req.M = rsp
			req.MsgType = common.RESPONSE_DATASTORE
			req.Write(c.writer)
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
		req, err := parser.GetNextPeerMessage(c.placement)
		if err != nil {
			log.Println("Received Error ", err)
			return err
		}
		//log.Printf("Getting next request %+v ", req)

		c.outQueue <- req
		c.msgForwarder.MsgForward(req.M)
	}
	return nil
}

func (c PeerClientConn) MsgForward(m common.Message) error {
	log.Panicf("%s does not implement MsgForward")
	return nil
}

