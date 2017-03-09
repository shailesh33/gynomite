package topology

import (
	"bitbucket.org/shailesh33/dynomite/common"
	"bitbucket.org/shailesh33/dynomite/datastore"
	"bufio"
	"fmt"
	"log"
	"net"
)

// Implement a connection handle
type PeerConn struct {
	conn        net.Conn
	Writer      *bufio.Writer
	outQueue    chan common.Message
	forwardChan chan common.Message
	quit        chan int
}

func (c PeerConn) String() string {
	return fmt.Sprintf("<Peer connection To %s>", c.conn.RemoteAddr())
}

func newPeerConnHandler(conn net.Conn) common.Conn {
	log.Println("Creating new peer conn")
	return PeerConn{
		conn:        conn,
		Writer:      bufio.NewWriter(conn),
		forwardChan: make(chan common.Message, 20000),
		outQueue:    make(chan common.Message, 20000),
		quit:        make(chan int)}
}

func (c PeerConn) forwardRequestsToPeer() error {
	var m common.Message

	for m = range c.forwardChan {
		log.Println("Forwarding", m, " to", c)
		c.outQueue <- m
		err := m.Write(c.Writer)
		if err != nil {
			log.Println("Error while sending to peer", err)
		}
	}
	log.Printf("Peer loop exiting %s", c)

	return nil
}

func (c PeerConn) Run() error {
	defer func() {
		close(c.outQueue)
		close(c.forwardChan)
	}()

	log.Printf("Running Loop for %s", c)
	parser := datastore.NewResponseParser(bufio.NewReader(c.conn))
	go c.forwardRequestsToPeer()
	for {
		_, err := parser.GetNextResponse()
		if err != nil {
			log.Println("Received Error ", err)
			return err
		}

		// to maintain ordering
		<-c.outQueue
		//req := m.(common.Request)
		//req.HandleResponse(rsp)
	}
	return nil
}

func (c PeerConn) MsgForward(msg common.Message) error {
	c.forwardChan <- msg
	return nil
}
