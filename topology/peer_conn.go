package topology

import (
	"github.com/shailesh33/gynomite/common"
	"bufio"
	"fmt"
	"log"
	"net"
)

// Implement a connection handle
type PeerConn struct {
	conn        net.Conn
	writer      *bufio.Writer
	outQueue    chan common.Message
	forwardChan chan common.Message
	quit        chan int
}

func (c PeerConn) String() string {
	return fmt.Sprintf("<Peer connection To %s>", c.conn.RemoteAddr())
}

func newPeerConn(conn net.Conn) common.Conn {
	log.Println("Creating new peer conn")
	return PeerConn{
		conn:        conn,
		writer:      bufio.NewWriter(conn),
		forwardChan: make(chan common.Message, 20000),
		outQueue:    make(chan common.Message, 20000),
		quit:        make(chan int),
	}
}

func (c PeerConn) forwardRequestsToPeer() error {
	var m common.Message

	for m = range c.forwardChan {
		c.outQueue <- m
		//log.Println("Forwarded", m, " to", c, " outqueue:", len(c.outQueue))
		err := m.Write(c.writer)
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
	//parser := datastore.NewResponseParser(bufio.NewReader(c.conn))
	parser := newPeerMessageParser(bufio.NewReader(c.conn), c)
	go c.forwardRequestsToPeer()
	for {
		//log.Printf("%s Waiting for response", c)
		rsp, err := parser.GetNextPeerMessage()

		if err != nil {
			log.Println("Received Error ", err)
			return err
		}

		// to maintain ordering
		m := <-c.outQueue
		//log.Printf("%s Received response for %s", c, m)
		peerMessage := m.(PeerMessage)
		req := peerMessage.M.(common.Request)
		req.HandleResponse(rsp.M)
	}
	return nil
}

func (c PeerConn) MsgForward(msg common.Message) error {
	c.forwardChan <- msg
	return nil
}
