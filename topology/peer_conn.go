package topology

import (
	"github.com/shailesh33/gynomite/common"
	"bufio"
	"fmt"
	"log"
	"net"
	"time"
	"bytes"
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

func (c PeerConn) batchIntoBuffer(reqs [] common.Message) (bytes.Buffer, error) {
	var b bytes.Buffer
	writer := bufio.NewWriter(&b)

	for _, req := range reqs {
		req.Write(writer)
	}
	return b, nil
}


func (c PeerConn) forwardRequestsToPeer() error {
	var req common.Message
	var reqs []common.Message
	var batchTimeout <-chan time.Time
	var timedout bool
	batchSize := 10

	for {
		if batchTimeout == nil {
			batchTimeout = time.After(50 * time.Microsecond)
		}

		select {
		case req = <-c.forwardChan:
			c.outQueue <- req
			timedout = false
		// queue up the request
			reqs = append(reqs, req)

		case <-batchTimeout:
			timedout = true
		}

		// After the batch delay we want to get the requests that do exist moving along
		// Or, if there's enough to batch together, send them off
		if (timedout && len(reqs) > 0) || len(reqs) >= int(batchSize) {

			// Set batch timeout channel nil to reset it. Next batch will get a new timeout.
			batchTimeout = nil

			buf, err := c.batchIntoBuffer(reqs)
			if err != nil {
				return fmt.Errorf("Failed to batch requests")
			}

			// Write out the whole buffer
			_, _ = c.writer.Write(buf.Bytes())
			c.writer.Flush()
			reqs = reqs[:0]
		}

		// block until a request comes in if there's a timeout earlier so this doesn't constantly spin
		if timedout {
			req = <-c.forwardChan
			c.outQueue <- req
			reqs = append(reqs, req)

			// Reset timeout variables to base state
			timedout = false
			batchTimeout = nil
		}
	}
	//var m common.Message
	//
	//for m = range c.forwardChan {
	//	c.outQueue <- m
	//	err := m.Write(c.writer)
	//	if err != nil {
	//		log.Println("Error while sending to peer", err)
	//	}
	//}
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
		rsp, err := parser.GetNextPeerResponse()

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
