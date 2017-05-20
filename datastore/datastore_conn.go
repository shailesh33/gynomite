package datastore

import (
	"github.com/shailesh33/gynomite/common"
	"bufio"
	"log"
	"net"
	"bytes"
	"time"
	"fmt"
)

type DataStoreConn struct {
	//common.Conn
	conn        net.Conn
	Writer      *bufio.Writer
	forwardChan chan common.Message
	outQueue    chan common.Message
}

func NewDataStoreConn(conn net.Conn) (DataStoreConn, error) {
	log.Println("Creating new datastore conn")

	return DataStoreConn{
		conn:        conn,
		Writer:      bufio.NewWriter(conn),
		forwardChan: make(chan common.Message, 20000),
		outQueue:    make(chan common.Message, 20000)}, nil
}

func (c DataStoreConn) batchIntoBuffer(reqs [] common.Message) (net.Buffers, error) {
	var bufs net.Buffers
	for _, req := range reqs {
		var b bytes.Buffer
		writer := bufio.NewWriter(&b)
		req.Write(writer)
		bufs = append(bufs, b.Bytes())
	}
	return bufs, nil
}

func (c DataStoreConn) forwardRequestsToDatastore() error {
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

			buffers, err := c.batchIntoBuffer(reqs)
			if err != nil {
				return fmt.Errorf("Failed to batch requests")
			}

			// Write out the whole buffer
			_, err = buffers.WriteTo(c.conn)
			if err != nil {
				log.Printf("Failed to write batch requests")

			}
			//_, _ = c.Writer.Write(buf.Bytes())
			c.Writer.Flush()
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
/*
	var m common.Message


	for m = range c.forwardChan {
		c.outQueue <- m
		m.Write(c.Writer)
	}*/
	return nil
}

func (c DataStoreConn) Run() error {
	log.Printf("Running Loop for Datastore %v", c)
	parser := NewResponseParser(bufio.NewReader(c.conn))
	go c.forwardRequestsToDatastore()
	for {
		rsp, err := parser.GetNextResponse()
		if err != nil {
			log.Println("Datastore: Failed to get next message", err)
			return err
		}
		// to maintain ordering
		m := <-c.outQueue
		req := m.(common.Request)
		req.HandleResponse(rsp)
	}
	return nil
}

func (c DataStoreConn) MsgForward(msg common.Message) error {
	c.forwardChan <- msg
	return nil
}
