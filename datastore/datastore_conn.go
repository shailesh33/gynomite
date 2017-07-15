package datastore

import (
	"github.com/shailesh33/gynomite/common"
	"bufio"
	"log"
	"net"
	"io"
)

type DataStoreConn struct {
	//common.Conn
	conn        net.Conn
	Writer      io.Writer
	forwardChan chan common.Message
	outQueue    chan common.Message
}

func NewDataStoreConn(conn net.Conn) (DataStoreConn, error) {
	log.Println("Creating new datastore conn")

	return DataStoreConn{
		conn:        conn,
		Writer:      conn,
		forwardChan: make(chan common.Message, 20000),
		outQueue:    make(chan common.Message, 20000)}, nil
}

func (c DataStoreConn) forwardRequestsToDatastore() error {
	var m common.Message
	for m = range c.forwardChan {
		log.Printf("here, %s", m)
		c.outQueue <- m
		m.Write(c.Writer)
	}
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
		log.Printf("here %s", rsp)
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
