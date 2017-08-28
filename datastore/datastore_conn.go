package datastore

import (
	"github.com/shailesh33/gynomite/common"
	"bufio"
	"log"
	"net"
)

type DataStoreConn struct {
	//common.Conn
	conn        net.Conn
	Writer      *bufio.Writer
	forwardChan chan common.IMessage
	outQueue    chan common.IMessage
}

func NewDataStoreConn(conn net.Conn) (DataStoreConn, error) {
	log.Println("Creating new datastore conn")

	return DataStoreConn{
		conn:        conn,
		Writer:      bufio.NewWriter(conn),
		forwardChan: make(chan common.IMessage, 20000),
		outQueue:    make(chan common.IMessage, 20000)}, nil
}

func (c DataStoreConn) forwardRequestsToDatastore() error {
	var m common.IMessage
	for m = range c.forwardChan {
		c.outQueue <- m
		m.Write(c.Writer)
	}
	return nil
}

func (c DataStoreConn) Run() error {
	log.Printf("Running Loop for Datastore %v", c)
	parser := NewResponseParser()
	reader := bufio.NewReader(c.conn)
	go c.forwardRequestsToDatastore()
	for {
		rsp, err := parser.GetNextResponse(reader)
		if err != nil {
			log.Println("Datastore: Failed to get next message", err)
			return err
		}

		// to maintain ordering
		m := <-c.outQueue
		req := m.(common.IRequest)
		req.HandleResponse(rsp)
	}
	return nil
}

func (c DataStoreConn) MsgForward(msg common.IMessage) error {
	c.forwardChan <- msg
	return nil
}
