package datastore

import (
	"bitbucket.org/shailesh33/dynomite/common"
	"bufio"
	"log"
	"net"
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

// handle the response read from the datastore conn
func (c DataStoreConn) Handle(r common.Message) error {
	rsp := r.(common.Response)

	log.Println("Handling ", rsp)
	m := <-c.outQueue
	req := m.(common.Request)
	c_conn := (req.GetContext()).(common.Conn)

	// forward the response for the req
	c_conn.Forward(rsp)

	return nil
}

func (c DataStoreConn) forwardedMsgHandle() error {
	var m common.Message
	for m = range c.forwardChan {
		log.Println("Datastore: received a forwarded message from inchan ", m)
		c.outQueue <- m
		m.Write(c.Writer)
	}
	return nil
}

func (c DataStoreConn) Loop() error {
	log.Printf("Running Looop for Datastore %v", c)
	parser := NewResponseParser(bufio.NewReader(c.conn))
	go c.forwardedMsgHandle()
	for {
		var r common.Message
		r, err := parser.GetNextMessage()
		if err != nil {
			log.Println("Datastore: Failed to get next message", err)
			return err
		}
		log.Println("Received message ", r)
		c.Handle(r)
	}
	return nil
}

func (c DataStoreConn) Forward(msg common.Message) error {
	log.Println("Forwarding msg ", msg)
	c.forwardChan <- msg
	return nil
}
