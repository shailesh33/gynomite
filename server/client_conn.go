package server

import (
	"bitbucket.org/shailesh33/dynomite/common"
	"bitbucket.org/shailesh33/dynomite/datastore"
	"bufio"
	"log"
	"net"
)

type ClientConn struct {
	//common.Conn
	conn        net.Conn
	Writer      *bufio.Writer
	forwardChan chan common.Message
	outQueue    chan common.Message
}

func NewClientConnHandler(conn net.Conn) (ClientConn, error) {
	log.Println("Creating new client conn")
	return ClientConn{
		conn:        conn,
		Writer:      bufio.NewWriter(conn),
		forwardChan: make(chan common.Message, 20000),
		outQueue:    make(chan common.Message, 20000)}, nil
}

// handle the request read from the client
func (c ClientConn) Handle(r common.Message) error {
	//req := r.(common.Request)
	//log.Println("Client: Handling ", datastore.RequestTypeDesc[req.GetType()])
	c.outQueue <- r
	datastoreConn := datastore.GetDatastoreConn()
	datastoreConn.Forward(r)
	return nil
}

func (c *ClientConn) forwardedResponseHandle() error {
	for m := range c.forwardChan {
		rsp := m.(common.Response)

		//log.Println("received a message from inchan", m)
		req := <-c.outQueue
		log.Println("Received Response for request ", req)
		rsp.Write(c.Writer)
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
			return err
		}
		//log.Println("Received message ", r)
		c.Handle(r)
	}
	return nil
}

func (c ClientConn) Forward(msg common.Message) error {
	c.forwardChan <- msg
	return nil
}
