package server

import (
	"bitbucket.org/shailesh33/dynomite/common"
	"bitbucket.org/shailesh33/dynomite/datastore"
	"bufio"
	"net"
)

type ClientConnHandler struct {
	ConnHandler
}

func NewClientConnHandler(conn net.Conn) (*ClientConnHandler, error) {

	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	c := ClientConnHandler{}
	c.reader = datastore.NewRedisRequestParser(reader)
	c.writer = writer
	c.Notify = make(chan common.Message, 20000)
	return &c, nil
}
