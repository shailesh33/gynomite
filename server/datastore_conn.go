package server

import (
	"bitbucket.org/shailesh33/dynomite/common"
	"bitbucket.org/shailesh33/dynomite/datastore"
	"bufio"
	"net"
)

type DatastoreConnHanler struct {
	ConnHandler
}

func NewDatastoreConnHanler(conn net.Conn) (*DatastoreConnHanler, error) {

	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	c := DatastoreConnHanler{}
	c.reader = datastore.NewRedisResponseParser(reader)
	c.writer = writer
	c.Notify = make(chan common.Message, 20000)
	return &c, nil
}
