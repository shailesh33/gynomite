package server

import (
	"bitbucket.org/shailesh33/dynomite/common"
	"bufio"
)

type NotificationHandler interface {
	Handle(*common.Message) error
}

type ConnectionHandler struct {
	reader common.Parser
	writer *bufio.Writer
	Notify chan<- *common.Message
}

func (c *ConnectionHandler) Run() error {
	for {
		var r common.Message
		r, err := c.reader.GetNextMessage()
		if err != nil {
			return err
		}
		r.Handle()
	}
}
