package server

import (
	"bitbucket.org/shailesh33/dynomite/common"
	"bufio"
	"log"
)

type NotificationHandler interface {
	Handle(*common.Message) error
}

type ConnHandler struct {
	reader common.Parser
	writer *bufio.Writer
	Notify chan common.Message
}

func (c *ConnHandler) InChanHandler() error {
	var m common.Message
	for m = range c.Notify {
		log.Println("received a message from inchan")
		m.Write(c.writer)
	}
	return nil
}

func (c *ConnHandler) Run() error {
	go c.InChanHandler()
	for {
		var r common.Message
		r, err := c.reader.GetNextMessage()
		if err != nil {
			return err
		}
		r.Handle()
	}
	return nil
}
