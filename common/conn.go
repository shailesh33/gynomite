package common

import "net"

type Conn interface {
	Run() error
	//Handle(Message) error  // Handle the message read
	MsgForward(Message) error // Forward a message to this connection
}

type ConnHandlerCreator func(net.Conn, MsgForwarder) (Conn, error)
