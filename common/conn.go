package common

import (
	"net"
)
// Conn Connection interface
type Conn interface {
	Run() error
	//Handle(Message) error  // Handle the message read
	MsgForward(IMessage) error // Forward a message to this connection
}

// Consistency type of consistency
type Consistency int

const (
	DC_ONE Consistency = iota
	DC_QUORUM
	DC_SAFE_QUORUM
)

type ConnCreator func(net.Conn, INodePlacement, IMsgForwarder) (Conn, error)
