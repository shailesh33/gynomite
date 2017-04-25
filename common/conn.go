package common

import (
	"net"
)

type Conn interface {
	Run() error
	//Handle(Message) error  // Handle the message read
	MsgForward(Message) error // Forward a message to this connection
}

type Consistency int

const (
	DC_ONE Consistency = iota
	DC_QUORUM
	DC_SAFE_QUORUM
)

type ConnCreator func(net.Conn, NodePlacement, MsgForwarder) (Conn, error)
