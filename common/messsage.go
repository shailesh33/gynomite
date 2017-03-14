package common

import (
	"bufio"
)

type MessageType int

const (
	REQUEST_DATASTORE MessageType = iota
)

type RoutingOverride int

const (
	ROUTING_NORMAL RoutingOverride = iota
	ROUTING_LOCAL_NODE_ONLY
	ROUTING_LOCAL_RACK_TOKEN_OWNER
	ROUTING_LOCAL_DC_ALL_RACKS_TOKEN_OWNER
	ROUTING_ALL_DCS_TOKEN_OWNER
	ROUTING_ALL_DCS_ALL_NODES
)

//////////////////////////
// This is a generic message that flow in the system
type Message interface {
	Write(w *bufio.Writer) error
}

type Context interface {
}

// This is a request, could be a datastore request or a dnode request
type Request interface {
	Message
	GetContext() Context
	GetType() MessageType
	GetName() string
	GetKey() []byte
	GetHashCode() uint32
	String() string
	GetRoutingOverride() RoutingOverride
	Done() Response
	HandleResponse(Response) error
}

// This is a response, could be a datastore response or a dnode response
type Response interface {
	Message
}

// This is an interface that parses request from the stream of data typically from the client.
type RequestParser interface {
	GetNextRequest() (Request, error)
}

// This is an interfact that parses request from the stream of data typically from the underlying datastore.
type ResponseParser interface {
	GetNextResponse() (Response, error)
}

type MsgForwarder interface {
	MsgForward(Message) error
}