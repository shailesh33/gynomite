package common

import (
	"bufio"
)

type MessageType int

const (
	REQUEST_DATASTORE MessageType = iota
	RESPONSE_DATASTORE
)

type RoutingOverride int

const (
	ROUTING_DEFAULT RoutingOverride = iota
	ROUTING_LOCAL_NODE_ONLY
	ROUTING_LOCAL_RACK_TOKEN_OWNER
	ROUTING_LOCAL_DC_ALL_RACKS_TOKEN_OWNER
	ROUTING_ALL_DCS_TOKEN_OWNER
)

//////////////////////////
// This is a generic message that flow in the system
type IMessage interface {
	GetId()		uint64
	GetType() MessageType
	Write(w *bufio.Writer) error
}

type Context interface {
}

// This is a request, could be a datastore request or a dnode request
type IRequest interface {
	IMessage
	GetContext() Context
	GetHashCode() uint32
	GetRoutingOverride() RoutingOverride
	SetRoutingOverride(RoutingOverride)
	SetResponseCounts(quorumResponses, maxResponses int)
	Done() IResponse
	HandleResponse(IResponse) error
}

// This is a response, could be a datastore response or a dnode response
type IResponse interface {
	IMessage
}

// This is an interface that parses request from the stream of data typically from the client.
type IRequestParser interface {
	GetNextRequest(Context, *bufio.Reader, Consistency, INodePlacement) (IRequest, error)
}

// This is an interfact that parses request from the stream of data typically from the underlying datastore.
type IResponseParser interface {
	GetNextResponse(*bufio.Reader) (IResponse, error)
}

type BaseMessage struct {
	Id uint64
	MsgType     MessageType
}

func (m BaseMessage) GetId() uint64 {
	return m.Id
}

func (m BaseMessage) GetType() MessageType {
	return m.MsgType
}

type IMsgForwarder interface {
	MsgForward(IMessage) error
}

type INodePlacement interface {
	GetResponseCounts(RoutingOverride, Consistency) (quorumResponses, maxResponses int)
	GetLocalRackCount() int
	GetDCCount() int
}