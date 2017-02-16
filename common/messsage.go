package common

import (
	"bufio"
)

//////////////////////////
// This is a generic message that flow in the system
type Message interface {
	Handle() error
	Write(w *bufio.Writer) error
}

// This is a response, could be a datastore response on a dnode response
type Response struct {
	Message
}

type RequestType int

// This is a request, could be datastore request from the client of a message to a peer
type Request struct {
	Message
	Name string
	Type RequestType
	Args [][]byte
	//func GetType (RequestType)
	//func GetKey ([]byte)

}

type Parser interface {
	GetNextMessage() (Message, error)
}

/*
// This is an interfact that parses request from the stream of data typically from the client.
type RequestParser interface {
	GetNextRequest() (*Request, error)
}*/
/*
// This is an interfact that parses request from the stream of data typically from the underlying datastore.
type ResponseParser interface {
	GetNextResponse() (*Response, error)
}
*/
