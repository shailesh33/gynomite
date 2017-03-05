package common

type Conn interface {
	Loop() error
	Handle(Message) error  // Handle the message read
	Forward(Message) error // Forward a message to this connection
}
