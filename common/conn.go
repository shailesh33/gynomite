package common

type Conn interface {
	Run() error
	//Handle(Message) error  // Handle the message read
	Forward(Message) error // Forward a message to this connection
}
