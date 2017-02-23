package common

/* Another connection will call forward to forward a request or a response to this connection */
type Forwarder interface {
	Forward(Message) error
}

/* Once a message (req or rsp) is received, the connections handle function is called */
type MsgHandler interface {
	Handle(Message) error
}

/* Handler for a message forwarded by another connection on the node */
type ForwardedMsgHandler interface {
	ForwardedMsgHandle(Message) error
}

type Conn interface {
	Loop() error
	Handle(Message) error  // Handle the message read
	Forward(Message) error // Forward a message to this connection

	//Writer              *bufio.Writer // Initialize. writes to a tcp connection, either a req, rsp or a dnode msg
	//	MsgHandler                        // Implement. handles a message received from the tcp connection
	//	Forwarder                         // Implemented.
	//ForwardChan         chan *Message // Initialize
	//Outqueue            chan *Message
}
