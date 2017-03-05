package topology

import "bitbucket.org/shailesh33/dynomite/common"

type Handler interface {
	Handle(common.Message) error
}

type MsgForwarder interface {
	MsgForward(common.Message) error
}
