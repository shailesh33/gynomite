package topology

import "github.com/shailesh33/gynomite/common"

type Handler interface {
	Handle(common.Message) error
}

type MsgForwarder interface {
	MsgForward(common.Message) error
}
