package topology

import (
	"github.com/shailesh33/gynomite/common"
	"bufio"
	"bytes"
	"fmt"
	"github.com/shailesh33/gynomite/datastore"
	"sync"
	"io"
)

type Handler interface {
	Handle(common.Message) error
}

type MsgForwarder interface {
	MsgForward(common.Message) error
}



type PeerMessage struct {
	 //"   $2014$ <msg_id> <type> <flags> <version> <is_same_dc> *<keylen> <key> *<payload_len>\r\n"
	 // type:
	 // flags: uint8, 0x01 if encrypted
	 // version: 1
	 // is_same_dc: 0 or 1
	 // keylen: length of aes key or 1
	 // key: encrypted aes key or 'd'
	 // payload_len: length of payload
	common.BaseMessage
	Flags   uint8
	Version uint8
	IsSameDC bool
	KeyLength uint16
	Key	string
	PayloadLength uint64
	M	common.Message // holds the message

	//ctx         common.Context

}

func (m PeerMessage) String() string {
	return fmt.Sprintf("<PEER Message %v %s", m.Id, m.M)
}

var peerMessagePool = &sync.Pool{
	New: func() interface{} {
		return new(PeerMessage)
	},
}

func NewPeerMessage(n *Node, m common.Message) *PeerMessage {
	peerMessage := peerMessagePool.Get().(*PeerMessage)
	peerMessage.BaseMessage.Id = common.GetNextId()
	peerMessage.BaseMessage.MsgType = m.GetType()
	peerMessage.Flags = 0
	peerMessage.Version = 1
	peerMessage.IsSameDC = n.isLocalDC
	peerMessage.KeyLength = 1
	peerMessage.Key = "d"
	peerMessage.M = m
	return peerMessage
}


func (m PeerMessage) Write(w io.Writer) error {
	//log.Printf("Sending %+v\n", m)
	// get the length of the payload
	//TODO: try a better way to get the payload length here
	var b1 bytes.Buffer
	tempW := bufio.NewWriter(&b1)
	m.M.Write(tempW)

	fmt.Fprintf(w, "   $2014$ %u %u %u %u %d *%d %s *%d\r\n",int(m.Id), int(m.MsgType), int(m.Flags), int(m.Version), 1, int(m.KeyLength), m.Key, b1.Len())


	w.Write(b1.Bytes())

	return nil

}

func (r PeerMessage) Done() common.Response {
	// TODO: Implement some timeout here
	req := r.M.(common.Request)
	return req.Done()
}

func (r PeerMessage) GetContext() common.Context {
	//return r.ctx
	return nil
}

type PeerMessageParser struct {
	r     *bufio.Reader
	owner common.Context
}

func newPeerMessageParser(r *bufio.Reader, owner common.Context) PeerMessageParser {
	return PeerMessageParser{r: r, owner: owner}
}

func (parser PeerMessageParser) GetNextPeerMessage(placement common.NodePlacement) (*PeerMessage, error) {
	r := parser.r
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	if len(line) == 0 {
		return nil, fmt.Errorf("Empty line")
	}

	var msgId	uint64
	var msgType common.MessageType
	var flags   int
	var version int
	var isSameDC int
	var keyLength int
	var key	string
	var payloadLength int

	//"   $2014$ <msg_id> <type> <flags> <version> <is_same_dc> *<keylen> <key> *<payload_len>\r\n"
	if _, err := fmt.Sscanf(line, "   $2014$ %d %d %d %d %d *%d %s *%d\r\n",
		&msgId, &msgType, &flags, &version, &isSameDC, &keyLength, &key, &payloadLength); err != nil {
		return nil, fmt.Errorf("invalid arguments in ", line)
	}
	m := PeerMessage{
		BaseMessage : common.BaseMessage {
			Id:msgId,
			MsgType: msgType,
		},
		Flags: uint8(flags),
		Version:uint8(version),
		IsSameDC:bool(isSameDC == 1),
		KeyLength:uint16(keyLength),
		Key:key,
		PayloadLength:uint64(payloadLength),

		//ctx:parser.owner,
	}

	// depending on the message type, call the right parser and add it in PeerMessage::m
	switch m.MsgType {
	case common.REQUEST_DATASTORE:
		datastoreParser := datastore.NewRequestParser(parser.r, parser.owner)
		req, err := datastoreParser.GetNextRequest(common.DC_ONE, placement)
		if err != nil {
			return nil, fmt.Errorf("Failed to parse request from peer", err)
		}
		if !m.IsSameDC {
			//log.Println("Overriding routing to", common.ROUTING_LOCAL_DC_ALL_RACKS_TOKEN_OWNER)
			req.SetRoutingOverride(common.ROUTING_LOCAL_DC_ALL_RACKS_TOKEN_OWNER)
			req.SetResponseCounts(1, placement.GetLocalRackCount())
		} else {
			req.SetRoutingOverride(common.ROUTING_LOCAL_NODE_ONLY)
			req.SetResponseCounts(1, 1)
		}
		m.M = req
	}
	return &m, nil
}

func (parser PeerMessageParser) GetNextPeerResponse() (common.Response, error) {
	r := parser.r
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	if len(line) == 0 {
		return nil, fmt.Errorf("Empty line")
	}

	var msgId	uint64
	var msgType common.MessageType
	var flags   int
	var version int
	var isSameDC int
	var keyLength int
	var key	string
	var payloadLength int

	//"   $2014$ <msg_id> <type> <flags> <version> <is_same_dc> *<keylen> <key> *<payload_len>\r\n"
	if _, err := fmt.Sscanf(line, "   $2014$ %d %d %d %d %d *%d %s *%d\r\n",
		&msgId, &msgType, &flags, &version, &isSameDC, &keyLength, &key, &payloadLength); err != nil {
		return nil, fmt.Errorf("invalid arguments in ", line)
	}
	/*m := peerMessagePool.Get().(*PeerMessage)
	m.Id = msgId
	m.MsgType = msgType
	m.Flags = uint8(flags)
	m.Version = uint8(version)
	m.IsSameDC = bool(isSameDC == 1)
	m.KeyLength = uint16(keyLength)
	m.Key = key
	m.PayloadLength = uint64(payloadLength)
*/
	// depending on the message type, call the right parser and add it in PeerMessage::m
	switch msgType {
	case common.RESPONSE_DATASTORE:
		datastoreParser := datastore.NewResponseParser(parser.r)
		rsp, err := datastoreParser.GetNextResponse()
		if err != nil {
			return nil, fmt.Errorf("Failed to parse response from peer", err)
		}

		//m.M = rsp
		return rsp, nil
	}
	return nil, fmt.Errorf("invalid type")
}