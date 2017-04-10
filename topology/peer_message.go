package topology

import (
	"github.com/shailesh33/gynomite/common"
	"bufio"
	"strconv"
	"bytes"
	"fmt"
	"github.com/shailesh33/gynomite/datastore"
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

	ctx         common.Context

}

func (m PeerMessage) String() string {
	return fmt.Sprintf("<PEER Message %v %s", m.Id, m.M)
}

func NewPeerMessage(n *Node, m common.Message) PeerMessage {
	return PeerMessage{
		BaseMessage : common.BaseMessage{
			Id:common.GetNextId(),
			MsgType:m.GetType(),
		},
		Flags:0,
		Version:1,
		IsSameDC:n.isLocalDC,
		KeyLength:1,
		Key:"d",
		M: m,
	}
}


func (m PeerMessage) Write(w *bufio.Writer) error {
	//log.Printf("Sending %+v\n", m)
	w.WriteString("   $2014$ ")
	w.WriteString(strconv.Itoa(int(m.Id)))
	w.WriteString(" ")
	w.WriteString(strconv.Itoa(int(m.MsgType)))
	w.WriteString(" ")
	w.WriteString(strconv.Itoa(int(m.Flags)))
	w.WriteString(" ")
	w.WriteString(strconv.Itoa(int(m.Version)))
	if (m.IsSameDC) {
		w.WriteString(" 1")
	} else {
		w.WriteString(" 0")
	}

	w.WriteString(" *")
	w.WriteString(strconv.Itoa(int(m.KeyLength)))
	w.WriteString(" ")
	w.WriteString(m.Key)
	w.WriteString(" ")
	w.WriteString("*")

	// get the length of the payload
	var b1 bytes.Buffer
	tempW := bufio.NewWriter(&b1)
	m.M.Write(tempW)


	w.WriteString(strconv.Itoa(b1.Len()))
	w.WriteByte('\r')
	w.WriteByte('\n')
	m.M.Write(w)
	w.Flush()

	return nil

}

func (r PeerMessage) Done() common.Response {
	// TODO: Implement some timeout here
	req := r.M.(common.Request)
	return req.Done()
}

func (r PeerMessage) GetContext() common.Context {
	return r.ctx
}

type PeerMessageParser struct {
	r     *bufio.Reader
	owner common.Context
}

func newPeerMessageParser(r *bufio.Reader, owner common.Context) PeerMessageParser {
	return PeerMessageParser{r: r, owner: owner}
}

func (parser PeerMessageParser) GetNextPeerMessage() (PeerMessage, error) {
	r := parser.r
	line, err := r.ReadString('\n')
	if err != nil {
		return PeerMessage{}, err
	}
	if len(line) == 0 {
		return PeerMessage{}, fmt.Errorf("Empty line")
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
		return PeerMessage{}, fmt.Errorf("invalid arguments in ", line)
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

		ctx:parser.owner,
	}

	// depending on the message type, call the right parser and add it in PeerMessage::m
	switch m.MsgType {
	case common.REQUEST_DATASTORE:
		datastoreParser := datastore.NewRequestParser(parser.r, parser.owner)
		req, err := datastoreParser.GetNextRequest()
		if err != nil {
			return PeerMessage{}, fmt.Errorf("Failed to parse request from peer", err)
		}
		if !m.IsSameDC {
			//log.Println("Overriding routing to", common.ROUTING_LOCAL_DC_ALL_RACKS_TOKEN_OWNER)
			req.SetRoutingOverride(common.ROUTING_LOCAL_DC_ALL_RACKS_TOKEN_OWNER)
		} else {
			req.SetRoutingOverride(common.ROUTING_LOCAL_NODE_ONLY)
		}
		m.M = req
	case common.RESPONSE_DATASTORE:
		datastoreParser := datastore.NewResponseParser(parser.r)
		rsp, err := datastoreParser.GetNextResponse()
		if err != nil {
			return PeerMessage{}, fmt.Errorf("Failed to parse response from peer", err)
		}

		m.M = rsp
	}
	return m, nil
}