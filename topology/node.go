package topology

import (
	"github.com/shailesh33/gynomite/common"
	"github.com/shailesh33/gynomite/datastore"
	"fmt"
	"log"
	"net"
	"strconv"
	"time"
)

type NodeState int

const (
	NODE_DOWN NodeState = iota
	NODE_CONNECTED
)

type Node struct {
	token       uint32
	addr        string
	Port        int
	dcName      string
	rackName    string
	isLocalNode bool
	isLocalRack bool
	isLocalDC   bool
	Handler     common.Conn
	state       NodeState
}

func (n *Node) String() string {
	return fmt.Sprintf("<Node %s|%s|%s|%d>", n.dcName, n.rackName, n.addr, n.Port)
}

func newNode(token uint32, addr string, port int, dcName string, rackName string,
	isLocalDC bool, isLocalRack bool, isLocal bool) *Node {
	return &Node{
		token:       token,
		addr:        addr,
		Port:        port,
		dcName:      dcName,
		rackName:    rackName,
		isLocalDC:   isLocalDC,
		isLocalRack: isLocalRack,
		isLocalNode: isLocal,
		state:       NODE_DOWN,
	}
}

func (n *Node) connect() error {

	if n.isLocalNode {
		return nil
	}
	delay := 1
	var err error
	for i := 0; ; i++ {
		log.Println("Connecting to ", n.addr, n.Port, "Retry attempt ", i, "....")
		conn, err := net.DialTimeout("tcp", net.JoinHostPort(n.addr, strconv.Itoa(n.Port)), 1*time.Second)
		if err != nil {
			time.Sleep(time.Duration(delay) * time.Second)
			delay = delay * 2
			if delay > 10 {
				delay = 10
			}
			continue
		}
		n.state = NODE_CONNECTED
		log.Println("CONNECTED to ", n)
		tcpConn := conn.(*net.TCPConn)
		tcpConn.SetKeepAlive(true)
		tcpConn.SetKeepAlivePeriod(30 * time.Second)
		n.Handler = newPeerConnHandler(conn)
		go n.Handler.Run()
		return nil
	}
	return err
}

func (n *Node) MsgForward(m common.Message) error {
	//log.Printf("Node %s Received %s", n, m)

	if n.isLocalNode {
		req := m.(common.Request)
		// forward to datastore connection
		dataStoreConn := datastore.GetDatastoreConn()
		//log.Printf("Node Received %s", req)

		dataStoreConn.MsgForward(req)
		return nil
	}
	if n.state != NODE_CONNECTED {
		log.Printf("Node %s is not connected", n)

		return nil
	}

	m = NewPeerMessage(m)
	// write it on the network
	n.Handler.MsgForward(m)
	return nil

}