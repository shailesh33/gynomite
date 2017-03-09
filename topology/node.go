package topology

import (
	"bitbucket.org/shailesh33/dynomite/common"
	"bitbucket.org/shailesh33/dynomite/datastore"
	"log"
	"net"
	"strconv"
	"strings"
	"time"
	"fmt"
)

type NodeState int

const (
	NODE_DOWN NodeState = iota
	NODE_CONNECTED
)

type Node struct {
	token        string
	addr         string
	Port         int
	dc_name      string
	rack_name    string
	is_local     bool
	is_same_rack bool
	is_same_dc   bool
	Handler      common.Conn
	state        NodeState
}

func (n Node) String() string {
	return fmt.Sprintf("<Node %s|%s|%s|%d>", n.dc_name, n.rack_name, n.addr, n.Port)
}

type nodeMap struct {
	m map[string]*Node
}

func newNodeMap() nodeMap {
	return nodeMap{m: make(map[string]*Node)}
}

func (m nodeMap) add(node *Node) {
	m.m[strings.ToLower(node.token)] = node
}

func (m nodeMap) get(nodeToken string) (b *Node, ok bool) {
	b, ok = m.m[strings.ToLower(nodeToken)]
	return
}

func newNode() *Node {
	return &Node{state: NODE_DOWN}
}

func (n *Node) connect() error {

	if n.is_local {
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
		log.Println("Connected to ", n)
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
	log.Printf("Node %s Received %s", n, m)

	if n.is_local {
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

	// write it on the network
	n.Handler.MsgForward(m)
	return nil

}
