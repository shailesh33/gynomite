package topology

import (
	"bitbucket.org/shailesh33/dynomite/common"
	"bitbucket.org/shailesh33/dynomite/datastore"
	"log"
	"net"
	"strconv"
	"strings"
	"time"
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
	conn         net.Conn
	state        NodeState
}

type nodeMap struct {
	m map[string]Node
}

func newNodeMap() nodeMap {
	return nodeMap{m: make(map[string]Node)}
}

func (m nodeMap) add(node Node) {
	m.m[strings.ToLower(node.token)] = node
}

func (m nodeMap) get(nodeToken string) (b Node, ok bool) {
	b, ok = m.m[strings.ToLower(nodeToken)]
	return
}

func newNode() Node {
	return Node{state: NODE_DOWN}
}

func (n Node) connect() error {

	if n.is_local {
		return nil
	}
	delay := 1
	var err error
	for i := 0; ; i++ {
		log.Println("Connecting to ", n.addr, n.Port, "Retry attempt ", i, "....")
		n.conn, err = net.DialTimeout("tcp", net.JoinHostPort(n.addr, strconv.Itoa(n.Port)), 1*time.Second)
		if err != nil {
			time.Sleep(time.Duration(delay) * time.Second)
			delay = delay * 2
			if delay > 10 {
				delay = 10
			}
			continue
		}
		log.Println("Connected to ", n.addr, n.Port)
		n.state = NODE_CONNECTED
		return nil
	}
	//go n.connect()
	return err
}

func (n Node) MsgForward(m common.Message) {
	if n.is_local {
		req := m.(common.Request)
		// forward to datastore connection
		dataStoreConn := datastore.GetDatastoreConn()
		log.Printf("Node Received %s", req)

		dataStoreConn.Forward(req)
		return
	}
	if n.state != NODE_CONNECTED {
		return
	}
	// write it on the network

}
