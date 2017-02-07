package topology

import "strings"

type Node struct {
	token 	string
	addr 	string
	port 	int
	dc_name string
	rack_name string
	is_local bool
	is_same_rack bool
	is_same_dc bool
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

func (n Node) Addr() string {
	return n.addr
}

func (n Node) Port() int {
	return n.port
}