package topology

import "strings"

type Rack struct {
	name    string
	nodeMap nodeMap
}

type rackMap struct {
	m map[string]Rack
}

func newRackMap() rackMap {
	return rackMap{m: make(map[string]Rack)}
}

func (m rackMap) add(rack Rack) {
	m.m[strings.ToLower(rack.name)] = rack
}

func (m rackMap) get(rackName string) (b Rack, ok bool) {
	b, ok = m.m[strings.ToLower(rackName)]
	return
}

func rack_get_or_create_node(rack Rack, token string) Node {
	node, ok := rack.nodeMap.get(token)
	if ok == true {
		return node
	}
	node = Node{token: token}
	rack.nodeMap.add(node)
	return node
}
