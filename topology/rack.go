package topology

import (
	"bitbucket.org/shailesh33/dynomite/common"
	"fmt"
	"log"
	"sort"
)

type Rack struct {
	name        string
	nodeMap     map[int]*Node
	isLocalDC   bool
	isLocalRack bool
	tokens      []int
}

func newRack(name string, isLocalDC bool, isLocalRack bool) *Rack {
	return &Rack{
		name:        name,
		isLocalDC:   isLocalDC,
		nodeMap:     make(map[int]*Node),
		isLocalRack: isLocalRack,
		tokens:      make([]int, 0, 3),
	}
}

func (r *Rack) getNode(token int) (*Node, error) {
	node, ok := r.nodeMap[token]
	if ok == true {
		return node, nil
	}
	return &Node{}, fmt.Errorf("Node not found with token %d in rack %s", token, r.name)
}

func (r *Rack) addNode(node *Node) error {
	if _, err := r.getNode(node.token); err == nil {
		return fmt.Errorf("Adding duplicate Node with token %d", node.token)
	}
	r.nodeMap[node.token] = node
	r.tokens = append(r.tokens, node.token)
	sort.Ints(r.tokens)
	log.Println(r.name, r.tokens)
	return nil
}

func (r *Rack) MsgForward(req common.Request) error {
	// check if this message should be forwarded
	if !r.canForwardMessage(req.GetRoutingOverride()) {
		log.Printf("Not forwarding %s to %s", req, r)
		return nil
	}

	// get the hashcode for the message and use it to route
	// Depending on the routing type forward accordingly
	for _, peer := range r.nodeMap {
		peer.MsgForward(req)
	}
	return nil
}

func (r *Rack) canForwardMessage(routing_type common.RoutingOverride) bool {
	if !r.isLocalDC {
		// we came here that means we want to forward to this rack
		// only if its the preferred rack.
		return true
	}

	// local dc
	if !r.isLocalRack {
		if routing_type == common.ROUTING_LOCAL_DC_ALL_RACKS_TOKEN_OWNER {
			return true
		}
		return false
	}
	return true
}
