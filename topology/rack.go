package topology

import (
	"github.com/shailesh33/gynomite/common"
	"fmt"
	"log"
	"sort"
)

type TokenSlice []uint32

func (a TokenSlice) Len() int           { return len(a) }
func (a TokenSlice) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a TokenSlice) Less(i, j int) bool { return a[i] < a[j] }


type Rack struct {
	t 		*Topology
	name        string
	nodeMap     map[uint32]*Node
	isLocalDC   bool
	isLocalRack bool
	tokens      TokenSlice
}

func (r Rack) String() string {
	return fmt.Sprintf("<Rack %s isLocalDC:%t isLocalRack:%t", r.name, r.isLocalDC, r.isLocalRack)
}

func newRack(t *Topology, name string, isLocalDC bool, isLocalRack bool) *Rack {
	return &Rack{
		t:		t,
		name:        name,
		isLocalDC:   isLocalDC,
		nodeMap:     make(map[uint32]*Node),
		isLocalRack: isLocalRack,
		tokens:      make([]uint32, 0, 3),
	}
}

func (r *Rack) getNode(token uint32) (*Node, error) {
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
	sort.Sort(r.tokens)
	log.Println(r.name, r.tokens)
	return nil
}

func (r *Rack)GetTokenForHash(hashCode uint32) uint32{
	// given the token array find which bucket does the hash lands:
	i := sort.Search(len(r.tokens), func(i int) bool { return r.tokens[i] >= hashCode })
	if i < len(r.tokens) {
		fmt.Printf("Token for  %d found at %d\n", hashCode, r.tokens[i])
		return r.tokens[i]
	} else {
		fmt.Printf("Token for %d found at %d\n", hashCode, r.tokens[0])
		return r.tokens[0]
	}
}

func (r *Rack) MsgForward(req common.Request) error {
	if req.GetRoutingOverride() == common.ROUTING_LOCAL_NODE_ONLY {
		localNode, err := r.getNode(r.t.myToken)
		if err != nil {
			log.Printf("Did not find mytoken %d in local rack %s", r.t.myToken, r)
		}
		return localNode.MsgForward(req)
	} else {
		if (req.GetRoutingOverride() == common.ROUTING_LOCAL_RACK_TOKEN_OWNER) ||
			(req.GetRoutingOverride() == common.ROUTING_LOCAL_DC_ALL_RACKS_TOKEN_OWNER) ||
			(req.GetRoutingOverride() == common.ROUTING_ALL_DCS_TOKEN_OWNER){
			// get the hashcode for the message and use it to route
			token := r.GetTokenForHash(req.GetHashCode())

			node, err := r.getNode(token)
			if err != nil {
				log.Printf("Failed to find node for token %d in rack %s for message with hash %d", token, r, req.GetHashCode())
				return fmt.Errorf("Failed to find node for token %d in rack %s for message with hash %d", token, r, req.GetHashCode())
			}
			return node.MsgForward(req)
		} else {
			return fmt.Errorf("Invalid routing type %d for message %s", req.GetRoutingOverride(), req)
		}
	}
}