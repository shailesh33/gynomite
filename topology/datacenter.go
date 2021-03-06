package topology

import (
	"github.com/shailesh33/gynomite/common"
	"fmt"
	"log"
	"strings"
)

type Datacenter struct {
	t                   *Topology
	name                string
	rackMap             map[string]*Rack
	preSelectedRackName string
	isLocal             bool
}

func (dc *Datacenter) String() string {
	if dc.isLocal {
		return fmt.Sprintf("<LOCAL DC %s>", dc.name)
	} else {
		return fmt.Sprintf("<REMOTE DC %s>", dc.name)
	}

}

func (dc *Datacenter) getRack(rackName string) (*Rack, error) {
	rack, ok := dc.rackMap[strings.ToLower(rackName)]
	if ok == true {
		return rack, nil
	}
	return &Rack{}, fmt.Errorf("Rack not Found %s", rackName)
}

func (dc *Datacenter) addRack(rack *Rack) error {
	if _, err := dc.getRack(rack.name); err == nil {
		return fmt.Errorf("Adding duplicate Rack with name %s", rack.name)
	}
	dc.rackMap[strings.ToLower(rack.name)] = rack
	return nil
}

func newDatacenter(t *Topology, dcName string, isLocal bool) *Datacenter {
	return &Datacenter{
		t:       t,
		name:    dcName,
		rackMap: make(map[string]*Rack),
		isLocal: isLocal,
	}
}

func (dc *Datacenter) preselectRack(index int) {
	racks := make([]string, 0, len(dc.rackMap))
	for k := range dc.rackMap {
		racks = append(racks, k)
	}
	selectedRackName := racks[index%len(racks)]
	log.Printf("Selecting rack %s for remote replication to DC %s", selectedRackName, dc.name)
	dc.preSelectedRackName = selectedRackName
}

func (dc *Datacenter) MsgForward(req common.IRequest) error {

	// This is a local dc, check if it needs to be sent to all the racks
	// otherwise send to preselected rack
	//log.Printf("%s: Forwarding %s", dc.name, req)

	if dc.isLocal {
		if (req.GetRoutingOverride() == common.ROUTING_LOCAL_DC_ALL_RACKS_TOKEN_OWNER) ||
			(req.GetRoutingOverride() == common.ROUTING_ALL_DCS_TOKEN_OWNER){
			for _, rack := range dc.rackMap {
				//log.Printf("%s: Forwarding %s to %s", dc.name, req, rack.name)
				rack.MsgForward(req)
			}
		} else {
			// forward only to local rack
			rack, _ := dc.getRack(dc.t.myRack)
			rack.MsgForward(req)
		}
	} else {
		rack, _ := dc.getRack(dc.preSelectedRackName)
		return rack.MsgForward(req)
	}
	return nil
}

func (dc *Datacenter) getRackCount() int {
	return len(dc.rackMap)
}

func (dc *Datacenter) canForwardMessage(routing_type common.RoutingOverride) bool {
	//log.Printf("Routing override %d", routing_type)
	switch routing_type {
	case common.ROUTING_LOCAL_NODE_ONLY:
		fallthrough;
	case common.ROUTING_LOCAL_RACK_TOKEN_OWNER:
		fallthrough
	case common.ROUTING_LOCAL_DC_ALL_RACKS_TOKEN_OWNER:
		if dc.isLocal {
			return true
		}
		return false
	case common.ROUTING_ALL_DCS_TOKEN_OWNER:
		return true
	}
	return false
}
