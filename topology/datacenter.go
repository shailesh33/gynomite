package topology

import (
	"bitbucket.org/shailesh33/dynomite/common"
	"fmt"
	"log"
	"strings"
)

type Datacenter struct {
	name    string
	rackMap map[string]Rack
	isLocal bool
}
func (dc Datacenter) String() string {
	if dc.isLocal {
		return fmt.Sprintf("<LOCAL DC %s>", dc.name)
	} else {
		return fmt.Sprintf("<REMOTE DC %s>", dc.name)
	}

}

func (dc Datacenter) getRack(rackName string) (Rack, error) {
	rack, ok := dc.rackMap[strings.ToLower(rackName)]
	if ok == true {
		return rack, nil
	}
	return Rack{}, fmt.Errorf("Rack not Found %s", rackName)
}

func (dc Datacenter) addRack(rack Rack) error {
	if _, err := dc.getRack(rack.name); err == nil {
		return fmt.Errorf("Adding duplicate Rack with name %s", rack.name)
	}
	dc.rackMap[strings.ToLower(rack.name)] = rack
	return nil
}

func newDatacenter(dcName string, isLocal bool) Datacenter {
	return Datacenter{
		name:    dcName,
		rackMap: make(map[string]Rack),
		isLocal: isLocal,
	}
}

func (dc Datacenter) MsgForward(req common.Request) error {
	// check if this message should be forwarded
	if !dc.canForwardMessage(req.GetRoutingOverride()) {
		log.Printf("Not forwarding %s to %s", req, dc)
		return nil
	}

	for _, rack := range dc.rackMap {
		rack.MsgForward(req)
	}
	return nil
}

func (dc Datacenter) canForwardMessage(routing_type common.RoutingOverride) bool {
	if !dc.isLocal {
		if (routing_type == common.ROUTING_ALL_DCS_TOKEN_OWNER) ||
			(routing_type == common.ROUTING_ALL_DCS_ALL_NODES) {
			return true
		}
		return false
	}
	return true
}
