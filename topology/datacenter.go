package topology

import "strings"

type Datacenter struct {
	name    string
	rackMap rackMap
}

type dcMap struct {
	m map[string]Datacenter
}

func newDcMap() dcMap {
	return dcMap{m: make(map[string]Datacenter)}
}

func (m dcMap) add(dc Datacenter) {
	m.m[strings.ToLower(dc.name)] = dc
}

func (m dcMap) get(dcName string) (b Datacenter, ok bool) {
	b, ok = m.m[strings.ToLower(dcName)]
	return
}

func dc_get_or_create_rack(dc Datacenter, rack_name string) Rack {
	rack, ok := dc.rackMap.get(rack_name)
	if ok == true {
		return rack
	}
	rack = Rack{name: rack_name, nodeMap: newNodeMap()}
	dc.rackMap.add(rack)
	return rack
}
