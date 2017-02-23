package topology

import (
	"bitbucket.org/shailesh33/dynomite/conf"
	"fmt"
	"log"
	"strconv"
	"strings"
)

type Topology struct {
	mydc               string
	myrack             string
	mydatastore_server string
	dcMap              dcMap
	localNode          Node
}

func topo_get_or_create_dc(topo Topology, dc_name string) Datacenter {
	dc, ok := topo.dcMap.get(dc_name)
	if ok == true {
		return dc
	}
	dc = Datacenter{name: dc_name, rackMap: newRackMap()}
	topo.dcMap.add(dc)
	return dc
}

func Topology_print(t Topology) {
	log.Println("DC: " + t.mydc + " Rack: " + t.myrack)
	for dcname, dc := range t.dcMap.m {
		for rackname, rack := range dc.rackMap.m {
			for token, node := range rack.nodeMap.m {
				log.Println("Peers: " + dcname + " " + rackname + " " + node.addr + ":" + strconv.Itoa(node.port) + " " + token)
			}
		}
	}
}

func GetLocalNode(topo Topology) Node {
	return topo.localNode
}

func InitTopology(conf conf.Conf) (Topology, error) {
	// get local dc, rack, servername etc
	// get peer information
	// create nodes

	var topo Topology

	topo.mydc = conf.Pool.Datacenter
	topo.myrack = conf.Pool.Rack
	topo.mydatastore_server = conf.Pool.Servers[0]
	topo.dcMap = newDcMap()
	// add local node

	dc := topo_get_or_create_dc(topo, topo.mydc)
	rack := dc_get_or_create_rack(dc, topo.myrack)

	var node Node
	node.is_local = true
	node.is_same_dc = true
	node.is_same_rack = true
	node.token = conf.Pool.Tokens
	listen := conf.Pool.DynListen
	host_port := strings.Split(listen, ":")
	node.addr = host_port[0]
	var err error = nil
	node.port, err = strconv.Atoi(host_port[1])
	if err != nil {
		return Topology{}, fmt.Errorf("Invalid port in dyn_listen option %s", conf.Pool.DynListen)
	}
	topo.localNode = node
	rack.nodeMap.add(node)

	for _, p := range conf.Pool.DynSeeds {
		parts := strings.Split(p, ":")
		if len(parts) != 5 {
			return Topology{}, fmt.Errorf("Invalid entry in dyn_seeds %s", p)
		}
		var peer Node
		peer.addr = parts[0]
		peer.port, err = strconv.Atoi(parts[1])
		if err != nil {
			return Topology{}, fmt.Errorf("Invalid port in peer option %s", p)
		}
		peer.rack_name = parts[2]
		peer.dc_name = parts[3]
		peer.token = parts[4]

		if strings.EqualFold(peer.dc_name, topo.mydc) {
			peer.is_same_dc = true
		}

		if strings.EqualFold(peer.rack_name, topo.myrack) {
			peer.is_same_rack = true
		}
		peer.is_local = false

		dc := topo_get_or_create_dc(topo, peer.dc_name)
		rack := dc_get_or_create_rack(dc, peer.rack_name)

		_, err := rack.nodeMap.get(peer.token)
		if err != false {
			log.Panicf("Duplicate token in rack %s token %s", rack.name, peer.token)
			return Topology{}, fmt.Errorf("Duplicate token in rack %s token %s", rack.name, peer.token)

		}
		rack.nodeMap.add(peer)

	}
	return topo, nil
}
