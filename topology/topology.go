package topology

import (
	"bitbucket.org/shailesh33/dynomite/common"
	"bitbucket.org/shailesh33/dynomite/conf"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Topology struct {
	mydc               string
	myrack             string
	mydatastore_server string
	dcMap              dcMap
	localNode          *Node
	listener           net.Listener
}

func (t Topology) run() error {
	return nil
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

func (t Topology) Print() {
	log.Println("DC: " + t.mydc + " Rack: " + t.myrack)
	for dcname, dc := range t.dcMap.m {
		for rackname, rack := range dc.rackMap.m {
			for token, node := range rack.nodeMap.m {
				log.Println("Peers: " + dcname + " " + rackname + " " + node.addr + ":" + strconv.Itoa(node.Port) + " " + token)
			}
		}
	}
}

func GetLocalNode(topo Topology) *Node {
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

	var node *Node = newNode()
	node.is_local = true
	node.is_same_dc = true
	node.is_same_rack = true
	node.token = conf.Pool.Tokens
	listen := conf.Pool.DynListen
	host_port := strings.Split(listen, ":")
	node.addr = host_port[0]
	var err error = nil
	node.Port, err = strconv.Atoi(host_port[1])
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
		var peer *Node = newNode()
		peer.addr = parts[0]
		peer.Port, err = strconv.Atoi(parts[1])
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
		peer.state = NODE_DOWN

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

func (t Topology) connect(c chan<- int) error {
	var wg sync.WaitGroup
	for _, dc := range t.dcMap.m {
		for _, rack := range dc.rackMap.m {
			for _, node := range rack.nodeMap.m {
				wg.Add(1)
				go func(n *Node) {
					n.connect()
					log.Println(n)
					wg.Done()
				}(node)
			}
		}
	}
	log.Println("waiting for connections to peer")
	wg.Wait()
	log.Println("Done waiting for connections to peer")

	c <- 1
	return nil
}

func (t Topology) Start() error {
	go common.ListenAndServe(net.JoinHostPort(t.localNode.addr, strconv.Itoa(t.localNode.Port)), t.localNode, newPeerClientConnHandler)

	c := make(chan int, 1)
	t.connect(c)

	select {
	case <-c:
		log.Println("All nodes connected successfully")
	case <-time.After(5 * time.Second):
	}
	log.Println("After select")
	//go t.run()
	return nil
}

func (t Topology) MsgForward(m common.Message) error {
	for _, dc := range t.dcMap.m {
		for _, rack := range dc.rackMap.m {
			for _, node := range rack.nodeMap.m {
				log.Printf("Forwarding %s to %s",m, node)
				node.MsgForward(m)
			}
		}
	}
	return nil
}
