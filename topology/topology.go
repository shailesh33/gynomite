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
	myDC              string
	myRack            string
	myDataStoreServer string
	dcMap             map[string]Datacenter
	localNode         *Node
	forwardChan       chan common.Message
}

func (t Topology) getDC(dcName string) (Datacenter, error) {
	dc, ok := t.dcMap[strings.ToLower(dcName)]
	if ok == true {
		return dc, nil
	}
	return Datacenter{}, fmt.Errorf("DC not found with name %s", dcName)
}

func (t Topology) addDC(dc Datacenter) error {
	if _, err := t.getDC(dc.name); err == nil {
		return fmt.Errorf("Adding duplicate DC with name", dc.name)
	}
	t.dcMap[strings.ToLower(dc.name)] = dc
	return nil
}

func (t Topology) Print() {
	log.Println("DC: " + t.myDC + " Rack: " + t.myRack)
	for dcname, dc := range t.dcMap {
		for rackname, rack := range dc.rackMap {
			for token, node := range rack.nodeMap {
				log.Println("Peers: " + dcname + " " + rackname + " " + node.addr + ":" + strconv.Itoa(node.Port) + " " + strconv.Itoa(token) + "state:" + strconv.Itoa(int(node.state)))
			}
		}
	}
}

func InitTopology(conf conf.Conf) (Topology, error) {
	// get local dc, rack, servername etc
	// get peer information
	// create nodes

	topo := Topology{
		myDC:              conf.Pool.Datacenter,
		myRack:            conf.Pool.Rack,
		dcMap:             make(map[string]Datacenter),
		myDataStoreServer: conf.Pool.Servers[0],
		forwardChan:       make(chan common.Message, 20000),
	}

	// add local node
	dc := newDatacenter(topo.myDC, true)
	err := topo.addDC(dc)
	if err != nil {
		return Topology{}, err
	}
	log.Println("New DC", dc.name)

	// Add local rack
	rack := newRack(topo.myRack, true, true)
	err = dc.addRack(rack)
	if err != nil {
		return Topology{}, err
	}
	log.Println("New rack", rack.name)

	// Add local node
	listen := conf.Pool.DynListen
	host_port := strings.Split(listen, ":")
	port, err := strconv.Atoi(host_port[1])
	if err != nil {
		return Topology{}, fmt.Errorf("Invalid port in dyn_listen option %s", conf.Pool.DynListen)
	}
	token, err := strconv.Atoi(conf.Pool.Tokens)
	if err != nil {
		return Topology{}, fmt.Errorf("Invalid port in dyn_listen option %s", conf.Pool.DynListen)
	}
	var node *Node = newNode(token, host_port[0], port, dc.name, rack.name, true, true, true)
	err = rack.addNode(node)
	if err != nil {
		return Topology{}, err
	}
	topo.localNode = node

	// Go over dyn_seeds and init the topology structure
	for _, p := range conf.Pool.DynSeeds {
		parts := strings.Split(p, ":")
		if len(parts) != 5 {
			return Topology{}, fmt.Errorf("Invalid entry in dyn_seeds %s", p)
		}
		addr := parts[0]
		port, err = strconv.Atoi(parts[1])
		if err != nil {
			return Topology{}, fmt.Errorf("Invalid port in peer option %s", p)
		}
		rackName := parts[2]
		dcName := parts[3]
		token, err = strconv.Atoi(parts[4])
		if err != nil {
			return Topology{}, fmt.Errorf("Invalid token in peer option %s", p)
		}

		isLocalDC := false
		if strings.EqualFold(dcName, topo.myDC) {
			isLocalDC = true
		}

		isLocalRack := false
		if strings.EqualFold(rackName, topo.myRack) {
			isLocalRack = true
		}

		if dc, err = topo.getDC(dcName); err != nil {
			dc = newDatacenter(dcName, isLocalDC)
			topo.addDC(dc)
		}

		if rack, err = dc.getRack(rackName); err != nil {
			rack = newRack(rackName, isLocalDC, isLocalRack)
			dc.addRack(rack)
		}
		var peer *Node
		if peer, err = rack.getNode(token); err == nil {
			log.Panicf("Duplicate token in rack %s token %s", rack.name, token)
		}
		peer = newNode(token, addr, port, dcName, rackName, isLocalDC, isLocalRack, false)
		rack.addNode(peer)
	}

	return topo, nil
}

func (t Topology) connect(c chan<- int) error {
	var wg sync.WaitGroup
	for _, dc := range t.dcMap {
		for _, rack := range dc.rackMap {
			for _, node := range rack.nodeMap {
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
	t.Print()

	c <- 1
	return nil
}

func (t Topology) Start() error {
	go common.ListenAndServe(net.JoinHostPort(t.localNode.addr, strconv.Itoa(t.localNode.Port)), newPeerClientConnHandler, t.localNode)

	c := make(chan int, 1)
	go t.connect(c)

	select {
	case <-c:
		log.Println("All nodes connected successfully")
	case <-time.After(15 * time.Second):
	}
	log.Println("After select")
	go t.Run()
	return nil
}

func (t Topology) MsgForward(m common.Message) error {
	t.forwardChan <- m
	return nil
}

func (t Topology) Run() error {
	for m := range t.forwardChan {
		for _, dc := range t.dcMap {
			req := m.(common.Request)
			dc.MsgForward(req)
			/*for _, rack := range dc.rackMap {
				for _, node := range rack.nodeMap {
					log.Printf("Forwarding %s to %s", m, node)
					node.MsgForward(m)
				}
			}*/
		}
	}
	return nil
}
