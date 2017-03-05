// Hello World!!

package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"

	"bitbucket.org/shailesh33/dynomite/conf"
	"bitbucket.org/shailesh33/dynomite/datastore"
	"bitbucket.org/shailesh33/dynomite/hashkit"
	"bitbucket.org/shailesh33/dynomite/server"
	"bitbucket.org/shailesh33/dynomite/topology"
	"io"
	"runtime"
)

var (
	verbosity     int
	confFile      string
	logFileName   string
	daemonize     bool
	testConf      bool
	version       bool
	statsAddr     string
	statsInterval int
)

/*
-g, --gossip           : enable gossip (default: disable)
-D, --describe-stats   : print stats description and exit
-p, --pid-file=S             : set pid file (default: off)
-x, --admin-operation=N      : set size of admin operation (default: 0)
*/

func init() {
	flag.StringVar(&confFile, "c", "conf/dynomite.yml", "set configuration file (default: conf/dynomite.yml)")
	flag.IntVar(&verbosity, "v", 5, "set logging level (default 5, min: 0, max:11")
	flag.StringVar(&logFileName, "o", "dynomite.log", "set logging file (default: stdout)")
	flag.BoolVar(&daemonize, "d", false, "run as a daemon")
	flag.BoolVar(&testConf, "t", false, "test configuration for syntax errors and exit")
	flag.BoolVar(&version, "V", false, "show version and exit")
	flag.StringVar(&statsAddr, "a", "0.0.0.0", "set stats monitoring ip (default: 0.0.0.0)")
	flag.IntVar(&statsInterval, "i", 30000, "set stats aggregation interval in msec (default: 30000 msec)")
	flag.Parse()
}

func main() {
	runtime.GOMAXPROCS(1)
	if (len(logFileName) > 0) {
		file, err := os.OpenFile(logFileName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
		if err != nil {
			log.Fatalln("Failed to open log file", logFileName, ":", err)
		}
		multi := io.MultiWriter(file, os.Stdout)

		log.SetOutput(multi)
	}
	log.SetFlags(log.Lshortfile | log.Lmicroseconds)

	conf, err := conf.Parse(confFile)
	if err != nil {
		fmt.Println("Failed to parse file", err)
		log.Fatal("Failed to parse file", err)
	}
	fmt.Println(conf.Pool.Hash)
	err = hashkit.InitHashkit(conf.Pool.Hash)
	if err != nil {
		log.Fatal("Failed to initialize hashkit", err)
	}
	var ds datastore.Datastore
	ds, err = datastore.InitDataStore(conf)
	if err != nil {
		log.Fatal("Failed to initialize Datastore", err)
	}
	log.Println("Using Datastore at ", ds)

	topo, err := topology.InitTopology(conf)
	topo.Print()
	topo.Start()

	err = datastore.InitDataStoreConn(ds)
	if err != nil {
		log.Printf("Failed to connect to datastore")
		os.Exit(1)
	}
	listener, err := net.Listen("tcp", conf.Pool.Listen)
	if err != nil {
		log.Println("Error listening on ", conf.Pool.Listen, err.Error())
		os.Exit(1)
	}
	defer listener.Close()
	log.Println("Listening on ", conf.Pool.Listen)
	for {
		// Listen for an incoming connection.
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting: ", err.Error())
			os.Exit(1)
		}
		// Handle connections in a new goroutine.
		go handleClient(conn, topo)
	}
}

func handleClient(clientConn net.Conn, topo topology.Topology) {
	c, err := server.NewClientConnHandler(clientConn, topo)
	if err != nil {
		log.Println("Failed to handle client ", err)
	}
	c.Run()
}
