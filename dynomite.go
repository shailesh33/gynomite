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
	flag.StringVar(&logFileName, "o", "dynomite.log", "set logging file (default: stderr)")
	flag.BoolVar(&daemonize, "d", false, "run as a daemon")
	flag.BoolVar(&testConf, "t", false, "test configuration for syntax errors and exit")
	flag.BoolVar(&version, "V", false, "show version and exit")
	flag.StringVar(&statsAddr, "a", "0.0.0.0", "set stats monitoring ip (default: 0.0.0.0)")
	flag.IntVar(&statsInterval, "i", 30000, "set stats aggregation interval in msec (default: 30000 msec)")
	flag.Parse()
}

func main() {
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
	topology.Topology_print(topo)

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
		go handleClient2(conn)
	}
}

func handleClient2(clientConn net.Conn) {
	datastoreConn, err := server.NewDatastoreConnHanler(clientConn)
	if err != nil {
		log.Println("Failed to handle client ", err)
		return
	}
	go datastoreConn.Run()

	// reader from clientReader and parse it
	/*parser := datastore.NewRedisRequestParser(clientReader)

	for {
		r, err := parser.GetNextMessage()
		if err != nil {
			if err == io.EOF {
				return
			}
			log.Println("Failed to parse a request ", err)
			return
		}
		//log.Println("Received request ", parser.GetRequestType(r.Name), r.Name)

		switch r.Type {
		case datastore.REQUEST_REDIS_COMMAND:
			rsp := datastore.NewArrayResponse()

			rsp.Write(clientWriter)
		case datastore.REQUEST_REDIS_SET:
			rsp := datastore.NewStatusResponse("OK")
			rsp.Write(clientWriter)
		}

		clientWriter.Flush()
	}*/
}

func handleClient(clientConn net.Conn) {
	c, err := server.NewClientConnHandler(clientConn)
	if err != nil {
		log.Println("Failed to handle client ", err)
	}
	c.Run()
	//
	//tcpConn := clientConn.(*net.TCPConn)
	//tcpConn.SetKeepAlive(true)
	//tcpConn.SetKeepAlivePeriod(30 * time.Second)
	//clientReader := bufio.NewReader(clientConn)
	//clientWriter := bufio.NewWriter(clientConn)
	//
	//// reader from clientReader and parse it
	//parser := datastore.NewRedisRequestParser(clientReader)
	//
	//for {
	//	r, err := parser.GetNextMessage()
	//	if err != nil {
	//		if err == io.EOF {
	//			return
	//		}
	//		log.Println("Failed to parse a request ", err)
	//		return
	//	}
	//	//log.Println("Received request ", parser.GetRequestType(r.Name), r.Name)
	//
	//	switch r.Type {
	//	case datastore.REQUEST_REDIS_COMMAND:
	//		rsp := datastore.NewArrayResponse()
	//
	//		rsp.Write(clientWriter)
	//	case datastore.REQUEST_REDIS_SET:
	//		rsp := datastore.NewStatusResponse("OK")
	//		rsp.Write(clientWriter)
	//	}
	//
	//	clientWriter.Flush()
	//}
}
