// Hello World!!

package main

import (
	"fmt"

	"bitbucket.org/shailesh33/dynomite/conf"
	"log"
	"flag"
	"bitbucket.org/shailesh33/dynomite/datastore"
	"bitbucket.org/shailesh33/dynomite/topology"
)


var (
        verbosity       int
	confFile        string
	logFileName     string
	daemonize       bool
	testConf        bool
	version         bool
	statsAddr       string
	statsInterval	int
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
	/*err = hashkit.InitHashkit(conf.Pool.Hash)
	if err != nil {
		log.Fatal("Failed to initialize hashkit", err)
	}*/
	err = datastore.InitDataStore(conf)
	if err != nil {
		log.Fatal("Failed to initialize Datastore", err)
	}

	topo, err := topology.InitTopology(conf)
	topology.Topology_print(topo)

}