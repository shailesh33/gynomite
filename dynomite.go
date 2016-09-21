// Hello World!!

package main

import (
        "flag"
)

var (
        verbosity       int
	confFile        string
	logFileName     string
	daemonize       bool
	testConf        bool
	version         bool
	statsPort       int
	statsAddr       string
	statsInterval	int
	mbufSize        int
	maxMsgs         uint64

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
	flag.IntVar(&statsPort, "s", 22222, "set stats monitoring port (default: 22222)")
	flag.StringVar(&statsAddr, "a", "0.0.0.0", "set stats monitoring ip (default: 0.0.0.0)")
	flag.IntVar(&statsInterval, "i", 30000, "set stats aggregation interval in msec (default: 30000 msec)")
	flag.IntVar(&mbufSize, "m", 16384, "set size of mbuf chunk in bytes (default: 16384 bytes)")
	flag.Uint64Var(&maxMsgs, "M", 200000, "set max number of messages to allocate (default: 200000)")

        flag.Parse()

}

func main() {
	c conf
}