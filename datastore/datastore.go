package datastore

import (
	"bitbucket.org/shailesh33/dynomite/conf"
	"log"
	"strings"
	"fmt"
	"strconv"
)

type DataStoreType int
const (
	REDIS = iota
	MEMCACHE = iota
)

var gdatastore DataStoreType
var dataStoreTypeDesc = [...]string{REDIS: "REDIS", MEMCACHE: "MEMCACHE"}


type Datastore struct {

	ip	string
	port	int
// read
// write
// parse
// get next message: takes a buf io  and returns a message and can iterate over that
}

func InitDataStore(conf conf.Conf) (Datastore, error) {
    	gdatastore = DataStoreType(conf.Pool.DataStore)
	log.Println("Using datastore", dataStoreTypeDesc[gdatastore])
	if (len(conf.Pool.Servers) != 1) {
		return Datastore{}, fmt.Errorf("Expecting only 1 server in the server list of the yaml file")
	}
	ip := ""
	port := 0
	var err error = nil
	for _, s:= range conf.Pool.Servers {
		parts := strings.Split(s, ":")
		if (len(parts) != 3) {
			return Datastore{}, fmt.Errorf("Invalid string in servers %s", s)
		}
		ip = parts[0]
		port, err = strconv.Atoi(parts[1])
		if (err != nil) {
			return Datastore{}, fmt.Errorf("Invalid port in Server option %s", s)
		}
	}
	d := Datastore{ip:ip, port:port}
	return d, nil
}
