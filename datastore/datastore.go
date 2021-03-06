package datastore

import (
	"github.com/shailesh33/gynomite/common"
	"github.com/shailesh33/gynomite/conf"
	"github.com/shailesh33/gynomite/datastore/redis"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
)

type DataStoreType int

const (
	REDIS    = iota
	MEMCACHE
)

var gdatastore DataStoreType
var dataStoreTypeDesc = [...]string{
	REDIS: "REDIS",
	MEMCACHE: "MEMCACHE",
}

var gDatastoreConn DataStoreConn

type Datastore struct {
	ip   string
	port int
	// read
	// write
	// parse
	// get next message: takes a buf io  and returns a message and can iterate over that
}

func (ds Datastore) String () string {
	return fmt.Sprintf("<Datastore at %s:%d>", ds.ip, ds.port)
}

func InitDataStore(conf conf.Conf) (Datastore, error) {
	gdatastore = DataStoreType(conf.Pool.DataStore)
	log.Println("Using datastore", dataStoreTypeDesc[gdatastore])
	if len(conf.Pool.Servers) != 1 {
		return Datastore{}, fmt.Errorf("Expecting only 1 server in the server list of the yaml file")
	}
	ip := ""
	port := 0
	var err error = nil
	for _, s := range conf.Pool.Servers {
		parts := strings.Split(s, ":")
		if len(parts) != 3 {
			return Datastore{}, fmt.Errorf("Invalid string in servers %s", s)
		}
		ip = parts[0]
		port, err = strconv.Atoi(parts[1])
		if err != nil {
			return Datastore{}, fmt.Errorf("Invalid port in Server option %s", s)
		}
	}
	d := Datastore{ip: ip, port: port}
	return d, nil
}

func InitDataStoreConn(d Datastore) error {
	conn, err := net.Dial("tcp", net.JoinHostPort(d.ip, strconv.Itoa(d.port)))
	if err != nil {
		return err
	}
	gDatastoreConn, err = NewDataStoreConn(conn)
	if err != nil {
		log.Println("Failed to initialize Datastore conn")
		return err
	}
	log.Println("Connected to datastore ", d)
	go gDatastoreConn.Run()
	return nil
}

func GetDatastoreConn() DataStoreConn {
	return gDatastoreConn
}

func NewRequestParser() common.IRequestParser {
	switch gdatastore {
	case REDIS:
		return datastore.NewRedisRequestParser()
	}
	log.Panicln("Unsupported datastore")
	return nil
}

func NewResponseParser() common.IResponseParser {
	switch gdatastore {
	case REDIS:
		return datastore.NewRedisResponseParser()
	}
	log.Panicln("Unsupported datastore")
	return nil
}
