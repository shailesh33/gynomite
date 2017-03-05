package datastore

import (
	"bitbucket.org/shailesh33/dynomite/common"
	"bitbucket.org/shailesh33/dynomite/conf"
	"bitbucket.org/shailesh33/dynomite/datastore/redis"
	"bufio"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
)

type DataStoreType int

const (
	REDIS    = iota
	MEMCACHE = iota
)

var gdatastore DataStoreType
var dataStoreTypeDesc = [...]string{REDIS: "REDIS", MEMCACHE: "MEMCACHE"}
var gDatastoreConn DataStoreConn

type Datastore struct {
	ip   string
	port int
	// read
	// write
	// parse
	// get next message: takes a buf io  and returns a message and can iterate over that
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

func NewRequestParser(reader *bufio.Reader, owner common.Context) common.Parser {
	switch gdatastore {
	case REDIS:
		return datastore.NewRedisRequestParser(reader, owner)
	}
	log.Panicln("Unsupported datastore")
	return nil
}

func NewResponseParser(reader *bufio.Reader) common.Parser {
	switch gdatastore {
	case REDIS:
		return datastore.NewRedisResponseParser(reader)
	}
	log.Panicln("Unsupported datastore")
	return nil
}
