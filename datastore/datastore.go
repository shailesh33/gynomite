package datastore

import (
	"bitbucket.org/shailesh33/dynomite/conf"
	"log"
)

type DataStore int
const (
	REDIS = iota
	MEMCACHE = iota
)

var gdatastore DataStore

type MessageParser interface {

}
func InitDataStore(conf conf.Conf) error {
    	gdatastore = DataStore(conf.Pool.DataStore)
	log.Println("Using datastore ", gdatastore)
	return nil
}
