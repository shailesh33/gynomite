package datastore

import (
	"bitbucket.org/shailesh33/dynomite/common"
	"log"
	"strings"
)

// Redis Request Types supported
const (
	REQUEST_UNSUPPORTED common.RequestType = iota
	REQUEST_REDIS_GET
	REQUEST_REDIS_SET
	REQUEST_REDIS_COMMAND
)

// Redis Request type to protocol string Map
var RequestTypeDesc = [...]string{
	REQUEST_UNSUPPORTED:   "REQUEST_UNKNOWN",
	REQUEST_REDIS_GET:     "GET",
	REQUEST_REDIS_SET:     "SET",
	REQUEST_REDIS_COMMAND: "COMMAND",
}

// Helper to map a protocol string to its internal request type
type requestStringMapper struct {
	m map[string]common.RequestType
}

func newRequestStringMapper() requestStringMapper {
	return requestStringMapper{m: make(map[string]common.RequestType)}
}

func (m *requestStringMapper) add(name string, id common.RequestType) {
	m.m[strings.ToUpper(name)] = id
	return
}

func (m *requestStringMapper) get(request string) common.RequestType {
	t, ok := m.m[strings.ToUpper(request)]
	if ok != true {
		t = REQUEST_UNSUPPORTED
	}
	return t
}

var gRM requestStringMapper = newRequestStringMapper()

func init() {
	for i, v := range RequestTypeDesc {
		log.Println("Adding ", v, common.RequestType(i))
		gRM.add(v, common.RequestType(i))
	}
}

func GetRequestType(r string) common.RequestType {
	return gRM.get(r)
}
