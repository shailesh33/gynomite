package datastore

import (
	"bitbucket.org/shailesh33/dynomite/common"
	"log"
	"strings"
)

type RedisRequestType int

// Redis Request Types supported
const (
	REQUEST_UNSUPPORTED RedisRequestType = iota
	REQUEST_REDIS_GET
	REQUEST_REDIS_SET
	REQUEST_REDIS_COMMAND
	REQUEST_REDIS_INFO
)

type requestProperties struct {
	name     string
	override common.RoutingOverride
}

// Redis Request type to protocol string Map
var RequestTypeDesc = [...]requestProperties{
	REQUEST_UNSUPPORTED:   requestProperties{name: "REQUEST_UNKNOWN", override: common.ROUTING_NORMAL},
	REQUEST_REDIS_GET:     requestProperties{name: "GET", override: common.ROUTING_LOCAL_RACK_TOKEN_OWNER},
	REQUEST_REDIS_SET:     requestProperties{name: "SET", override: common.ROUTING_ALL_DCS_TOKEN_OWNER},
	REQUEST_REDIS_COMMAND: requestProperties{name: "COMMAND", override: common.ROUTING_LOCAL_NODE_ONLY},
	REQUEST_REDIS_INFO:    requestProperties{name: "INFO", override: common.ROUTING_LOCAL_NODE_ONLY},
}

// Helper to map a protocol string to its internal request type
type requestStringMapper struct {
	m map[string]RedisRequestType
}

func newRequestStringMapper() requestStringMapper {
	return requestStringMapper{m: make(map[string]RedisRequestType)}
}

func (m *requestStringMapper) add(name string, id RedisRequestType) {
	m.m[strings.ToUpper(name)] = id
	return
}

func (m *requestStringMapper) get(request string) RedisRequestType {
	t, ok := m.m[strings.ToUpper(request)]
	if ok != true {
		t = REQUEST_UNSUPPORTED
	}
	return t
}

var gRM requestStringMapper = newRequestStringMapper()

func init() {
	for i, v := range RequestTypeDesc {
		log.Println("Adding ", v, RedisRequestType(i))
		gRM.add(v.name, RedisRequestType(i))
	}
}

func GetRequestTypeFromString(r string) RedisRequestType {
	return gRM.get(r)
}

func GetRequestOverride(t RedisRequestType) common.RoutingOverride {
	return RequestTypeDesc[t].override
}
