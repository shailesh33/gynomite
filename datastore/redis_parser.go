package datastore

import (
	"bufio"
	"fmt"
	"log"
	"strings"

	"bitbucket.org/shailesh33/dynomite/common"
	"strconv"
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

type RedisRequest struct {
	common.Request
}

func (r *RedisRequest) Write(w *bufio.Writer) {
	w.WriteByte('*')
	w.WriteString(strconv.Itoa(len(r.Args)))
	w.WriteString("\r\n")
	for _, i := range r.Args {
		w.WriteByte('$')
		w.WriteString(strconv.Itoa(len(i)))
		w.WriteString("\r\n")
		w.Write(i)
		w.WriteString("\r\n")
	}
	w.Flush()
}

// Redis request Parser
type RedisRequestParser struct {
	r *bufio.Reader
}

func NewRedisRequestParser(r *bufio.Reader) RedisRequestParser {
	return RedisRequestParser{r: r}
}

func (parser RedisRequestParser) GetNextMessage() (common.Message, error) {
	r := parser.r
	line, err := parser.r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	if len(line) == 0 {
		return nil, fmt.Errorf("Empty line")
	}

	var argsCount int

	if _, err := fmt.Sscanf(line, "*%d\r\n", &argsCount); err != nil {
		return nil, fmt.Errorf("invalid number of arguments in ", line)
	}
	// All next lines are pairs of:
	//$<argument length> CR LF
	//<argument data> CR LF
	// first argument is a command name, so just convert

	firstArg, err := readArgument(r)
	if err != nil {
		return nil, err
	}

	args := make([][]byte, argsCount-1)
	for i := 0; i < argsCount-1; i += 1 {
		if args[i], err = readArgument(r); err != nil {
			return nil, err
		}
	}

	return common.Request{
		Type: GetRequestType(string(firstArg)),
		Name: strings.ToUpper(string(firstArg)),
		Args: args,
	}, nil
}
