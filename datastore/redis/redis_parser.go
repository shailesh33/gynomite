package datastore

import (
	"bufio"
	"fmt"
	"strings"

	"github.com/shailesh33/gynomite/common"
	"github.com/shailesh33/gynomite/hashkit"
	"strconv"
	"time"
	"log"
)

type RedisRequest struct {
	common.BaseMessage
	Name        string
	requestType RedisRequestType
	override    common.RoutingOverride
	ctx         common.Context
	Args        [][]byte
	hashCode    uint32
	responses   []common.Response
	done        chan common.Response
}

func (r *RedisRequest) GetName() string {
	return r.Name
}

func (r *RedisRequest) GetHashCode() uint32 {
	return r.hashCode
}

func (r *RedisRequest) GetRoutingOverride() common.RoutingOverride {
	return r.override
}

func (r *RedisRequest) SetRoutingOverride(newOverride common.RoutingOverride) {
	r.override = newOverride
}

func (r *RedisRequest) Write(w *bufio.Writer) error {
	w.WriteByte('*')
	w.WriteString(strconv.Itoa(len(r.Args) + 1))
	w.WriteString("\r\n")
	w.WriteByte('$')
	w.WriteString(strconv.Itoa(len(r.Name)))
	w.WriteString("\r\n")
	w.WriteString(r.Name)
	w.WriteString("\r\n")
	for _, i := range r.Args {
		w.WriteByte('$')
		w.WriteString(strconv.Itoa(len(i)))
		w.WriteString("\r\n")
		w.Write(i)
		w.WriteString("\r\n")
	}
	w.Flush()
	return nil
}

func (r *RedisRequest) GetKey() []byte {
	if len(r.Args) > 0 {
		return r.Args[0]
	}
	return []byte{}
}

func (r *RedisRequest) GetContext() common.Context {
	return r.ctx
}

func (r *RedisRequest) String() string {
	return fmt.Sprintf("<%v %s '%s' Hash:%d Routing:%d>", r.Id, r.Name, r.GetKey(), r.GetHashCode(), r.GetRoutingOverride())
}

func (r *RedisRequest) Done() common.Response {
	// TODO: Implement some timeout here
	var rsp common.Response
	select {
	case rsp = <- r.done:
	case <- time.After(5 * time.Second):
		log.Printf("req %s timedout", r)

	}
	return rsp
}

func (r *RedisRequest) HandleResponse(rsp common.Response) error {
	r.done <- rsp
	return nil
}

// Redis request Parser
type RedisRequestParser struct {
	r     *bufio.Reader
	owner common.Context
}

func NewRedisRequestParser(r *bufio.Reader, owner common.Context) RedisRequestParser {
	return RedisRequestParser{r: r, owner: owner}
}

func (parser RedisRequestParser) GetNextRequest() (common.Request, error) {

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
	var requestType RedisRequestType = GetRequestTypeFromString(string(firstArg))
	if requestType == REQUEST_UNSUPPORTED {
		return nil, fmt.Errorf("Invalid or unsupported request")
	}

	req := &RedisRequest{
		BaseMessage :struct {
			Id uint64
			MsgType     common.MessageType
		} {
			Id:		common.GetNextId(),
			MsgType:     common.REQUEST_DATASTORE,
		},
		requestType: requestType,
		Name:        strings.ToUpper(string(firstArg)),
		override:    GetRequestOverride(requestType),
		ctx:         parser.owner,
		Args:        args,
		done:        make(chan common.Response, 5), // TODO: this is a hack, ideally the reader of this channel should close the channel
	}

	req.hashCode = hashkit.GetHash(req.GetKey())
	return req, nil
}
