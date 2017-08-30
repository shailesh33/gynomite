package datastore

import (
	"bufio"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/shailesh33/gynomite/common"
	"github.com/shailesh33/gynomite/hashkit"
)

// RedisRequest This is a request message for redis
type RedisRequest struct {
	common.BaseMessage
	Name              string
	requestType       RedisRequestType
	override          common.RoutingOverride
	ctx               common.Context
	Args              [][]byte
	hashCode          uint32
	quorumResponses   int
	maxResponses      int
	receivedResponses int
	responses         []common.IResponse
	done              chan common.IResponse
}

// GetHashCode Return the hashcode of the key in the request
func (r *RedisRequest) GetHashCode() uint32 {
	return r.hashCode
}

// GetRoutingOverride Return the routing override for this request
func (r *RedisRequest) GetRoutingOverride() common.RoutingOverride {
	return r.override
}

// SetRoutingOverride set the routing override for this request
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

func (r *RedisRequest) getKey() []byte {
	if len(r.Args) > 0 {
		return r.Args[0]
	}
	return []byte{}
}

// GetContext return the context of the request.
func (r *RedisRequest) GetContext() common.Context {
	return r.ctx
}

func (r *RedisRequest) String() string {
	return fmt.Sprintf("<%v %s '%s' Hash:%d Routing:%d>", r.Id, r.Name, r.getKey(), r.GetHashCode(), r.GetRoutingOverride())
}

// Done one can call this function to wait for the response on this request.
func (r *RedisRequest) Done() common.IResponse {
	// TODO: Implement some timeout here
	t := time.NewTimer(5 * time.Second)
	var rsp common.IResponse
	select {
	case rsp = <-r.done:
		t.Stop()
	case <-t.C:
		log.Printf("req %s timedout", r)

	}
	return rsp
}

func (r *RedisRequest) HandleResponse(rsp common.IResponse) error {
	r.receivedResponses = r.receivedResponses + 1
	//log.Printf("request %s received responses %d max_responses %d", r.Name, r.received_responses, r.max_responses)
	r.done <- rsp
	return nil
}

func (r *RedisRequest) SetResponseCounts(quorumResponses, maxResponses int) {
	r.quorumResponses = quorumResponses
	r.maxResponses = maxResponses
}

// Redis request Parser
type RedisRequestParser struct {
}

func NewRedisRequestParser() RedisRequestParser {
	return RedisRequestParser{}
}

func (parser RedisRequestParser) GetNextRequest(owner common.Context, r *bufio.Reader, consistency common.Consistency,
	topo common.INodePlacement) (common.IRequest, error) {

	line, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	if len(line) == 0 {
		return nil, fmt.Errorf("Empty line")
	}

	var argsCount int

	if _, err := fmt.Sscanf(line, "*%d\r\n", &argsCount); err != nil {
		return nil, fmt.Errorf("invalid number of arguments in %s", line)
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

	override := GetRequestOverride(requestType, consistency)
	quorumResponses, maxResponses := topo.GetResponseCounts(override, consistency)
	req := &RedisRequest{
		BaseMessage: struct {
			Id      uint64
			MsgType common.MessageType
		}{
			Id:      common.GetNextId(),
			MsgType: common.REQUEST_DATASTORE,
		},
		requestType:       requestType,
		Name:              strings.ToUpper(string(firstArg)),
		override:          override,
		ctx:               owner,
		Args:              args,
		quorumResponses:   quorumResponses,
		maxResponses:      maxResponses,
		receivedResponses: 0,
		done:              make(chan common.IResponse, 5), // TODO: this is a hack, ideally the reader of this channel should close the channel
	}

	req.hashCode = hashkit.GetHash(req.getKey())
	return req, nil
}
