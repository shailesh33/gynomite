package datastore

import (
	"github.com/shailesh33/gynomite/common"
	"bufio"
	"fmt"
	"io"
	"log"
)

type nilResponse struct {
	common.BaseMessage
}

func newNilResponse() *nilResponse {
	return &nilResponse{
		BaseMessage :struct {
			Id uint64
			MsgType     common.MessageType
		} {
			Id:common.GetNextId(),
			MsgType:common.RESPONSE_DATASTORE,
		},
	}
}

func (r *nilResponse) Write(w io.Writer) error {
	fmt.Fprint(w, "$-1\r\n")
	return nil
}

func (parser redisResponseParser) nilResponseParser() (*nilResponse, error) {
	r := parser.r
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}

	if line != "$-1\r\n" {
		return nil, fmt.Errorf("Received invalid line while parsing nil response %s", line)
	}
	return newNilResponse(), nil
}

/////////// integer response
type integerResponse struct {
	common.BaseMessage
	I int
}

func newIntegerResponse(i int) *integerResponse {
	return &integerResponse{
		BaseMessage :struct {
			Id uint64
			MsgType     common.MessageType
		} {
			Id: common.GetNextId(),
			MsgType:common.RESPONSE_DATASTORE,
		},
		I: i,
	}
}

func (r *integerResponse) Write(w io.Writer) error {
	fmt.Fprintf(w, ":%d\r\n", r.I)
	return nil
}

func (parser redisResponseParser) integerResponseParser() (*integerResponse, error) {
	r := parser.r
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	if len(line) == 0 {
		return nil, fmt.Errorf("Empty line")
	}
	var i int

	if _, err := fmt.Sscanf(line, ":%d\r\n", &i); err != nil {
		return nil, fmt.Errorf("invalid status ", line)
	}

	return newIntegerResponse(i), nil
}

//////////// Status response
type StatusResponse struct {
	common.BaseMessage
	S string
}

func NewStatusResponse(s string) *StatusResponse {
	return &StatusResponse{
		BaseMessage :struct {
			Id uint64
			MsgType     common.MessageType
		} {
			Id: common.GetNextId(),
			MsgType:common.RESPONSE_DATASTORE,
		},
		S: s,
	}
}

func (parser redisResponseParser) statusResponseParser() (*StatusResponse, error) {
	r := parser.r
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	if len(line) == 0 {
		return nil, fmt.Errorf("Empty line")
	}
	var s string

	if _, err := fmt.Sscanf(line, "+%s\r\n", &s); err != nil {
		return nil, fmt.Errorf("invalid status ", line)
	}

	return NewStatusResponse(s), nil
}

func (r *StatusResponse) Write(w io.Writer) error {
	fmt.Fprintf(w, "+%s\r\n", r.S)
	return nil
}

/////////////// error response
// error response
type ErrorResponse struct {
	common.BaseMessage
	ErrorString string
}

func NewErrorResponse(s string) *ErrorResponse {
	return &ErrorResponse{
		BaseMessage :struct {
			Id uint64
			MsgType     common.MessageType
		} {
			Id: common.GetNextId(),
			MsgType:common.RESPONSE_DATASTORE,
		},
		ErrorString: s,
	}
}

func (parser redisResponseParser) errorResponseParser() (*ErrorResponse, error) {
	r := parser.r
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	if len(line) == 0 {
		return nil, fmt.Errorf("Empty line")
	}
	var s string

	if _, err := fmt.Sscanf(line, "-%s\r\n", &s); err != nil {
		return nil, fmt.Errorf("invalid status ", line)
	}

	return NewErrorResponse(s), nil
}

func (r *ErrorResponse) Write(w io.Writer) error {
	fmt.Fprintf(w, "-%s\r\n", r.ErrorString)
	return nil
}

/////////////// string response
type StringResponse struct {
	common.BaseMessage
	data []byte
}

func NewStringResponse(b []byte) *StringResponse {
	return &StringResponse{
		BaseMessage :struct {
			Id uint64
			MsgType     common.MessageType
		} {
			Id: common.GetNextId(),
			MsgType:common.RESPONSE_DATASTORE,
		},
		data: b,
	}
}

func (parser redisResponseParser) stringResponseParser() (*StringResponse, error) {
	r := parser.r
	line, err := r.ReadString('\n')

	if err != nil {
		return nil, err
	}
	if len(line) == 0 {
		return nil, fmt.Errorf("Empty line")
	}
	var length int

	if _, err := fmt.Sscanf(line, "$%d\r\n", &length); err != nil {
		return nil, fmt.Errorf("invalid length for string ", line, err)
	}

	b := make([]byte, length)
	read, err := io.ReadFull(r, b)
	if err != nil {
		log.Println("Failed to read full", length, "bytes:", err)
		return nil, err

	}
	if read != length {
		log.Println("Failed to read full", length, "bytes")
		return nil, fmt.Errorf("Read only ", read, "bytes from stream out of ", length)
	}
	c, err := r.ReadByte()
	if err != nil {
		return nil, fmt.Errorf("while reading \\r", err)

	}
	if c != '\r' {
		return nil, fmt.Errorf("Expected \\r")
	}

	c, err = r.ReadByte()
	if err != nil {
		return nil, fmt.Errorf("while reading \\n", err)

	}
	if c != '\n' {
		return nil, fmt.Errorf("Expected \\n")
	}

	return NewStringResponse(b), nil
}

func (r *StringResponse) Write(w io.Writer) error {
	fmt.Fprintf(w, "$%d\r\n%s\r\n", len(r.data), string(r.data))
	return nil
}

//////////////// array response
type ArrayResponse struct {
	common.BaseMessage
	elems []common.Response
}

func (r *ArrayResponse) Write(w io.Writer) error {
	fmt.Fprintf(w, "*%d\r\n", len(r.elems))
	for _, i := range r.elems {
		i.Write(w)
	}

	return nil
}

func (r *ArrayResponse) AppendArgs(elem common.Response) {
	r.elems = append(r.elems, elem)
}

func NewArrayResponse() *ArrayResponse {
	return &ArrayResponse{
		BaseMessage :struct {
			Id uint64
			MsgType     common.MessageType
		} {
			Id: common.GetNextId(),
			MsgType:common.RESPONSE_DATASTORE,
		},
	}
}

//TODO: The array response can have different types of elements in it
// For example it could be array with few integers, some strings etc.
func (parser redisResponseParser) arrayResponseParser() (*ArrayResponse, error) {
	rsp := NewArrayResponse()
	r := parser.r
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	if len(line) == 0 {
		return nil, fmt.Errorf("Empty line")
	}
	var num int

	if _, err := fmt.Sscanf(line, "*%d\r\n", &num); err != nil {
		return nil, fmt.Errorf("invalid length for array ", line, err)
	}
	rsp.elems = make([]common.Response, num)
	for i := 0; i < num; i += 1 {
		rsp.elems[i], err = parser.GetNextResponse()
		//if rsp.elems[i], err = readArgument(r); err != nil {
		if err != nil {
			log.Println("Received error ", err)
			return nil, err
		}
		//rsp.AppendArgs(elem.(common.Response))
	}
	return rsp, nil
}

// Redis Response Parser
type redisResponseParser struct {
	r *bufio.Reader
}

func NewRedisResponseParser(r *bufio.Reader) redisResponseParser {
	return redisResponseParser{r: r}
}

func (parser redisResponseParser) GetNextResponse() (common.Response, error) {
	// peek first byte
	b, err := parser.r.Peek(1)
	if err != nil {
		if len(b) > 0 {
			return nil, fmt.Errorf("received error", err, " first byte :'", b[0], "'")
		} else {
			return nil, fmt.Errorf("received error", err)
		}

	}
	switch b[0] {
	case '$':
		b, err = parser.r.Peek(2)
		if b[1] == '-' {
			return parser.nilResponseParser()
		}
		return parser.stringResponseParser()
	case '+':
		return parser.statusResponseParser()
	case ':':
		return parser.integerResponseParser()
	case '-':
		return parser.errorResponseParser()
	case '*':
		return parser.arrayResponseParser()

	}

	return nil, fmt.Errorf("UNREACHED")
}
