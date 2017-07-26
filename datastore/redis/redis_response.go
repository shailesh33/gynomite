package datastore

import (
	"github.com/shailesh33/gynomite/common"
	"bufio"
	"fmt"
	"io"
	"log"
	"strconv"
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

func (r nilResponse) Write(w *bufio.Writer) error {
	w.WriteString("$-1\r\n")
	w.Flush()
	return nil
}

func (parser redisResponseParser) parseNilResponse() (common.IResponse, error) {
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

func (r integerResponse) Write(w *bufio.Writer) error {
	w.WriteByte(':')
	w.WriteString(strconv.Itoa(r.I))
	w.WriteString("\r\n")
	w.Flush()
	return nil
}

func (parser redisResponseParser) parseIntegerResponse() (common.IResponse, error) {
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

func (parser redisResponseParser) parseStatusResponse() (common.IResponse, error) {
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

func (r StatusResponse) Write(w *bufio.Writer) error {
	w.WriteString("+" + r.S)
	w.WriteString("\r\n")
	w.Flush()
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

func (parser redisResponseParser) parseErrorResponse() (common.IResponse, error) {
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

func (r ErrorResponse) Write(w *bufio.Writer) error {
	w.WriteString("-" + r.ErrorString)
	w.WriteString("\r\n")
	w.Flush()
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

func (parser redisResponseParser) parseStringResponse() (common.IResponse, error) {
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

func (r StringResponse) Write(w *bufio.Writer) error {
	w.WriteString("$" + strconv.Itoa(len(r.data)) + "\r\n")
	w.Write(r.data)
	w.WriteString("\r\n")
	w.Flush()
	return nil
}

//////////////// array response
type ArrayResponse struct {
	common.BaseMessage
	elems []common.IResponse
}

func (r ArrayResponse) Write(w *bufio.Writer) error {
	w.WriteByte('*')
	w.WriteString(strconv.Itoa(len(r.elems)))
	w.WriteString("\r\n")
	for _, i := range r.elems {
		i.Write(w)
	}
	w.Flush()
	return nil
}

func (r ArrayResponse) AppendArgs(elem common.IResponse) {
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
func (parser redisResponseParser) parseArrayResponse() (common.IResponse, error) {
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
	rsp.elems = make([]common.IResponse, num)
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

func (parser redisResponseParser) GetNextResponse() (common.IResponse, error) {
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
			return parser.parseNilResponse()
		}
		return parser.parseStringResponse()
	case '+':
		return parser.parseStatusResponse()
	case ':':
		return parser.parseIntegerResponse()
	case '-':
		return parser.parseErrorResponse()
	case '*':
		return parser.parseArrayResponse()

	}

	return nil, fmt.Errorf("UNREACHED")
}
