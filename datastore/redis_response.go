package datastore

import (
	"bitbucket.org/shailesh33/dynomite/common"
	"bufio"
	"fmt"
	"strconv"
)

/////////// integer response
type IntegerResponse struct {
	common.Response
	I int
}

func NewIntegerResponse(i int) *IntegerResponse {
	return &IntegerResponse{I: i}
}

func (r *IntegerResponse) Write(w *bufio.Writer) error {
	w.WriteByte(':')
	w.WriteString(strconv.Itoa(r.I))
	w.WriteString("\r\n")
	w.Flush()
	return nil
}

func integerResponseParser(r *bufio.Reader) (*IntegerResponse, error) {
	rsp := IntegerResponse{}
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
	rsp.I = i
	return &rsp, nil
}

//////////// Status response
type StatusResponse struct {
	common.Response
	S string
}

func NewStatusResponse(s string) *StatusResponse {
	return &StatusResponse{S: s}
}

func statusResponseParser(r *bufio.Reader) (common.Message, error) {
	rsp := StatusResponse{}
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
	rsp.S = s
	return &rsp, nil
}

func (r *StatusResponse) Write(w *bufio.Writer) error {
	w.WriteString("-" + r.S)
	w.WriteString("\r\n")
	w.Flush()
	return nil
}

/////////////// error response
// error response
type ErrorResponse struct {
	common.Response
	ErrorString string
}

func NewErrorResponse(s string) *ErrorResponse {
	return &ErrorResponse{ErrorString: s}
}

func errorResponseParser(r *bufio.Reader) (*ErrorResponse, error) {
	rsp := ErrorResponse{}
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
	rsp.ErrorString = s
	return &rsp, nil
}

func (r *ErrorResponse) Write(w *bufio.Writer) error {
	w.WriteString("-" + r.ErrorString)
	w.WriteString("\r\n")
	w.Flush()
	return nil
}

/////////////// string response
type StringResponse struct {
	common.Response
	String string
}

func NewStringResponse(s string) *StringResponse {
	return &StringResponse{String: s}
}

func stringResponseParser(r *bufio.Reader) (*StringResponse, error) {
	rsp := StringResponse{}
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

	var s string
	line, err = r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	if len(line) == 0 {
		return nil, fmt.Errorf("Empty line")
	}

	if _, err := fmt.Sscanf(line, "%s\r\n", &s); err != nil {
		return nil, fmt.Errorf("invalid length for string ", line, err)
	}
	rsp.String = s
	return &rsp, nil
}

func (r *StringResponse) Write(w *bufio.Writer) error {
	w.WriteString("-" + r.String)
	w.WriteString("\r\n")
	w.Flush()
	return nil
}

//////////////// array response
type ArrayResponse struct {
	common.Response
	Args [][]byte
}

func (r *ArrayResponse) Write(w *bufio.Writer) error {
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
	return nil
}

func (r *ArrayResponse) AppendArgs(arg []byte) {
	r.Args = append(r.Args, arg)
}

func NewArrayResponse() *ArrayResponse {
	r := new(ArrayResponse)
	return r
}

//TODO: The array response can have different types of elements in it
// For example it could be array with few integers, some strings etc.
func arrayResponseParser(r *bufio.Reader) (*ArrayResponse, error) {
	rsp := ArrayResponse{}
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
	rsp.Args = make([][]byte, num)
	for i := 0; i < num; i += 1 {
		if rsp.Args[i], err = readArgument(r); err != nil {
			return nil, err
		}
	}
	return &rsp, nil
}

// Redis Response Parser
type RedisResponseParser struct {
	r *bufio.Reader
}

func NewRedisResponseParser(r *bufio.Reader) RedisResponseParser {
	return RedisResponseParser{r: r}
}

func (parser RedisResponseParser) GetNextMessage() (common.Message, error) {
	// peek first byte
	b, err := parser.r.Peek(1)
	if err != nil {
		return nil, fmt.Errorf("received error", err, " first byte ", b[0])
	}
	switch b[0] {
	case '+':
		return statusResponseParser(parser.r)
	case ':':
		return integerResponseParser(parser.r)
	case '-':
		return errorResponseParser(parser.r)
	case '*':
		return arrayResponseParser(parser.r)

	}

	return nil, fmt.Errorf("UNREACHED")
}
