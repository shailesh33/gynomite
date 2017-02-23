package datastore

import (
	"bitbucket.org/shailesh33/dynomite/common"
	"bufio"
	"fmt"
	"log"
	"strconv"
)

/////////// integer response
type integerResponse struct {
	I int
}

func NewIntegerResponse(i int) integerResponse {
	return integerResponse{I: i}
}

func (r integerResponse) Write(w *bufio.Writer) error {
	w.WriteByte(':')
	w.WriteString(strconv.Itoa(r.I))
	w.WriteString("\r\n")
	w.Flush()
	return nil
}

func (parser redisResponseParser) integerResponseParser() (integerResponse, error) {
	r := parser.r
	line, err := r.ReadString('\n')
	if err != nil {
		return integerResponse{}, err
	}
	if len(line) == 0 {
		return integerResponse{}, fmt.Errorf("Empty line")
	}
	var i int

	if _, err := fmt.Sscanf(line, ":%d\r\n", &i); err != nil {
		return integerResponse{}, fmt.Errorf("invalid status ", line)
	}
	log.Println("Returning Integer ", i)

	return NewIntegerResponse(i), nil
}

//////////// Status response
type StatusResponse struct {
	S string
}

func NewStatusResponse(s string) StatusResponse {
	return StatusResponse{S: s}
}

func (parser redisResponseParser) statusResponseParser() (StatusResponse, error) {
	r := parser.r
	line, err := r.ReadString('\n')
	if err != nil {
		return StatusResponse{}, err
	}
	if len(line) == 0 {
		return StatusResponse{}, fmt.Errorf("Empty line")
	}
	var s string

	if _, err := fmt.Sscanf(line, "+%s\r\n", &s); err != nil {
		return StatusResponse{}, fmt.Errorf("invalid status ", line)
	}
	log.Println("Returning Status ", s)

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
	ErrorString string
}

func NewErrorResponse(s string) ErrorResponse {
	return ErrorResponse{ErrorString: s}
}

func (parser redisResponseParser) errorResponseParser() (ErrorResponse, error) {
	r := parser.r
	line, err := r.ReadString('\n')
	if err != nil {
		return ErrorResponse{}, err
	}
	if len(line) == 0 {
		return ErrorResponse{}, fmt.Errorf("Empty line")
	}
	var s string

	if _, err := fmt.Sscanf(line, "-%s\r\n", &s); err != nil {
		return ErrorResponse{}, fmt.Errorf("invalid status ", line)
	}
	log.Println("Returning Error ", s)

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
	String string
}

func NewStringResponse(s string) StringResponse {
	return StringResponse{String: s}
}

func (parser redisResponseParser) stringResponseParser() (StringResponse, error) {
	r := parser.r
	line, err := r.ReadString('\n')
	if err != nil {
		return StringResponse{}, err
	}
	if len(line) == 0 {
		return StringResponse{}, fmt.Errorf("Empty line")
	}
	var length int

	if _, err := fmt.Sscanf(line, "$%d\r\n", &length); err != nil {
		return StringResponse{}, fmt.Errorf("invalid length for string ", line, err)
	}

	var s string
	line, err = r.ReadString('\n')
	if err != nil {
		return StringResponse{}, err
	}
	if len(line) == 0 {
		return StringResponse{}, fmt.Errorf("Empty line")
	}

	if _, err := fmt.Sscanf(line, "%s\r\n", &s); err != nil {
		return StringResponse{}, fmt.Errorf("invalid length for string ", line, err)
	}
	log.Println("Returning String ", s)
	return NewStringResponse(s), nil
}

func (r StringResponse) Write(w *bufio.Writer) error {
	w.WriteString("$" + strconv.Itoa(len(r.String)))
	w.WriteString("\r\n" + r.String + "\r\n")
	w.Flush()
	return nil
}

//////////////// array response
type ArrayResponse struct {
	elems []common.Response
}

func (r ArrayResponse) Write(w *bufio.Writer) error {
	w.WriteByte('*')
	w.WriteString(strconv.Itoa(len(r.elems)))
	w.WriteString("\r\n")
	for _, i := range r.elems {
		i.Write(w)
		//w.WriteByte('$')
		//w.WriteString(strconv.Itoa(len(i)))
		//w.WriteString("\r\n")
		//w.Write(i)
		//w.WriteString("\r\n")
	}
	w.Flush()
	return nil
}

func (r ArrayResponse) AppendArgs(elem common.Response) {
	r.elems = append(r.elems, elem)
}

func NewArrayResponse() ArrayResponse {
	return ArrayResponse{}
}

//TODO: The array response can have different types of elements in it
// For example it could be array with few integers, some strings etc.
func (parser redisResponseParser) arrayResponseParser() (ArrayResponse, error) {
	rsp := NewArrayResponse()
	r := parser.r
	line, err := r.ReadString('\n')
	if err != nil {
		return ArrayResponse{}, err
	}
	if len(line) == 0 {
		return ArrayResponse{}, fmt.Errorf("Empty line")
	}
	var num int

	if _, err := fmt.Sscanf(line, "*%d\r\n", &num); err != nil {
		return ArrayResponse{}, fmt.Errorf("invalid length for array ", line, err)
	}
	rsp.elems = make([]common.Response, num)
	for i := 0; i < num; i += 1 {
		rsp.elems[i], err = parser.GetNextMessage()
		//if rsp.elems[i], err = readArgument(r); err != nil {
		if err != nil {
			log.Println("Received error ", err)
			return ArrayResponse{}, err
		}
		//rsp.AppendArgs(elem.(common.Response))
	}
	return rsp, nil
}

// Redis Response Parser
type redisResponseParser struct {
	r *bufio.Reader
}

func newRedisResponseParser(r *bufio.Reader) redisResponseParser {
	return redisResponseParser{r: r}
}

func (parser redisResponseParser) GetNextMessage() (common.Message, error) {
	// peek first byte
	b, err := parser.r.Peek(1)
	if err != nil {
		return nil, fmt.Errorf("received error", err, " first byte ", b[0])
	}
	switch b[0] {
	case '$':
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
