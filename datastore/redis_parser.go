package datastore

import (
	"bufio"
	"fmt"
	"strings"

	"bitbucket.org/shailesh33/dynomite/common"
	"log"
	"strconv"
)

type RedisRequest struct {
	//common.Request
	Name        string
	requestType common.RequestType
	ctx         common.Context
	Args        [][]byte
}

func (r RedisRequest) Write(w *bufio.Writer) error {
	w.WriteByte('*')
	w.WriteString(strconv.Itoa(len(r.Args) + 1))
	w.WriteString("\r\n")
	w.WriteByte('$')
	w.WriteString(strconv.Itoa(len(r.Name)))
	w.WriteString("\r\n")
	w.WriteString(r.Name)
	w.WriteString("\r\n")
	log.Println("Writing", r)
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

func (r RedisRequest) GetType() common.RequestType {
	return r.requestType
}

func (r RedisRequest) GetContext() common.Context {
	return r.ctx
}

// Redis request Parser
type RedisRequestParser struct {
	r     *bufio.Reader
	owner common.Context
}

func newRedisRequestParser(r *bufio.Reader, owner common.Context) RedisRequestParser {
	return RedisRequestParser{r: r, owner: owner}
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

	return RedisRequest{
		requestType: GetRequestType(string(firstArg)),
		Name:        strings.ToUpper(string(firstArg)),
		ctx:         parser.owner,
		Args:        args,
	}, nil
}
