package datastore

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
)

func readArgument(r *bufio.Reader) ([]byte, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	var length int
	if _, err = fmt.Sscanf(line, "$%d\r\n", &length); err != nil {
		return nil, fmt.Errorf("invalid length for argument in ", line)
	}

	// we know the length of the argument. Just read it.
	data, err := ioutil.ReadAll(io.LimitReader(r, int64(length)))
	if err != nil {
		return nil, err
	}
	if len(data) != length {
		return nil, fmt.Errorf("Expected length %d, received %d : data'%s'", length, len(data), data)
	}

	// Now check for trailing CR
	if b, err := r.ReadByte(); err != nil || b != '\r' {
		return nil, fmt.Errorf("Expected \\r, ", err)
	}

	// And LF
	if b, err := r.ReadByte(); err != nil || b != '\n' {
		return nil, fmt.Errorf("Expected \\n, ", err)
	}

	return data, nil
}
