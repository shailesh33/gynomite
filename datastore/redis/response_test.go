package datastore

import (
	"bufio"
	"os"
	"testing"
)

func TestResponse(t *testing.T) {
	var r ArrayResponse
	r1 := NewStringResponse([]byte("test1"))
	r.AppendArgs(r1)
	r2 := NewStringResponse([]byte("test1"))
	r.AppendArgs(r2)

	stdoutW := bufio.NewWriter(os.Stdout)
	r.Write(stdoutW)
	stdoutW.Flush()
}
