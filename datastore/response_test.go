package datastore

import (
	"bufio"
	"os"
	"testing"
)

func TestResponse(t *testing.T) {
	var r ArrayResponse
	r.AppendArgs([]byte("test1"))
	r.AppendArgs([]byte("test2"))

	stdoutW := bufio.NewWriter(os.Stdout)
	r.Write(stdoutW)
	stdoutW.Flush()
}
