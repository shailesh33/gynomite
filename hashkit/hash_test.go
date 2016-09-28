package hashkit

import (
	"testing"
	"fmt"
)

func TestHashMurmur(T *testing.T) {
	hash:= hash_murmur("This is a key")
	fmt.Println("HashCode: ", hash)
}