package hashkit

// This is a file test hash function really

/*
#include <stdio.h>
#include <string.h>

unsigned int hash(const char *key, size_t length) {
	const unsigned int m = 0x5bd1e995;
	const unsigned int seed = (0xdeadbeef * (unsigned int)length);
	const int r = 24;

	unsigned int h = seed ^ (unsigned int)length;

	const unsigned char * data = (const unsigned char *)key;

	while (length >= 4) {
		unsigned int k = *(unsigned int *)data;

		k *= m;
		k ^= k >> r;
		k *= m;

		h *= m;
		h ^= k;


		data += 4;
		length -= 4;
	}
	switch(length) {
		case 3:
			h ^= ((unsigned int)data[2]) << 16;

		case 2:
			h ^= ((unsigned int)data[1]) << 8;

		case 1:
			h ^= data[0];
			h *= m;

		default:
			break;
	};

	h ^= h >> 13;
	h *= m;
	h ^= h >> 15;

	return h;
}
*/
import "C"
import (
	"fmt"
	"math/rand"
	"testing"
	"time"
	"unsafe"
)

const letterBytes = "abcdefghijklmnopqrstuvABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

func init() {
	rand.Seed(time.Now().UnixNano())
}

func getRandomKey() []byte {
	keyLen := rand.Intn(1023) + 1 // Add 1 for 0 sized keyLen
	key := make([]byte, keyLen)
	for j := 0; j < keyLen; j++ {
		key[j] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return key
}

func testHashMurmur(t *testing.T) {
	t.Logf("Staring test")
	for i := 0; i < 1000000; i++ {

		// Generate key
		key := getRandomKey()
		ccp := (*C.char)(unsafe.Pointer(&key[0]))

		// Calculate hashes
		goHash := hash_murmur(string(key))
		cHash := uint32(C.hash(ccp, C.size_t(len(key))))

		if goHash != cHash {
			fmt.Println("Hash mismatch Key", key, "\ngoHash:", goHash, "\ncHash:", cHash)
			fmt.Println("HashCode: ", goHash, "CHash:", cHash)
		}
	}

}
