package hashkit

import (
	"errors"
	"log"
	"strings"
)

type HashType int

const (
	HASH_MURMUR = iota
)

var hashType HashType

func GetHash(key []byte) uint32 {
	switch hashType {
	case HASH_MURMUR:
		return hash_murmur(key)
	}
	log.Panic("Hash Type not set")
	return 0
}

func set_hash_type(h string) error {
	if len(h) == 0 {
		hashType = HASH_MURMUR
		return nil
	}
	if (strings.EqualFold("MURMUR", h)) {
		hashType = HASH_MURMUR
		return nil
	}
	log.Panic("Invalid hash type", h)
	return errors.New("Invalid hash type or Hash module not initialized")
}

func init() {
	log.Println("hashkit")
}

func InitHashkit(h string) error {
	return set_hash_type(h)
}
