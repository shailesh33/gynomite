package hashkit

import (
	"errors"
	"log"
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
	switch h {
	case "":
		hashType = HASH_MURMUR
		return nil
	case "HASH_MURMUR":
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
