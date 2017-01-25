package hashkit

import (
	"bitbucket.org/shailesh33/dynomite/conf"
	"log"
	"errors"
)
type HashType int

const (
	HASH_MURMUR = iota
)

var hashType HashType

func GetHash(key string) uint32 {
	switch hashType {
	case HASH_MURMUR:
		return hash_murmur(key)
	}
	log.Panic("Hash Type not set")
	return 0
}

func set_hash_type(h string) error {
	switch h {
	case "HASH_MURMUR":
		hashType = HASH_MURMUR
		return nil
	}
	log.Panic("Invalid hash type", h)
	return errors.New("Invalid hash type or Hash module not initialized")
}

func InitHashkit(conf conf.Conf) error {
	return set_hash_type(conf.Pool.Hash)
}