package hashkit

import (
	"bitbucket.org/shailesh33/dynomite/conf"
	"errors"
)
var hashType string


func get_hash(key string ) uint64 {
	switch hashType {
	case "HASH_MURMUR":
		return hash_murmur(key)
	}
	return 0;
}

func validate_hash_type() error {
	switch hashType {
	case "HASH_MURMUR":
		return nil
	default:
		return errors.New("Invalid hash type")
	}
}

func init_hashkit(conf conf.Conf) error {
	hashType = conf.Pool.Hash
	return nil
}