package main

import (
	"gopkg.in/yaml.v2"
	"fmt"
	"log"
)

type Conf struct {
	Pool	struct {
		Datacenter	string
		Rack		string
		DynListen	string `dyn_listen`
		DynSeeds	[]string `dyn_seeds`
		Listen		string
		Servers		[]string
		Tokens		string
		Encryption string `secure_server_option`
		KeyFile	string `pem_key_file`
		DataStore	int    `data_store`
		ReadConsistency	string `read_consistency`
		WriteConsistency	string `write_consistency`
		Hash		string
		HashTag		string
		Distribution	string
		Timeout		int
		Backlog		int
		ClientConnections int `client_connections`
		PreConnect	bool
		AutoEjectHosts  bool `auto_eject_hosts`
		ServerConnections int `server_connections`
		ServerRetryTimeout int `server_retry_timeout`
		ServerFailureLimit int `server_failure_limit`
		DynReadTimeout int `dyn_read_timeout`
		DynWriteTimeout int `dyn_write_timeout`
		DynSeedsProvider string `dyn_seed_provider`
		DynPort	int `dyn_port`
		DynConnections int `dyn_connections`
		GosInterval int `gos_interval`
		Env	string `env`
		ConnMsgRate int `conn_msg_rate`
			  } `dyn_o_mite`

}

func Parse(fileName string) Conf {
	// TODO: Try to access and open filname
	// panic if fail

	// User yaml parse
	// start storing things in it
	var conf Conf
	data := []byte(`dyn_o_mite:
  datacenter: dc
  rack: rack3
  dyn_listen: 127.0.0.3:8101
  dyn_seeds:
  - 127.0.0.1:8101:rack1:dc:1383429731
  - 127.0.0.2:8101:rack2:dc:1383429731
  listen: 127.0.0.3:8102
  servers:
  - 127.0.0.1:22123:1
  tokens: '1383429731'
  secure_server_option: datacenter
  pem_key_file: conf/dynomite.pem
  data_store: 0
  read_consistency : DC_ONE
  write_consistency : DC_ONE`)

	err := yaml.Unmarshal(data, &conf)
	if err != nil {
		log.Fatalf("error: %v", err)
	}
	fmt.Printf("--- config:\n%v\n\n", conf)
	fmt.Println(conf.Pool.KeyFile)
	return conf

}

func main() {
	Parse("")
}