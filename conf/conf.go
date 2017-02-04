package conf

import (
	"errors"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
)

var (
	Port       int
	Datacenter string
	Rack       string
	hostaddr   string
)

type Conf struct {
	Pool struct {
		Datacenter         string
		Rack               string
		DynListen          string   `dyn_listen`
		DynSeeds           []string `dyn_seeds`
		Listen             string
		Servers            []string `servers`
		Tokens             string
		Encryption         string `secure_server_option`
		KeyFile            string `pem_key_file`
		DataStore          int    `data_store`
		ReadConsistency    string `read_consistency`
		WriteConsistency   string `write_consistency`
		Hash               string `hash`
		HashTag            string
		Distribution       string
		Timeout            int
		Backlog            int
		ClientConnections  int `client_connections`
		PreConnect         bool
		AutoEjectHosts     bool   `auto_eject_hosts`
		ServerConnections  int    `server_connections`
		ServerRetryTimeout int    `server_retry_timeout`
		ServerFailureLimit int    `server_failure_limit`
		DynReadTimeout     int    `dyn_read_timeout`
		DynWriteTimeout    int    `dyn_write_timeout`
		DynSeedsProvider   string `dyn_seed_provider`
		DynPort            int    `dyn_port`
		DynConnections     int    `dyn_connections`
		GosInterval        int    `gos_interval`
		Env                string `env`
		ConnMsgRate        int    `conn_msg_rate`
		MBufSize           int    `mbuf_size`
		MaxMsgs            int    `max_msgs`
		StatsListen        string `stats_listen`
	} `dyn_o_mite`
}

func verifyConf(conf Conf) error {
	if conf.Pool.ReadConsistency != "DC_ONE" {
		return errors.New("Invalid configuration for read_consistency in conf file")
	}
	if conf.Pool.WriteConsistency != "DC_ONE" {
		return errors.New("Invalid configuration for write_consistency in conf file")
	}
	return nil
}

func init() {
	log.Println("initing")
}

func Parse(fileName string) (Conf, error) {
	var conf Conf
	log.Printf("Parsing file " + fileName)
	data, err := ioutil.ReadFile(fileName)
	if err != nil {
		log.Fatal("error reading file ", fileName)
		return conf, err
	}

	// User yaml parse
	// start storing things in it
	err = yaml.Unmarshal(data, &conf)
	if err != nil {
		log.Fatalf("error: %v", err)
		return conf, nil
	}

	return conf, nil

}
