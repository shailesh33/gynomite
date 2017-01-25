package topology


import (
	"testing"
	"io/ioutil"
	"path/filepath"
	"bitbucket.org/shailesh33/dynomite/conf"
	"fmt"
)

func TestInitTopology(t *testing.T) {
	dir := "../conf/"
	data, err := ioutil.ReadDir(dir)
	if err != nil {
		t.Fatal("Failed to open directory .")
	}
	for _, f := range data {
		fullPath := dir + f.Name()
		ext := filepath.Ext(fullPath)
		if ext == ".yml" {
			t.Logf("Parsing file " + fullPath)
			c, err := conf.Parse(fullPath)
			topo, err := InitTopology(c)
			if err != nil {
				fmt.Println("failed to init topology for %s", f.Name())
			}
			Topology_print(topo)
		}
	}
}
