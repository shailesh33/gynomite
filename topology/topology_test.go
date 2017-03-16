package topology

import (
	"github.com/shailesh33/gynomite/conf"
	"fmt"
	"io/ioutil"
	"path/filepath"
	"testing"
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
			topo.Print()
		}
	}
}
