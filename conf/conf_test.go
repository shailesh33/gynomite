package conf

import (
	"testing"
	"io/ioutil"
	"path/filepath"
)

func TestParseConfFiles(t *testing.T) {
	data, err := ioutil.ReadDir(".")
	if err != nil {
		t.Fatal("Failed to open directory .")
	}
	for _, f := range data {
		ext := filepath.Ext(f.Name())
		if ext == ".yml" {
			Parse(f.Name())
		}
	}
}