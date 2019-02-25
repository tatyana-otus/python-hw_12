package main

import (
	"./apps"
	"bufio"
	"compress/gzip"
	"errors"
	"github.com/bradfitz/gomemcache/memcache"
	"os"
	"os/exec"
	"testing"
)

const TEST_FILE = "data/tmp_uniq_key.gz"

func restart_mc() {
	exec.Command("killall", "memcached").Run()
	exec.Command("killall", "memcached").Run()
	exec.Command("memcached", "-p", "33013").Start()
	exec.Command("memcached", "-p", "33014").Start()
	exec.Command("memcached", "-p", "33015").Start()
	exec.Command("memcached", "-p", "33016").Start()
}

func compare_mc_value(value_1 *[]byte, value_2 *[]byte) error {
	if len(*value_1) != len(*value_2) {
		return errors.New("Not equivalent by length")
	}
	for i := 0; i < len(*value_1); i++ {
		if (*value_1)[i] != (*value_2)[i] {
			return errors.New("Not equivalent")
		}
	}
	return nil
}

func TestMcLoad(t *testing.T) {
	mc_client := map[string]*memcache.Client{
		"idfa": memcache.New("127.0.0.1:33013"),
		"gaid": memcache.New("127.0.0.1:33014"),
		"adid": memcache.New("127.0.0.1:33015"),
		"dvid": memcache.New("127.0.0.1:33016")}
	restart_mc()

	exec.Command("go", "run", "main.go", "--test", "--pattern", "data/tmp_uniq_key.gz").Run()

	msg := apps.UserApps{Apps: make([]uint32, 0, 1024),
		Lat: new(float64),
		Lon: new(float64)}
	f, err := os.Open(TEST_FILE)
	defer f.Close()
	if err != nil {
		t.Fatalf("%s", err)
	}
	gr, err := gzip.NewReader(f)
	if err != nil {
		t.Fatalf("%s", err)
	}
	scanner := bufio.NewScanner(gr)
	for scanner.Scan() {
		s := scanner.Text()
		msg.Apps = msg.Apps[:0]
		dev_type, dev_id, _ := apps.Parse(s, &msg)
		key := dev_type + ":" + dev_id
		data, err := apps.Serialize(&msg)
		if err != nil {
			t.Fatalf("Marshaling error %s", err)
			return
		}
		item, err := mc_client[dev_type].Get(key)

		if err != nil {
			t.Fatalf("MC client error %s", err)
		}
		if err := compare_mc_value(&item.Value, &data); err != nil {
			t.Fatalf("key: %s (%s)", key, err)
		}
	}
}
