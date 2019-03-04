package main

import (
	"./apps"

	"bufio"
	"compress/gzip"
	"errors"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/golang/protobuf/proto"
)

const TEST_FILE = "testdata/tmp_uniq_key.gz"

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

	exec.Command("go", "run", "main.go", "--test", "--pattern", TEST_FILE).Run()

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
		parsedApps, _ := parse(s)
		key := parsedApps.devType + ":" + parsedApps.devId
		data, err := proto.Marshal(&apps.UserApps{Apps: parsedApps.apps,
			Lat: proto.Float64(parsedApps.lat),
			Lon: proto.Float64(parsedApps.lon)})
		if err != nil {
			t.Fatalf("Marshaling error %s", err)
			return
		}
		item, err := mc_client[parsedApps.devType].Get(key)
		if err != nil {
			t.Fatalf("MC client error %s", err)
		}
		if err := compare_mc_value(&item.Value, &data); err != nil {
			t.Fatalf("key: %s (%s)", key, err)
		}
	}
}

func TestApps(t *testing.T) {
	sample := "idfa\t1rfw452y52g2gq4g\t55.55\t42.42\t1423,43,567,3,7,23\ngaid\t7rfw452y52g2gq4g\t55.55\t42.42\t7423,424"

	for _, line := range strings.Split(sample, "\n") {
		parsedApps, _ := parse(line)
		data, err := proto.Marshal(&apps.UserApps{Apps: parsedApps.apps,
			Lat: proto.Float64(parsedApps.lat),
			Lon: proto.Float64(parsedApps.lon)})
		if err != nil {
			t.Fatalf("Marshaling error expected nil but got %s", err)
			return
		}

		msg2 := new(apps.UserApps)
		err = proto.Unmarshal(data, msg2)
		if err != nil {
			t.Fatalf("Unmarshaling error expected nil but got %s", err)
		}

		if len(parsedApps.apps) != len(msg2.Apps) {
			t.Fatalf("Error apps size %d != %d", len(parsedApps.apps), len(msg2.Apps))
		}

		for i := 0; i < len(parsedApps.apps); i++ {
			if parsedApps.apps[i] != msg2.Apps[i] {
				t.Fatalf("Error in apps %d != %d", parsedApps.apps[i], msg2.Apps[i])
			}
		}
		if parsedApps.lat != *msg2.Lat {
			t.Fatalf("Error in Lat %f != %f", parsedApps.lat, *msg2.Lat)
		}
		if parsedApps.lon != *msg2.Lon {
			t.Fatalf("Error in Lon %f != %f", parsedApps.lon, *msg2.Lon)
		}
	}
}
