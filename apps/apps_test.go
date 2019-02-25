package apps

import (
	"github.com/golang/protobuf/proto"
	"strings"
	"testing"
)

func TestApps(t *testing.T) {
	sample := "idfa\t1rfw452y52g2gq4g\t55.55\t42.42\t1423,43,567,3,7,23\ngaid\t7rfw452y52g2gq4g\t55.55\t42.42\t7423,424"

	msg := UserApps{Apps: make([]uint32, 0, 1024),
		Lat: new(float64),
		Lon: new(float64)}
	for _, line := range strings.Split(sample, "\n") {
		Parse(line, &msg)

		msg.Apps = msg.Apps[:0]
		data, err := proto.Marshal(&msg)
		if err != nil {
			t.Fatalf("Marshaling error expected nil but got %s", err)
			return
		}

		msg2 := new(UserApps)
		err = proto.Unmarshal(data, msg2)
		if err != nil {
			t.Fatalf("Unmarshaling error expected nil but got %s", err)
		}

		for i := 0; i < len(msg.Apps); i++ {
			if msg.Apps[i] != msg2.Apps[i] {
				t.Fatalf("Error in apps %d != %d", msg.Apps[i], msg2.Apps[i])
			}
		}
		if *msg.Lat != *msg2.Lat {
			t.Fatalf("Error in Lat %f != %f", *msg.Lat, *msg2.Lat)
		}
		if *msg.Lon != *msg2.Lon {
			t.Fatalf("Error in Lon %f != %f", *msg.Lon, *msg2.Lon)
		}
	}
}
