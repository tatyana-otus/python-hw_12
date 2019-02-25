package apps

import (
	"errors"
	"log"
	"strconv"
	"strings"

	"github.com/golang/protobuf/proto"
)

func Parse(s string, msg *UserApps) (string, string, error) {

	d := strings.Split(strings.TrimSpace(s), "\t")
	if len(d) != 5 {
		log.Printf("parsing error: %s", s)
		return "", "", errors.New("parsing error")
	}
	dev_type, dev_id, lat, lon, raw_apps := d[0], d[1], d[2], d[3], d[4]
	if dev_type == "" || dev_id == "" {
		log.Printf("parsing error: %s", s)
		return "", "", errors.New("parsing error")
	}
	for _, str_app := range strings.Split(raw_apps, ",") {
		v, err := strconv.ParseInt(str_app, 10, 32)
		if err == nil {
			msg.Apps = append(msg.Apps, uint32(v))
		} else {
			log.Printf("Not all user apps are digits: %s", s)
		}
	}
	lat_f, err := strconv.ParseFloat(lat, 64)
	if err != nil {
		log.Printf("Invalid geo coords: %s", s)
		lat_f = 0
	}
	lon_f, err := strconv.ParseFloat(lon, 64)
	if err != nil {
		log.Printf("Invalid geo coords: %s", s)
		lon_f = 0
	}
	*msg.Lat = lat_f
	*msg.Lon = lon_f

	return dev_type, dev_id, nil
}

func Serialize(msg *UserApps) ([]byte, error) {
	return proto.Marshal(msg)
}
