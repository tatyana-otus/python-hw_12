package main

import (
	"./apps"

	"bufio"
	"compress/gzip"
	"errors"
	"flag"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/golang/protobuf/proto"
)

type (
	config struct {
		pattern    string
		workers    uint
		loaders    uint
		idfa       string
		gaid       string
		adid       string
		dvid       string
		timeout    int
		retries    int
		retryDelay int
		test       bool
	}

	memcacheInfo struct {
		client *memcache.Client
		queue  chan *memcache.Item
	}

	appsInstalled struct {
		devType string
		devId   string
		lat     float64
		lon     float64
		apps    []uint32
	}
)

const (
	constQueueSize     = 1024
	constNormalErrRate = 0.01
)

func parse(s string) (*appsInstalled, error) {

	d := strings.Split(strings.TrimSpace(s), "\t")
	if len(d) != 5 {
		log.Printf("parsing error: %s", s)
		return nil, errors.New("parsing error")
	}
	devType, devId, lat, lon, raw_apps := d[0], d[1], d[2], d[3], d[4]
	if devType == "" || devId == "" {
		log.Printf("parsing error: %s", s)
		return nil, errors.New("parsing error")
	}
	parsedApps := appsInstalled{devType: devType, devId: devId}
	for _, str_app := range strings.Split(raw_apps, ",") {
		v, err := strconv.ParseInt(str_app, 10, 32)
		if err == nil {
			parsedApps.apps = append(parsedApps.apps, uint32(v))
		} else {
			log.Printf("Not all user apps are digits: %s", s)
		}
	}
	lat_f, err := strconv.ParseFloat(lat, 64)
	if err != nil {
		log.Printf("Invalid geo coords: %s", s)
		lat_f = 0
	}
	parsedApps.lat = lat_f
	lon_f, err := strconv.ParseFloat(lon, 64)
	if err != nil {
		log.Printf("Invalid geo coords: %s", s)
		lon_f = 0
	}
	parsedApps.lon = lon_f

	return &parsedApps, nil
}

func worker(job chan string, loadersJob map[string]*memcacheInfo, errorsStat chan int) {
	errors := 0
	for s := range job {
		parsedApps, err := parse(s)
		if err != nil {
			errors++
			continue
		}
		key := parsedApps.devType + ":" + parsedApps.devId
		data, err := proto.Marshal(&apps.UserApps{Apps: parsedApps.apps,
			Lat: proto.Float64(parsedApps.lat),
			Lon: proto.Float64(parsedApps.lon)})
		if err != nil {
			log.Printf("%s", err)
			errors++
			continue
		}
		if _, ok := loadersJob[parsedApps.devType]; ok {
			loadersJob[parsedApps.devType].queue <- &memcache.Item{Key: key, Value: data}
		} else {
			errors++
		}
	}
	errorsStat <- errors
}

func loader(job chan *memcache.Item, client *memcache.Client,
	errorsStat chan int, processedStat chan int,
	retries int, retryDelay int) {
	errors := 0
	processed := 0

	for d := range job {
		attempts := retries
		var err error
		for attempts > 0 {
			err = client.Set(d)
			if err == nil {
				break
			}
			attempts -= 1
			time.Sleep(time.Duration(retryDelay) * time.Second)
		}
		if err != nil {
			log.Printf("%s", err)
			errors++
		} else {
			processed++
		}
	}
	errorsStat <- errors
	processedStat <- processed
}

func getConfig() *config {
	cfg := &config{}
	flag.StringVar(&cfg.pattern, "pattern", "src/data/tmp_uniq_key.gz", "file name pattern")
	flag.UintVar(&cfg.workers, "workers", uint(runtime.NumCPU()), "number of workers")
	flag.UintVar(&cfg.loaders, "loaders", 4, "number of loaders per Memcached address")
	flag.StringVar(&cfg.idfa, "idfa", "127.0.0.1:33013", "idfa device Memcached address")
	flag.StringVar(&cfg.gaid, "gaid", "127.0.0.1:33014", "gaid device Memcached address")
	flag.StringVar(&cfg.adid, "adid", "127.0.0.1:33015", "adid device Memcached address")
	flag.StringVar(&cfg.dvid, "dvid", "127.0.0.1:33016", "dvid device Memcached address")
	flag.IntVar(&cfg.timeout, "timeout", 1, "Memcached socket read/write timeout in sec")
	flag.IntVar(&cfg.retries, "retries", 5, "Memcached retries in sec")
	flag.IntVar(&cfg.retryDelay, "retry_delay", 1, "Memcached retry delay in sec")
	flag.BoolVar(&cfg.test, "test", false, "if set - don't dot rename files")

	flag.Parse()

	return cfg
}

func getStat(stat chan int) int {
	sum := 0
	for s := range stat {
		sum += s
	}
	return sum
}

func dotRename(path string) {
	head, fn := filepath.Split(path)
	if err := os.Rename(path, filepath.Join(head, "."+fn)); err != nil {
		log.Printf("%s", err)
	}
}

func finalizeProcess(path string, errors int, processed int, isTest bool) {
	if !isTest {
		dotRename(path)
	}
	log.Printf("errors = %d processed = %d", errors, processed)
	if processed == 0 {
		return
	}
	errRate := float64(errors) / float64(processed)
	if errRate < constNormalErrRate {
		log.Printf("Acceptable error rate (%f). Successfull load", errRate)
	} else {
		log.Printf("High error rate (%f > %f). Failed load", errRate, constNormalErrRate)
	}
}

func processFile(filepath string, mc map[string]*memcacheInfo,
	workers uint, loaders uint, retries int, retryDelay int) (int, int) {

	var workersWg sync.WaitGroup
	var loadersWg sync.WaitGroup

	errorsStat := make(chan int, loaders*uint(len(mc))+workers)
	processedStat := make(chan int, loaders*uint(len(mc)))

	job := make(chan string, constQueueSize)
	for _, v := range mc {
		v.queue = make(chan *memcache.Item, constQueueSize)
	}

	f, err := os.Open(filepath)
	defer f.Close()
	if err != nil {
		log.Fatal(err)
	}
	gr, err := gzip.NewReader(f)
	if err != nil {
		log.Fatal(err)
	}
	defer gr.Close()

	scanner := bufio.NewScanner(gr)

	for i := uint(0); i < workers; i++ {
		workersWg.Add(1)
		go func() {
			defer workersWg.Done()
			worker(job, mc, errorsStat)
		}()
	}

	for _, v := range mc {
		for i := uint(0); i < loaders; i++ {
			loadersWg.Add(1)
			go func(v *memcacheInfo) {
				defer loadersWg.Done()
				loader(v.queue, v.client, errorsStat, processedStat,
					retries, retryDelay)
			}(v)
		}
	}

	for scanner.Scan() {
		job <- scanner.Text()
	}
	close(job)
	workersWg.Wait()

	for _, v := range mc {
		close(v.queue)
	}
	loadersWg.Wait()

	close(errorsStat)
	close(processedStat)

	return getStat(errorsStat), getStat(processedStat)
}

func main() {
	cfg := getConfig()

	memcacheAddr := map[string]string{"idfa": cfg.idfa,
		"gaid": cfg.gaid,
		"adid": cfg.adid,
		"dvid": cfg.dvid}

	memcacheCQ := make(map[string]*memcacheInfo)
	for k, v := range memcacheAddr {
		memcacheCQ[k] = &memcacheInfo{memcache.New(v), nil}
		memcacheCQ[k].client.MaxIdleConns = int(cfg.loaders)
		memcacheCQ[k].client.Timeout = time.Duration(cfg.timeout) * time.Second
	}

	files, _ := filepath.Glob(cfg.pattern)
	for _, filepath := range files {
		log.Printf("Processing %s", filepath)
		errors, processed := processFile(filepath, memcacheCQ, cfg.workers,
			cfg.loaders, cfg.retries, cfg.retryDelay)
		finalizeProcess(filepath, errors, processed, cfg.test)
	}
}
