package main

import (
	"./apps"

	"bufio"
	"compress/gzip"
	"flag"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
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
)

const (
	constQueueSize     = 1024
	constNormalErrRate = 0.01
)

func worker(job chan string, loadersJob map[string]*memcacheInfo, errorsStat chan int) {
	msg := apps.UserApps{Apps: make([]uint32, 0, 1024),
		Lat: new(float64),
		Lon: new(float64)}

	errors := 0
	for s := range job {
		msg.Apps = msg.Apps[:0]
		devType, devId, err := apps.Parse(s, &msg)
		if err != nil {
			errors++
			continue
		}
		key := devType + ":" + devId

		data, err := apps.Serialize(&msg)
		if err != nil {
			log.Printf("%s", err)
			errors++
			continue
		}
		if _, ok := loadersJob[devType]; ok {
			loadersJob[devType].queue <- &memcache.Item{Key: key, Value: data}
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
