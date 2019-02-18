package main

import (
	"./apps"
	"bufio"
	"compress/gzip"
	"flag"
	"github.com/bradfitz/gomemcache/memcache"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"
)

type config struct {
	pattern        string
	workers        uint
	loaders_per_mc uint
	idfa           string
	gaid           string
	adid           string
	dvid           string
	timeout        int
	retries        int
	retry_delay    int
}

type mc_info struct {
	client *memcache.Client
	queue  chan *memcache.Item
}

var DEFAUL_CONFIG config = config{pattern: "src/data/tmp_uniq_key.gz",
	workers:        uint(runtime.NumCPU()),
	loaders_per_mc: 3,
	idfa:           "127.0.0.1:33013",
	gaid:           "127.0.0.1:33014",
	adid:           "127.0.0.1:33015",
	dvid:           "127.0.0.1:33016",
	timeout:        0,
	retries:        5,
	retry_delay:    1}

const QUEUE_SIZE = 1024

func worker(job chan *string, mc_jobs map[string]*mc_info, e_stat *int) {
	msg := apps.UserApps{Apps: make([]uint32, 0, 1024),
		Lat: new(float64),
		Lon: new(float64)}
	e_count := 0
	for {
		s := <- job
		if s == nil {
			*e_stat = e_count
			return
		}
		msg.Apps = msg.Apps[:0]
		dev_type, dev_id, err := apps.Parse(s, &msg)
		if err != nil {
			e_count++
			continue
		}
		key := dev_type + ":" + dev_id

		data, err := apps.Serialize(&msg)
		if err != nil {
			log.Printf("%s", err)
			e_count++
			continue
		}
		if _, ok := mc_jobs[dev_type]; ok {
			mc_jobs[dev_type].queue <- &memcache.Item{Key: key, Value: data}
		} else {
			e_count++
		}
	}
}

func loader(job chan *memcache.Item, client *memcache.Client,
	e_stat *int, p_stat *int, retries int, retry_delay int) {
	e_cnt := 0
	p_cnt := 0
	for {
		d := <- job
		if d == nil {
			*e_stat = e_cnt
			*p_stat = p_cnt
			return
		}
		attempts := retries
		var err error
		for {
			err = client.Set(d)
			if err == nil {
				break
			}
			attempts -= 1
			if attempts <= 0 {
				break
			}
			time.Sleep(time.Duration(retry_delay) * time.Second)
		}
		if err != nil {
			log.Printf("%s", err)
			e_cnt++
		} else {
			p_cnt++
		}
	}
}

func get_config() config {
	pattern := flag.String("pattern", DEFAUL_CONFIG.pattern, "")
	workers := flag.Uint("workers", DEFAUL_CONFIG.workers, "")
	loaders_per_mc := flag.Uint("loaders", DEFAUL_CONFIG.loaders_per_mc, "")
	idfa := flag.String("idfa", DEFAUL_CONFIG.idfa, "")
	gaid := flag.String("gaid", DEFAUL_CONFIG.gaid, "")
	adid := flag.String("adid", DEFAUL_CONFIG.adid, "")
	dvid := flag.String("dvid", DEFAUL_CONFIG.dvid, "")
	timeout := flag.Int("timeout", DEFAUL_CONFIG.timeout, "")
	retries := flag.Int("retries", DEFAUL_CONFIG.retries, "")
	retry_delay := flag.Int("retry_delay", DEFAUL_CONFIG.retry_delay, "")

	flag.Parse()

	cfg := config{pattern: *pattern,
		workers:        *workers,
		loaders_per_mc: *loaders_per_mc,
		idfa:           *idfa, gaid: *gaid,
		adid: *adid, dvid: *dvid,
		timeout: *timeout,
		retries: *retries, retry_delay: *retry_delay}
	return cfg
}

func sum_slice(sl []int) int {
	sum := 0
	for _, v := range sl {
		sum += v
	}
	return sum
}

func process_file(fn string, mc map[string]*mc_info,
	job chan *string, W uint, L uint,
	retries int, retry_delay int) (int, int) {

	var wg sync.WaitGroup
	var mc_wg sync.WaitGroup

	f, err := os.Open(fn)
	defer f.Close()
	if err != nil {
		log.Fatal(err)
	}
	gr, err := gzip.NewReader(f)
	if err != nil {
		log.Fatal(err)
	}
	scanner := bufio.NewScanner(gr)

	w_stat_e := make([]int, W)
	for i := uint(0); i < W; i++ {
		wg.Add(1)
		go func(i uint) {
			defer wg.Done()
			worker(job, mc, &w_stat_e[i])
		}(i)
	}

	l_stat_e := make([]int, len(mc)*int(L))
	l_stat_p := make([]int, len(mc)*int(L))
	idx := 0
	for _, v := range mc {
		for i := uint(0); i < L; i++ {
			mc_wg.Add(1)
			go func(v *mc_info, idx int) {
				defer mc_wg.Done()
				loader(v.queue, v.client, &l_stat_e[idx], &l_stat_p[idx],
					retries, retry_delay)
			}(v, idx)
			idx++
		}
	}

	for scanner.Scan() {
		s := scanner.Text()
		job <- &s
	}

	for i := uint(0); i < W; i++ {
		job <- nil
	}
	wg.Wait()

	for _, v := range mc {
		for i := uint(0); i < L; i++ {
			v.queue <- nil
		}
	}
	mc_wg.Wait()

	errors := sum_slice(l_stat_e)
	processed := sum_slice(l_stat_p)

	errors += sum_slice(w_stat_e)

	return errors, processed
}

func main() {
	cfg := get_config()

	mc_addr := map[string]string{"idfa": cfg.idfa,
		"gaid": cfg.gaid,
		"adid": cfg.adid,
		"dvid": cfg.dvid}

	mc_clients := make(map[string]*mc_info)
	for k, v := range mc_addr {
		mc_clients[k] = &mc_info{memcache.New(v), make(chan *memcache.Item, QUEUE_SIZE)}
		mc_clients[k].client.MaxIdleConns = int(cfg.loaders_per_mc)
		mc_clients[k].client.Timeout = time.Duration(cfg.timeout) * time.Second
	}

	job_ch := make(chan *string, QUEUE_SIZE)

	ff, _ := filepath.Glob(cfg.pattern)
	for _, f := range ff {
		log.Printf("Processing %s", f)
		errors, processed := process_file(f, mc_clients, job_ch, cfg.workers, cfg.loaders_per_mc,
			cfg.retries, cfg.retry_delay)
		log.Printf("errors = %d processed = %d", errors, processed)
	}
}
