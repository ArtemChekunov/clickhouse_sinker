package main

import (
	"flag"
	"net/http"
	"os"
	"runtime/pprof"

	"github.com/housepower/clickhouse_sinker/creator"
	"github.com/housepower/clickhouse_sinker/task"
	_ "github.com/kshvakov/clickhouse"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/wswz/go_commons/app"
	"github.com/wswz/go_commons/log"
)

var (
	config     = flag.String("conf", "", "config dir")
	cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")
	http_addr  = flag.String("http-addr", "0.0.0.0:2112", "http interface")
)

func init() {

	flag.Parse()
}

func main() {

	go func() {
		log.Info("Run http server", *http_addr)
		http.Handle("/metrics", promhttp.Handler())
		log.Error(http.ListenAndServe(*http_addr, nil))
	}()

	var cfg creator.Config
	var runner *Sinker

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Error(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}
	app.Run("clickhouse_sinker", func() error {
		cfg = *creator.InitConfig(*config)
		runner = NewSinker(cfg)
		return runner.Init()
	}, func() error {
		runner.Run()
		return nil
	}, func() error {
		runner.Close()
		return nil
	})
}

type Sinker struct {
	tasks   []*task.TaskService
	config  creator.Config
	stopped chan struct{}
}

func NewSinker(config creator.Config) *Sinker {
	s := &Sinker{config: config, stopped: make(chan struct{})}
	return s
}

func (s *Sinker) Init() error {
	s.tasks = s.config.GenTasks()
	for _, t := range s.tasks {
		if err := t.Init(); err != nil {
			return err
		}
	}
	return nil
}

func (s *Sinker) Run() {
	for i := range s.tasks {
		go s.tasks[i].Run()
	}
	<-s.stopped
}

func (s *Sinker) Close() {
	for i := range s.tasks {
		s.tasks[i].Stop()
	}
	close(s.stopped)
}
