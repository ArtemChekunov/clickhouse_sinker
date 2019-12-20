package main

import (
	"flag"
	"github.com/housepower/clickhouse_sinker/health"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"net/http"
	"os"
	"runtime/pprof"

	"github.com/housepower/clickhouse_sinker/creator"
	"github.com/housepower/clickhouse_sinker/prom"
	"github.com/housepower/clickhouse_sinker/task"
	_ "github.com/kshvakov/clickhouse"

	"github.com/wswz/go_commons/app"

	"github.com/wswz/go_commons/log"
)

var (
	config     = flag.String("conf", "", "config dir")
	cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")
	http_addr  = flag.String("http-addr", "0.0.0.0:2112", "http interface")

	httpMetrcs = promhttp.Handler()
)

func init() {
	flag.Parse()

	prometheus.MustRegister(prometheus.NewBuildInfoCollector())
	prometheus.MustRegister(prom.ClickhouseReconnectTotal)
	prometheus.MustRegister(prom.ClickhouseEventsSuccess)
	prometheus.MustRegister(prom.ClickhouseEventsErrors)
	prometheus.MustRegister(prom.ClickhouseEventsTotal)
	prometheus.MustRegister(prom.KafkaConsumerErrors)

}

func main() {

	var cfg creator.Config
	var runner *Sinker

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Error(err)
			os.Exit(1)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}
	app.Run("clickhouse_sinker", func() error {
		cfg = *creator.InitConfig(*config)
		runner = NewSinker(cfg)
		return runner.Init()
	}, func() error {
		go func() {
			mux := http.NewServeMux()
			mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
				w.Write([]byte(`<html><head><title>ClickHouse Sinker</title></head>
				<body>
					<p><a href="/metrics">Metrics</a></p>
					<p><a href="/ready">Ready</a></p>
					<p><a href="/ready?full=1">Ready Full</a></p>
					<p><a href="/live">Live</a></p>
					<p><a href="/live?full=1">Live Full</a></p>
				</body></html>`))
			})

			mux.Handle("/metrics", httpMetrcs)
			mux.HandleFunc("/ready", health.Health.ReadyEndpoint) // GET /ready?full=1
			mux.HandleFunc("/live", health.Health.LiveEndpoint)   // GET /live?full=1

			log.Info("Run http server", *http_addr)
			log.Error(http.ListenAndServe(*http_addr, mux))
		}()

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
