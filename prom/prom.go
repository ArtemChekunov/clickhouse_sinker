package prom

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	ChEventsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "clickhouse_events_total",
		Help: "The total number of events to insert into ClickHouse",
	},
		[]string{"db", "table"})

	ChEventsSuccess = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "clickhouse_events_success",
		Help: "The number of events successfully inserted into ClickHouse",
	},
		[]string{"db", "table"})

	ChEventsErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "clickhouse_events_errors",
		Help: "The number of events didn't inserted into ClickHouse",
	},
		[]string{"db", "table"})

	ChReconnectTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "clickhouse_reconnect_total",
		Help: "The total number of ClickHouse reconnects"})
)
