# clickhouse_sinker

clickhouse_sinker is a sinker program that consumes kafka message and import them to [ClickHouse](https://clickhouse.yandex/).

## Features

* Easy to use and deploy, you don't need write any hard code, just care about the configuration file
* Custom parser support.
* Support multiple sinker tasks, each runs on parallel.
* Support multiply kafka and ClickHouse clusters.
* Bulk insert (by config `bufferSize` and `flushInterval`).
* Loop write (when some node crashes, it will retry write the data to the other healthy node)
* Uses Native ClickHouse client-server TCP protocol, with higher performance than HTTP.

## Install && Run

### By binary files (suggested)

Download the binary files from [release](https://github.com/housepower/clickhouse_sinker/releases), choose the executable binary file according to your env, modify the `conf` files, then run ` ./clickhouse_sinker -conf conf  `

### By source

* Install Golang

* Go Get

```
go get -u github.com/housepower/clickhouse_sinker/...

cd $GOPATH/src/github.com/housepower/clickhouse_sinker

go get -u github.com/kardianos/govendor

# may take a while
govendor sync
```

* Build && Run
```
go build -o clickhouse_sinker bin/main.go

## modify the config files, set the configuration directory, then run it
./clickhouse_sinker -conf conf
```

## Examples

* there is a simple [tutorial in Chinese](https://note.youdao.com/ynoteshare1/index.html?id=c4b4a84a08e2312da6c6d733a5074c7a&type=note) which created by user @taiyang.

## Support parsers

* [x] Json
* [x] Csv

## Supported data types

* [x] UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64
* [x] Float32, Float64
* [x] String
* [x] FixedString
* [x] DateTime(UInt32), Date(UInt16)
* [x] Array(UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64)
* [x] Array(Float32, Float64)
* [x] Array(String)
* [x] Array(FixedString)
* [x] Array(DateTime(UInt32), Date(UInt16))
* [x] Nullable
* [x] [ElasticDate](https://www.elastic.co/guide/en/elasticsearch/reference/current/date.html) => Int64 (2019-12-16T12:10:30Z => 1576498230)

## Configuration

See config [example](./conf/config.json)

## Custom metric parser

* You just need to implement the parser interface on your own

```
type Parser interface {
	Parse(bs []byte) model.Metric
}
```
See [json parser](./parser/json.go)

# Debuggging 

```bash
echo '{"date": "2019-07-11T12:10:30Z", "level": "info", "message": "msg4"}' | kafkacat -b 127.0.0.1:9093 -P -t logstash

echo 'select * from default.logstash;' | clickhouse-client --port 9900
2019-12-16	info	msg4
2019-07-11	info	msg4
2015-05-11	info	msg4
2019-07-11	info	msg4

```