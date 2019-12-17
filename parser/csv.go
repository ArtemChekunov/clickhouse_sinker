package parser

import (
	"strconv"
	"strings"
	"time"

	"github.com/housepower/clickhouse_sinker/model"
)

type CsvParser struct {
	title     []string
	delimiter string
}

func (c *CsvParser) Parse(bs []byte) model.Metric {
	msgData := string(bs)
	msgs := strings.Split(msgData, c.delimiter)
	v := make(map[string]string)
	msLen := len(msgs)
	for i, key := range c.title {
		if i >= msLen {
			continue
		}
		v[key] = msgs[i]
	}
	return &CsvMetric{v}
}

type CsvMetric struct {
	mp map[string]string
}

func (c *CsvMetric) Get(key string) interface{} {
	return c.mp[key]
}

func (c *CsvMetric) GetString(key string) string {
	v, _ := c.mp[key]
	return v
}

func (c *CsvMetric) GetFloat(key string) float64 {
	v, _ := c.mp[key]
	n, _ := strconv.ParseFloat(v, 64)
	return n
}

func (c *CsvMetric) GetInt(key string) int64 {
	v, _ := c.mp[key]
	n, _ := strconv.ParseInt(v, 10, 64)
	return n
}

// GetArray is Empty implemented for CsvMetric
func (c *CsvMetric) GetArray(key string, t string) interface{} {
	return []interface{}{}
}

func (c *CsvMetric) GetElasticDate(key string) int64 {
	val := c.GetString(key)
	t, _ := time.Parse(time.RFC3339, val)

	return t.Unix()
}
