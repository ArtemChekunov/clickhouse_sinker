package parser

import (
	"github.com/tidwall/gjson"
	"github.com/wswz/go_commons/log"
	"time"

	"github.com/housepower/clickhouse_sinker/model"
)

type GjsonParser struct {
}

func (c *GjsonParser) Parse(bs []byte) model.Metric {
	return &GjsonMetric{string(bs)}
}

type GjsonMetric struct {
	raw string
}

func (c *GjsonMetric) Get(key string) interface{} {
	return gjson.Get(c.raw, key).Value()
}

func (c *GjsonMetric) GetString(key string) string {
	return gjson.Get(c.raw, key).String()
}

func (c *GjsonMetric) GetArray(key string, t string) interface{} {
	slice := gjson.Get(c.raw, key).Array()
	switch t {
	case "string":
		results := make([]string, 0, len(slice))
		for _, s := range slice {
			results = append(results, s.String())
		}
		return results

	case "float":
		results := make([]float64, 0, len(slice))

		for _, s := range slice {
			results = append(results, s.Float())
		}
		return results

	case "int":
		results := make([]int64, 0, len(slice))
		for _, s := range slice {
			results = append(results, s.Int())
		}
		return results

	default:
		panic("not supported array type " + t)
	}
	return nil
}

func (c *GjsonMetric) GetFloat(key string) float64 {
	return gjson.Get(c.raw, key).Float()
}

func (c *GjsonMetric) GetInt(key string) int64 {
	return gjson.Get(c.raw, key).Int()
}

func (c *GjsonMetric) GetElasticDate(key string) int64 {
	val := c.GetString(key)
	t, _ := time.Parse(time.RFC3339, val)

	log.Debug("KEY:", key, "VAL:", t.Unix())
	return t.Unix()
}
