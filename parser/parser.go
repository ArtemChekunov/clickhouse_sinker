package parser

import (
	"encoding/json"
	"github.com/housepower/clickhouse_sinker/model"
)

type Parser interface {
	Parse(bs []byte) model.Metric
}

func NewParser(typ string, title []string, delimiter string) Parser {
	switch typ {
	case "json", "gjson":
		return &GjsonParser{}
	case "fastjson":
		return &FastjsonParser{}
	case "csv":
		return &CsvParser{title: title, delimiter: delimiter}
	case "gjson_extend": //extend gjson that could extract the map
		return &GjsonExtendParser{}
	default:
		return &GjsonParser{}
	}
}

func GetJSONShortStr(v interface{}) string {
	bs, _ := json.Marshal(v)
	return string(bs)
}
