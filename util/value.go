package util

import (
	"strings"

	"github.com/housepower/clickhouse_sinker/model"
	"github.com/kshvakov/clickhouse"
)

// There are only three cases for the value type of metric, (float64, string, map [string] interface {})
func GetValueByType(metric model.Metric, cwt *model.ColumnWithType) interface{} {
	swType := switchType(cwt.Type)
	name := strings.Replace(cwt.Name, ".", "\\.", -1)
	switch swType {
	case "int":
		return metric.GetInt(name)
	case "float":
		return metric.GetFloat(name)
	case "string":
		return metric.GetString(name)
	case "stringArray":
		return clickhouse.Array(metric.GetArray(name, "string"))
	case "intArray":
		return clickhouse.Array(metric.GetArray(name, "int"))
	case "floatArray":
		return clickhouse.Array(metric.GetArray(name, "float"))
	case "ElasticDate":
		return metric.GetElasticDate(name)

	//never happen
	default:
		return ""
	}
}

func switchType(typ string) string {
	switch typ {
	case "Date", "DateTime", "UInt8", "UInt16", "UInt32", "UInt64", "Int8",
		"Int16", "Int32", "Int64", "Nullable(Date)", "Nullable(DateTime)",
		"Nullable(UInt8)", "Nullable(UInt16)", "Nullable(UInt32)", "Nullable(UInt64)",
		"Nullable(Int8)", "Nullable(Int16)", "Nullable(Int32)", "Nullable(Int64)":
		return "int"
	case "Array(Date)", "Array(DateTime)", "Array(UInt8)", "Array(UInt16)", "Array(UInt32)", "Array(UInt64)", "Array(Int8)", "Array(Int16)", "Array(Int32)", "Array(Int64)":
		return "intArray"
	case "String", "FixString", "Nullable(String)":
		return "string"
	case "Array(String)", "Array(FixString)":
		return "stringArray"
	case "Float32", "Float64", "Nullable(Float32)", "Nullable(Float64)":
		return "float"
	case "Array(Float32)", "Array(Float64)":
		return "floatArray"
	case "ElasticDate":
		return "ElasticDate"
	default:
		panic("unsupport type " + typ)
	}
}
