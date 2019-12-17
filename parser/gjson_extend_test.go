package parser

import (
	"testing"
)

func TestGjsonExtendInt(t *testing.T) {
	parser := NewParser("gjson_extend", nil, "")
	metric := parser.Parse(jsonSample)

	var expected int64 = 1536813227
	result := metric.GetInt("its")
	assertEqual(t, result, expected)
}

func TestGjsonExtendArrayInt(t *testing.T) {
	parser := NewParser("gjson_extend", nil, "")
	metric := parser.Parse(jsonSample)

	arr := metric.GetArray("mp_a", "int").([]int64)
	expected := []int64{1, 2, 3}
	for i := range arr {
		assertEqual(t, arr[i], expected[i])
	}

}

func TestGjsonExtendStr(t *testing.T) {
	parser := NewParser("gjson_extend", nil, "")
	metric := parser.Parse(jsonSample)

	var expected string = "ws"
	result := metric.GetString("channel")
	assertEqual(t, result, expected)
}

func TestGjsonExtendArrayString(t *testing.T) {
	parser := NewParser("gjson_extend", nil, "")
	metric := parser.Parse(jsonSample)

	arr := metric.GetArray("mps_a", "string").([]string)
	expected := []string{"aa", "bb", "cc"}
	for i := range arr {
		assertEqual(t, arr[i], expected[i])
	}

}

func TestGjsonExtendFloat(t *testing.T) {
	parser := NewParser("gjson_extend", nil, "")
	metric := parser.Parse(jsonSample)

	var expected float64 = 0.11
	result := metric.GetFloat("percent")
	assertEqual(t, result, expected)
}

func TestGjsonExtendArrayFloat(t *testing.T) {
	parser := NewParser("gjson_extend", nil, "")
	metric := parser.Parse(jsonSample)

	arr := metric.GetArray("mp_f", "float").([]float64)
	expected := []float64{1.11, 2.22, 3.33}
	for i := range arr {
		assertEqual(t, arr[i], expected[i])
	}

}

func TestGjsonExtendElasticDate(t *testing.T) {
	parser := NewParser("gjson_extend", nil, "")
	metric := parser.Parse(jsonSample)

	// {"date": "2019-12-16T12:10:30Z"}
	// TZ=UTC date -d @1576498230 => Mon 16 Dec 2019 12:10:30 PM UTC
	var expected int64 = 1576498230
	result := metric.GetElasticDate("date")
	assertEqual(t, result, expected)
}
