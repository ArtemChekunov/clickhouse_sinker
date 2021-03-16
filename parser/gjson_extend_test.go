package parser

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestGjsonExtendInt(t *testing.T) {
	parser := NewParser("gjson_extend", nil, "", []string{DefaultTSLayout[0], "2006-01-02 15:04:05", time.RFC3339})
	metric := parser.Parse(jsonSample)

	var expected int64 = 1536813227
	result := metric.GetInt("its", false).(int64)
	assert.Equal(t, result, expected)
}
func TestGjsonExtendIntNullableFalse(t *testing.T) {
	parser := NewParser("gjson_extend", nil, "", []string{DefaultTSLayout[0], "2006-01-02 15:04:05", time.RFC3339})
	metric := parser.Parse(jsonSample)

	var expected int64 = int64(0)
	result := metric.GetInt("its_not_exist", false).(int64)
	assert.Equal(t, expected, result)
}

func TestGjsonExtendIntNullableTrue(t *testing.T) {
	parser := NewParser("gjson_extend", nil, "", []string{DefaultTSLayout[0], "2006-01-02 15:04:05", time.RFC3339})
	metric := parser.Parse(jsonSample)

	result := metric.GetInt("its_not_exist", true)
	assert.Nil(t, result, "err should be nothing")
}

func TestGjsonExtendArrayInt(t *testing.T) {
	parser := NewParser("gjson_extend", nil, "", []string{DefaultTSLayout[0], "2006-01-02 15:04:05", time.RFC3339})
	metric := parser.Parse(jsonSample)

	arr := metric.GetArray("mp_a", "int").([]int64)
	expected := []int64{1, 2, 3}
	for i := range arr {
		assert.Equal(t, arr[i], expected[i])
	}
}

func TestGjsonExtendStr(t *testing.T) {
	parser := NewParser("gjson_extend", nil, "", []string{DefaultTSLayout[0], "2006-01-02 15:04:05", time.RFC3339})
	metric := parser.Parse(jsonSample)

	var expected string = "ws"
	result := metric.GetString("channel", false).(string)
	assert.Equal(t, result, expected)
}
func TestGjsonExtendStrNullableFalse(t *testing.T) {
	parser := NewParser("gjson_extend", nil, "", []string{DefaultTSLayout[0], "2006-01-02 15:04:05", time.RFC3339})
	metric := parser.Parse(jsonSample)

	var expected string = ""
	result := metric.GetString("channel_not_exist", false).(string)
	assert.Equal(t, result, expected)
}
func TestGjsonExtendStrNullableTrue(t *testing.T) {
	parser := NewParser("gjson_extend", nil, "", []string{DefaultTSLayout[0], "2006-01-02 15:04:05", time.RFC3339})
	metric := parser.Parse(jsonSample)

	result := metric.GetString("channel_not_exist", true)
	assert.Nil(t, result, "err should be nothing")
}

func TestGjsonExtendArrayString(t *testing.T) {
	parser := NewParser("gjson_extend", nil, "", []string{DefaultTSLayout[0], "2006-01-02 15:04:05", time.RFC3339})
	metric := parser.Parse(jsonSample)

	arr := metric.GetArray("mps_a", "string").([]string)
	expected := []string{"aa", "bb", "cc"}
	for i := range arr {
		assert.Equal(t, arr[i], expected[i])
	}
}

func TestGjsonExtendFloat(t *testing.T) {
	parser := NewParser("gjson_extend", nil, "", []string{DefaultTSLayout[0], "2006-01-02 15:04:05", time.RFC3339})
	metric := parser.Parse(jsonSample)

	var expected float64 = 0.11
	result := metric.GetFloat("percent", false).(float64)
	assert.Equal(t, result, expected)
}

func TestGjsonExtendFloatNullableFalse(t *testing.T) {
	parser := NewParser("gjson_extend", nil, "", []string{DefaultTSLayout[0], "2006-01-02 15:04:05", time.RFC3339})
	metric := parser.Parse(jsonSample)

	var expected int = 0
	result := metric.GetFloat("percent_not_exist", false).(int)
	assert.Equal(t, result, expected)
}
func TestGjsonExtendFloatNullableTrue(t *testing.T) {
	parser := NewParser("gjson_extend", nil, "", []string{DefaultTSLayout[0], "2006-01-02 15:04:05", time.RFC3339})
	metric := parser.Parse(jsonSample)

	result := metric.GetFloat("percent_not_exist", true)
	assert.Nil(t, result, "err should be nothing")
}

func TestGjsonExtendArrayFloat(t *testing.T) {
	parser := NewParser("gjson_extend", nil, "", []string{DefaultTSLayout[0], "2006-01-02 15:04:05", time.RFC3339})
	metric := parser.Parse(jsonSample)

	arr := metric.GetArray("mp_f", "float").([]float64)
	expected := []float64{1.11, 2.22, 3.33}
	for i := range arr {
		assert.Equal(t, arr[i], expected[i])
	}
}

func TestGjsonExtendElasticDateTime(t *testing.T) {
	parser := NewParser("gjson_extend", nil, "", []string{DefaultTSLayout[0], "2006-01-02 15:04:05", time.RFC3339})
	metric := parser.Parse(jsonSample)

	// {"date": "2019-12-16T12:10:30Z"}
	// TZ=UTC date -d @1576498230 => Mon 16 Dec 2019 12:10:30 PM UTC
	var expected int64 = 1576498230
	result := metric.GetElasticDateTime("date", false).(int64)
	assert.Equal(t, result, expected)
}

func TestGjsonExtendElasticDateTimeNullableFalse(t *testing.T) {
	parser := NewParser("gjson_extend", nil, "", []string{DefaultTSLayout[0], "2006-01-02 15:04:05", time.RFC3339})
	metric := parser.Parse(jsonSample)

	var expected int64 = -62135596800
	result := metric.GetElasticDateTime("date_not_exist", false).(int64)
	assert.Equal(t, result, expected)
}

func TestGjsonExtendElasticDateTimeNullableTrue(t *testing.T) {
	parser := NewParser("gjson_extend", nil, "", []string{DefaultTSLayout[0], "2006-01-02 15:04:05", time.RFC3339})
	metric := parser.Parse(jsonSample)

	result := metric.GetElasticDateTime("date_not_exist", true)
	assert.Nil(t, result, "err should be nothing")
}
