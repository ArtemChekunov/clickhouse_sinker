/*Copyright [2019] housepower

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package parser

import (
	"encoding/json"
	"time"

	"github.com/housepower/clickhouse_sinker/model"
)

var (
	DefaultTSLayout = []string{"2006-01-02", time.RFC3339Nano, time.RFC3339Nano}
	Epoch           = time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
)

// Parse is the Parser interface
type Parser interface {
	Parse(bs []byte) model.Metric
}

// NewParser is a factory method to generate new parse
func NewParser(typ string, title []string, delimiter string, tsLayout []string) Parser {
	switch typ {
	case "json", "gjson":
		return &GjsonParser{tsLayout: tsLayout}
	case "fastjson":
		return &FastjsonParser{tsLayout: tsLayout}
	case "csv":
		return &CsvParser{title: title, delimiter: delimiter, tsLayout: tsLayout}
	case "gjson_extend": //extend gjson that could extract the map
		return &GjsonExtendParser{tsLayout: tsLayout}
	default:
		return &GjsonParser{tsLayout: tsLayout}
	}
}

func GetJSONShortStr(v interface{}) string {
	bs, _ := json.Marshal(v)
	return string(bs)
}
