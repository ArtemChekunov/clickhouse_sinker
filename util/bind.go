package util

import (
	"encoding/json"
)

// 将config的配置注入到entity中
func IngestConfig(config interface{}, entity interface{}) {
	bs, _ := json.Marshal(config)
	err := json.Unmarshal(bs, entity)
	if err != nil {
		panic(err)
	}
}
