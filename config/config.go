package config

import (
	"encoding/json"
	"os"
)

const filePath = "config.json"

var Global *global

type global struct {
	Log *log `mapstructure:"log" json:"log" yaml:"log"`
}

func init() {
	Global = &global{
		Log: defaultLogConfig,
	}
	if _, err := os.Stat(filePath); err != nil {
		if os.IsNotExist(err) {
			return
		}
		panic(err)
	}
	readFile()
}

func readFile() {
	// 读取配置文件
	file, err := os.Open(filePath)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	// 解析配置文件
	decoder := json.NewDecoder(file)
	err = decoder.Decode(Global)
	if err != nil {
		panic(err)
	}
}
