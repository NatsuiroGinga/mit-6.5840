package config

var defaultLogConfig = &log{
	Dir:      "logs",
	Filename: "mit-6.5840",
	Ext:      "log",
}

type log struct {
	Dir      string `mapstructure:"dir" json:"dir" yaml:"dir"`
	Filename string `mapstructure:"filename" json:"filename" yaml:"filename"`
	Ext      string `mapstructure:"ext" json:"ext" yaml:"ext"`
}
