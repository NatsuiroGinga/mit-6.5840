package logger

import (
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	"config"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

var (
	consoleWriter *zerolog.ConsoleWriter // 控制台日志
	fileWriter    *os.File               // 文件日志
)

func Init() {
	initConsoleWriter()
	initFileWriter()
	multi := zerolog.MultiLevelWriter(consoleWriter, fileWriter)
	logger := zerolog.New(multi).With().Timestamp().Caller().Logger()
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	log.Logger = logger
	zerolog.SetGlobalLevel(zerolog.DebugLevel)
}

// initConsoleWriter 初始化控制台日志
func initConsoleWriter() {
	consoleWriter = &zerolog.ConsoleWriter{
		Out:        os.Stderr,
		TimeFormat: time.DateTime,
	}
}

// initFileWriter 初始化文件日志
func initFileWriter() {
	var err error
	// 按日期创建日志文件夹
	now := time.Now()
	// 拼接日志文件夹路径
	fileDir := path.Join(
		config.Global.Log.Dir,
		strconv.Itoa(now.Year()),
		strconv.Itoa(int(now.Month())),
		strconv.Itoa(now.Day()),
	)
	// 创建日志文件夹
	if err = os.MkdirAll(fileDir, 0666); err != nil {
		// 文件夹已存在
		if os.IsExist(err) {
			return
		}
		panic(err)
	}
	// 拼接日志文件路径
	filename := generateFilename()
	filepath := path.Join(fileDir, filename)
	fileWriter, err = os.OpenFile(filepath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0666)
	if err != nil {
		panic(err)
	}
}

func generateFilename() string {
	now := time.Now()
	sb := new(strings.Builder)
	sb.Grow(len(config.Global.Log.Filename) + 1 + len(time.DateOnly) + 1 + len(config.Global.Log.Ext))
	sb.WriteString(config.Global.Log.Filename)
	sb.WriteByte('_')
	sb.WriteString(now.Format(time.DateOnly))
	sb.WriteByte('.')
	sb.WriteString(config.Global.Log.Ext)
	return sb.String()
}
