package main

//
// start a worker process, which is implemented
// in ../mr/worker.go. typically there will be
// multiple worker processes, talking to one coordinator.
//
// go run mrworker.go wc.so
//
// Please do not change this file.
//

import (
	"6.5840/mr"
	"github.com/rs/zerolog/log"
)
import "plugin"
import "os"

func main() {
	if len(os.Args) != 2 {
		log.Error().Msg("Usage: mrworker xxx.so")
		os.Exit(1)
	}

	mapf, reducef := loadPlugin(os.Args[1])

	mr.Worker(mapf, reducef)
}

// load the application Map and Reduce functions
// from a plugin file, e.g. ../mrapps/wc.so
func loadPlugin(filename string) (func(string, string) []mr.KeyValue, func(string, []string) string) {
	p, err := plugin.Open(filename)
	if err != nil {
		log.Err(err).Msg("cannot load plugin")
	}
	xmapf, err := p.Lookup("Map")
	if err != nil {
		log.Err(err).Msg("cannot find Map function in plugin")
	}
	mapf := xmapf.(func(string, string) []mr.KeyValue)
	xreducef, err := p.Lookup("Reduce")
	if err != nil {
		log.Err(err).Msg("cannot find Reduce function in plugin")
	}
	reducef := xreducef.(func(string, []string) string)

	return mapf, reducef
}
