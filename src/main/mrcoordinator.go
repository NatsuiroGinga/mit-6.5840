package main

//
// start the coordinator process, which is implemented
// in ../mr/coordinator.go
//
// go run mrcoordinator.go pg*.txt
//
// Please do not change this file.
//

import (
	"errors"

	"6.5840/mr"
	"github.com/rs/zerolog/log"
	"lib/logger"
)
import "time"
import "os"

func init() {
	logger.Init()
}

func main() {
	if len(os.Args) < 2 {
		// fmt.Fprintf(os.Stderr, "Usage: mrcoordinator inputfiles...\n")
		log.Err(errors.New("Usage: mrcoordinator inputfiles...")).Send()
		os.Exit(1)
	}

	m := mr.MakeCoordinator(os.Args[1:], 10)
	for m.Done() == false {
		time.Sleep(time.Second)
	}

	time.Sleep(time.Second)
}
