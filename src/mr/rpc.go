package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"time"
)

// Timeout for RPC call
const Timeout = 5 * time.Second

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	// s := "/var/tmp/5840-mr-"
	// s += strconv.Itoa(os.Getuid())
	return "127.0.0.1:8888"
}
