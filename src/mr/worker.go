package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
)

// WorkerArgs is the argument of the RPC call
type WorkerArgs struct {
	workerId    int         // worker id
	requestType RequestType // request type

	output   []string // map or reduce output file names
	input    []string // map or reduce input file names
	taskId   int      // map or reduce task id
	taskType TaskType // map or reduce task type
}

func (args *WorkerArgs) GetWorkerId() int {
	return args.workerId
}

func (args *WorkerArgs) GetRequestType() RequestType {
	return args.requestType
}

func (args *WorkerArgs) GetOutput() []string {
	return args.output
}

func (args *WorkerArgs) GetInput() []string {
	return args.input
}

func (args *WorkerArgs) GetTaskId() int {
	return args.taskId
}

func (args *WorkerArgs) GetTaskType() TaskType {
	return args.taskType
}

// WorkerReply is the reply of the RPC call
type WorkerReply struct {
	workerId  int32
	taskType  TaskType
	nReduce   int // number of reduce tasks
	taskId    int32
	input     []string
	reduceNum int
	exitMsg   bool
}

func (reply *WorkerReply) GetWorkerId() int32 {
	return reply.workerId
}

func (reply *WorkerReply) GetTaskType() TaskType {
	return reply.taskType
}

func (reply *WorkerReply) GetNReduce() int {
	return reply.nReduce
}

func (reply *WorkerReply) GetTaskId() int32 {
	return reply.taskId
}

func (reply *WorkerReply) GetInput() []string {
	return reply.input
}

func (reply *WorkerReply) GetReduceNum() int {
	return reply.reduceNum
}

func (reply *WorkerReply) GetExitMsg() bool {
	return reply.exitMsg
}

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}
	fmt.Println(err)
	return false
}
