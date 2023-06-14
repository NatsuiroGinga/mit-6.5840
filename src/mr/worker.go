package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"time"
)

type MapFunc func(filename string, contents string) (kvs []KeyValue)

type ReduceFunc func(key string, values []string) (wordNum string)

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

	for {
		task := getTask()

		switch task.State {
		case PhaseMap:
			doMap(task, mapf)
		case PhaseReduce:
			doReduce(task, reducef)
		case PhaseWait:
			time.Sleep(Timeout)
		case PhaseExit:
			log.Println("Exit")
			return
		}
	}
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// doReduce does the reduce task
func doReduce(task *Task, reducef ReduceFunc) {
	intermediates := *readFromLocalFile(task.Intermediates)
	sort.Sort(ByKey(intermediates))
	dir, _ := os.Getwd()
	tempFile, err := os.CreateTemp(dir, "mr-tmp-*")
	if err != nil {
		log.Fatal(err)
	}

	i := 0
	for i < len(intermediates) {
		j := i + 1
		for j < len(intermediates) && intermediates[j].Key == intermediates[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, intermediates[k].Value)
		}
		output := reducef(intermediates[i].Key, values)

		// this is the correct format for each line of Reduce output.
		if _, err = fmt.Fprintf(tempFile, "%v %v\n", intermediates[i].Key, output); err != nil {
			log.Fatalf("cannot write %v", tempFile.Name())
			return
		}

		i = j
	}
	if err = tempFile.Close(); err != nil {
		log.Fatalf("cannot close %s", tempFile.Name())
		return
	}
	output := fmt.Sprintf("mr-out-%d", task.TaskId)
	if err = os.Rename(tempFile.Name(), output); err != nil {
		log.Fatalf("cannot rename %s to %s", tempFile.Name(), output)
	}
	task.Output = output
	log.Printf("Task %d completed, output file %s generated", task.TaskId, output)
	TaskCompleted(task)
}

// doMap does the map task
func doMap(task *Task, mapf MapFunc) {
	content, err := os.ReadFile(task.Input)
	if err != nil {
		log.Fatalf("cannot read %v", task.Input)
	}
	intermediates := mapf(task.Input, Bytes2Str(content))
	// write kvs to intermediate files
	buffer := make([][]KeyValue, task.NReduce)
	for _, kv := range intermediates {
		idx := ihash(kv.Key) % task.NReduce
		buffer[idx] = append(buffer[idx], kv)
	}
	// write to intermediate files
	var outputs []string
	for i := 0; i < task.NReduce; i++ {
		outputs = append(outputs, writeToLocalFile(task.TaskId, i, &buffer[i]))
	}
	// update task
	task.Intermediates = outputs
	log.Printf("Task %d completed, %d intermediate files generated", task.TaskId, len(outputs))
	TaskCompleted(task)
}

// TaskCompleted notifies the coordinator that the task is completed
func TaskCompleted(task *Task) {
	reply := new(ExampleReply)
	call("Coordinator.TaskCompleted", task, reply)
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
	c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	// sockname := coordinatorSock()
	// c, err := rpc.Dial("tcp", sockname)
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

// getTask gets a task from the coordinator
func getTask() (task *Task) {
	args := new(ExampleArgs)
	task = new(Task)
	call("Coordinator.AssignTask", args, task)
	return
}

// writeToLocalFile writes kvs to local file
func writeToLocalFile(mapperId, reducerId int, kvs *[]KeyValue) string {
	dir, _ := os.Getwd()
	tempFile, err := os.CreateTemp(dir, "mr-tmp-*")
	if err != nil {
		log.Fatal(err)
	}
	defer tempFile.Close()
	encoder := json.NewEncoder(tempFile)
	for _, kv := range *kvs {
		if err := encoder.Encode(&kv); err != nil {
			log.Fatal(err)
		}
	}
	output := fmt.Sprintf("mr-%d-%d", mapperId, reducerId)
	os.Rename(tempFile.Name(), output)
	return filepath.Join(dir, output)
}

func readFromLocalFile(files []string) *[]KeyValue {
	var kvs []KeyValue
	for _, filename := range files {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatal(err)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err = dec.Decode(&kv); err != nil {
				break
			}
			kvs = append(kvs, kv)
		}
	}
	return &kvs
}
