package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jinzhu/copier"
)

type Coordinator struct {
	nReduce  int // number of reduce tasks
	inputNum int // number of input files

	timeout time.Duration // timeout for a worker

	nextWorkerId  atomic.Int64
	nextTaskId    atomic.Int64
	reduceTaskNum atomic.Int64
	workerNum     atomic.Int64
	waitingNum    atomic.Int64 // pending worker number

	taskChan        chan *Task        // task channel
	TaskRequestChan chan *TaskRequest // task message channel

	mapResult    *sync.Map // map result file names
	workingTasks *sync.Map // working tasks
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) HandleTask(request *TaskRequest, reply *WorkerReply) (err error) {
	switch request.MsgType {
	case Create: // create task
		log.Println("create task")
		return c.createTask(request)
	case Finish: // finish task
		log.Println("finish task")
		return c.finishTask(request)
	case Update:
		log.Println("update task")
		return c.updateTask(request)
	case Fail:
		log.Println("fail task")
		return c.failTask(request)
	default:
		return
	}
}

// create a task
func (c *Coordinator) createTask(request *TaskRequest) (err error) {
	taskStatus := new(TaskStatus)
	if err = copier.Copy(taskStatus, request); err != nil {
		log.Printf("copy task status error: %v\n", err)
		return err
	}
	c.workingTasks.Store(taskStatus.TaskId, taskStatus)
	return
}

// finish a task
func (c *Coordinator) finishTask(request *TaskRequest) (err error) {
	if _, ok := c.workingTasks.Load(request.TaskId); ok { // task exists, delete it
		if request.TaskType == Map { // map task
			for _, output := range request.Output { // store map result
				c.mapResult.Store(output, struct{}{})
			}
			if MapLen(c.mapResult) == c.inputNum*c.nReduce { // all map tasks are finished
				reduceTasks := make([][]string, c.nReduce) // reduce tasks

				c.mapResult.Range(func(result, _ any) bool { // get map result
					var taskId, workerId, idx int
					OutputFilename := result.(string)
					fmt.Sscanf(OutputFilename, "mr-int-%d-%d-%d", &taskId, &workerId, &idx)
					reduceTasks[idx] = append(reduceTasks[idx], OutputFilename)
					return true
				})

				for i := 0; i < c.nReduce; i++ {
					task := &Task{
						Input:     reduceTasks[i],
						IsCreate:  true,
						TaskType:  Reduce,
						Id:        c.nextTaskId.Load(),
						ReduceNum: i,
					}
					c.taskChan <- task
				}
			} else {
				c.reduceTaskNum.Add(-1)
			}
			c.mapResult.Delete(request.TaskId)
		}
	}
	if c.reduceTaskNum.Load() == 0 { // all reduce tasks are finished
		task := &Task{IsExit: true}
		for {
			c.taskChan <- task
		}
	}
	return
}

// update a task
func (c *Coordinator) updateTask(request *TaskRequest) (err error) {
	if _, ok := c.workingTasks.Load(request.TaskId); ok { // task exists
		taskStatus := new(TaskStatus)
		if err = copier.Copy(taskStatus, request); err != nil {
			log.Println("copy task status error: ", err)
			return err
		}
		c.workingTasks.Swap(request.TaskId, taskStatus)
	}
	return
}

// fail a task
func (c *Coordinator) failTask(request *TaskRequest) (err error) {
	if _, ok := c.workingTasks.Load(request.TaskId); ok { // task exists
		task := new(Task)
		copier.Copy(task, request)
		v, _ := c.workingTasks.Load(request.TaskId)
		taskStatus := v.(*TaskStatus)
		task.ReduceNum = taskStatus.ReduceNum
		c.taskChan <- task
	}
	return
}

func (c *Coordinator) HandleWorker(request *WorkerRequest, reply *WorkerReply) (err error) {
	// pending worker num + 1
	c.waitingNum.Add(1)
	defer c.waitingNum.Add(-1)
	switch request.RequestType {
	case Initial: // initial worker
		log.Println("initial worker")
		reply.WorkerId = c.nextWorkerId.Load()
		request.WorkerId = reply.WorkerId
		c.workerNum.Add(1)
	case Finished: // worker finished
		log.Println("worker finished")
		taskRequest := new(TaskRequest)
		copier.Copy(taskRequest, request)
		taskRequest.MsgType = Finish
		taskRequest.TimeStamp = time.Now()
		c.TaskRequestChan <- taskRequest
	case Failed: // worker failed
		log.Println("worker failed")
		taskRequest := new(TaskRequest)
		copier.Copy(taskRequest, request)
		taskRequest.MsgType = Fail
		taskRequest.TimeStamp = time.Now()
	}
	task := <-c.taskChan // get a task
	if task.IsExit {
		reply.ExitMsg = true
		c.workerNum.Add(-1)
		return nil
	}
	for task.ExcludeWorkerId == request.WorkerId {
		c.taskChan <- task
		time.Sleep(time.Millisecond)
		task = <-c.taskChan
	}
	taskRequest := new(TaskRequest)
	if task.IsCreate {
		taskRequest.MsgType = Create
	} else {
		taskRequest.MsgType = Update
	}
	copier.Copy(taskRequest, task)
	copier.Copy(taskRequest, request)
	taskRequest.TimeStamp = time.Now()
	c.TaskRequestChan <- taskRequest
	copier.Copy(reply, taskRequest)
	return
}

func (c *Coordinator) CheckTimeout() {
	for {
		c.workingTasks.Range(func(key, value any) bool {
			taskStatus := value.(*TaskStatus)
			if time.Since(taskStatus.StartTime) > c.timeout {
				task := new(Task)
				copier.Copy(task, taskStatus)
				c.taskChan <- task
			}
			return true
		})
		time.Sleep(c.timeout)
	}

}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	// l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.

	c.server()
	return &c
}
