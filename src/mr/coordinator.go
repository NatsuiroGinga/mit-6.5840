package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"sync"
	"sync/atomic"
	"time"
)

type Coordinator struct {
	TaskQueue chan *Task // task queue

	TaskMeta *sync.Map // task id -> task meta

	Phase *atomic.Int32 // current phase

	NReduce int // number of reduce tasks

	Inputs        []string   // input filenames
	Intermediates [][]string // intermediate filenames, [reduce id][map id]
}

type CoordinatorPhase int

const (
	PhaseMap    CoordinatorPhase = 1 + iota // Map
	PhaseReduce                             // Reduce
	PhaseExit                               // Exit
	PhaseWait
)

var phaseName = [...]string{
	1: "Map",
	2: "Reduce",
	3: "Exit",
	4: "Wait",
}

func (p CoordinatorPhase) String() string {
	if p <= 0 || int(p) >= len(phaseName) {
		return "Unknown"
	}
	return phaseName[p]
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	_ = rpc.Register(c)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":1234")
	/* sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("tcp", sockname) */
	if e != nil {
		log.Fatal("listen error:", e)
	}
	log.Printf("Coordinator server start at %s", l.Addr())
	go func() {
		_ = http.Serve(l, nil)
	}()
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	return CoordinatorPhase(c.Phase.Load()) == PhaseExit
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := &Coordinator{
		TaskQueue:     make(chan *Task, Max(nReduce, len(files))),
		TaskMeta:      new(sync.Map),
		Phase:         new(atomic.Int32),
		NReduce:       nReduce,
		Inputs:        files,
		Intermediates: make([][]string, nReduce),
	}

	// create map tasks
	c.createMapTask()
	// start server
	c.server()
	// check timeout
	go c.checkTimeout()

	return c
}

func (c *Coordinator) createMapTask() {
	for i, input := range c.Inputs {
		id := i + 1
		task := &Task{
			TaskId:  id,       // task id starts from 1, not 0
			State:   PhaseMap, // map task
			Input:   input,
			NReduce: c.NReduce, // number of reduce tasks
		}
		// add to task queue
		c.TaskQueue <- task
		// add to task meta
		c.TaskMeta.Store(id, &CoordinatorTask{
			Status:        StatusIdle,
			TaskReference: task,
		})
	}
}

// AssignTask assigns a task to a worker
func (c *Coordinator) AssignTask(_ *ExampleArgs, reply *Task) error {
	if len(c.TaskQueue) > 0 {
		// get task from queue
		*reply = *<-c.TaskQueue
		// update task meta
		v, _ := c.TaskMeta.Load(reply.TaskId)
		taskMeta := v.(*CoordinatorTask)
		taskMeta.Status = StatusInProgress
		taskMeta.StartTime = time.Now()
		// signal a worker to start
		waitCond.Signal()
	} else if CoordinatorPhase(c.Phase.Load()) == PhaseExit { // no more task
		*reply = Task{State: PhaseExit}
	} else { // wait for task
		*reply = Task{State: PhaseWait}
	}
	return nil
}

func (c *Coordinator) checkTimeout() {
	for {
		time.Sleep(Timeout)
		if CoordinatorPhase(c.Phase.Load()) == PhaseExit {
			return
		}
		c.TaskMeta.Range(func(key, value any) bool {
			taskMeta := value.(*CoordinatorTask)
			if taskMeta.Status == StatusInProgress && time.Since(taskMeta.StartTime) > Timeout*2 {
				c.TaskQueue <- taskMeta.TaskReference
				taskMeta.Status = StatusIdle
			}
			return true
		})
	}
}

func (c *Coordinator) TaskCompleted(task *Task, _ *ExampleReply) error {
	// update task meta
	v, _ := c.TaskMeta.Load(task.TaskId)
	taskMeta := v.(*CoordinatorTask)
	// check task status
	if task.State != CoordinatorPhase(c.Phase.Load()) || taskMeta.Status == StatusCompleted {
		return nil
	}
	// update task meta
	taskMeta.Status = StatusCompleted
	go c.processTaskResult(task)

	return nil
}

func (c *Coordinator) processTaskResult(task *Task) {
	switch task.State {
	case PhaseMap:
		c.processMapTaskResult(task)
	case PhaseReduce:
		c.processReduceTaskResult(task)
	}
}

// processMapTaskResult processes the result of map task
func (c *Coordinator) processMapTaskResult(task *Task) {
	// add intermediate files
	for reduceId, filename := range task.Intermediates {
		c.Intermediates[reduceId] = append(c.Intermediates[reduceId], filename)
	}
	// check if all map tasks are completed
	if c.allTaskDone() {
		// start reduce phase
		c.createReduceTask()
		c.Phase.Store(int32(PhaseReduce))
	}
}

// processReduceTaskResult processes the result of reduce task
func (c *Coordinator) processReduceTaskResult(_ *Task) {
	if c.allTaskDone() {
		c.Phase.Store(int32(PhaseExit))
	}
}

// allTaskDone checks if all tasks are completed
func (c *Coordinator) allTaskDone() bool {
	done := true
	c.TaskMeta.Range(func(key, value any) bool {
		taskMeta := value.(*CoordinatorTask)
		if taskMeta.Status != StatusCompleted {
			done = false
			return false
		}
		return true
	})
	return done
}

// createReduceTask creates reduce tasks
func (c *Coordinator) createReduceTask() {
	c.TaskMeta = new(sync.Map)
	for idx, files := range c.Intermediates {
		task := &Task{
			TaskId:        idx + 1, // task id starts from 1, not 0
			State:         PhaseReduce,
			NReduce:       c.NReduce,
			Intermediates: files,
		}
		// add to task queue
		c.TaskQueue <- task
		// add to task meta
		c.TaskMeta.Store(task.TaskId, &CoordinatorTask{
			Status:        StatusIdle,
			TaskReference: task,
		})
	}
}
