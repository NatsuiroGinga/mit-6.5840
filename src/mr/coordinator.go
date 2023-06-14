package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"sync"
	"time"
)

type Coordinator struct {
	TaskQueue   chan *Task  // task queue
	TaskQueueMu *sync.Mutex // mutex for task queue

	TaskMeta *sync.Map // task id -> task meta

	Phase   CoordinatorPhase // current phase
	PhaseMu *sync.Mutex

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
	rpc.Register(c)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":1234")
	/* sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("tcp", sockname) */
	if e != nil {
		log.Fatal("listen error:", e)
	}
	log.Printf("Coordinator server start at %s", l.Addr())
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.PhaseMu.Lock()
	defer c.PhaseMu.Unlock()
	return c.Phase == PhaseExit
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := &Coordinator{
		TaskQueue:     make(chan *Task, Max(nReduce, len(files))),
		TaskMeta:      new(sync.Map),
		TaskQueueMu:   new(sync.Mutex),
		Phase:         PhaseMap,
		NReduce:       nReduce,
		Inputs:        files,
		Intermediates: make([][]string, nReduce),
		PhaseMu:       new(sync.Mutex),
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
			StartTime:     time.Now(),
		})
	}
}

// AssignTask assigns a task to a worker
func (c *Coordinator) AssignTask(_ *ExampleArgs, reply *Task) error {
	mu.Lock()
	defer mu.Unlock()

	if len(c.TaskQueue) > 0 {
		// get task from queue
		*reply = *<-c.TaskQueue
		// update task meta
		v, _ := c.TaskMeta.Load(reply.TaskId)
		taskMeta := v.(*CoordinatorTask)
		taskMeta.Status = StatusInProgress
		taskMeta.StartTime = time.Now()
	} else if c.Phase == PhaseExit { // no more task
		*reply = Task{State: PhaseExit}
	} else { // wait for task
		*reply = Task{State: PhaseWait}
	}

	return nil
}

func (c *Coordinator) checkTimeout() {
	times := time.Tick(Timeout)
	for tick := range times {
		mu.Lock()
		log.Printf("check timeout at %v", tick.Format("2006-01-02 15:04:05"))

		if c.Phase == PhaseExit {
			mu.Unlock()
			return
		}
		c.TaskMeta.Range(func(key, value any) bool {
			taskMeta := value.(*CoordinatorTask)
			if taskMeta.Status == StatusInProgress && time.Since(taskMeta.StartTime) >= Timeout {
				log.Printf("task %d timeout", taskMeta.TaskReference.TaskId)
				c.TaskQueue <- taskMeta.TaskReference
				taskMeta.Status = StatusIdle
				log.Printf("task %d added to queue", taskMeta.TaskReference.TaskId)
			}
			return true
		})
		mu.Unlock()
	}
	/*for {
		time.Sleep(Timeout)
		c.PhaseMu.Lock()

		if c.Phase == PhaseExit {
			c.PhaseMu.Unlock()
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
		c.PhaseMu.Unlock()
	}*/
}

var mu sync.Mutex

func (c *Coordinator) TaskCompleted(task *Task, _ *ExampleReply) error {
	mu.Lock()
	// update task meta
	v, _ := c.TaskMeta.Load(task.TaskId)
	taskMeta := v.(*CoordinatorTask)
	// check task status
	if task.State != c.Phase || taskMeta.Status == StatusCompleted {
		log.Printf("task %d has been completed or phase is not %s", task.TaskId, phaseName[c.Phase])
		return nil
	}
	// update task meta
	taskMeta.Status = StatusCompleted
	mu.Unlock()
	defer c.processTaskResult(task)

	return nil
}

func (c *Coordinator) processTaskResult(task *Task) {
	mu.Lock()
	defer mu.Unlock()

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
		c.Phase = PhaseReduce
	}
}

// processReduceTaskResult processes the result of reduce task
func (c *Coordinator) processReduceTaskResult(_ *Task) {
	if c.allTaskDone() {
		c.Phase = PhaseExit
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
			StartTime:     time.Now(),
		})
	}
}
