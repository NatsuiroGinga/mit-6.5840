package mr

import "time"

// TaskType is the type of the task
type TaskType int

const (
	Map    TaskType = iota + 1 // execute Map task
	Reduce                     // execute Reduce task
)

var taskTypes = [...]string{
	1: "Map",
	2: "Reduce",
}

func (t TaskType) String() string {
	if t <= 0 || int(t) >= len(taskTypes) {
		return "Unknown"
	}
	return taskTypes[t]
}

// Task is the task to be executed
type Task struct {
	input           []string
	isCreate        bool
	taskType        TaskType
	id              int64
	excludeWorkerId int64
	reduceNum       int
	isExit          bool
}

type TaskRequest struct {
	MsgType   MsgType
	TaskId    int64
	TaskType  TaskType
	Input     []string
	Output    []string
	TimeStamp time.Time
	WorkerId  int64
	ReduceNum int
}

func (request *TaskRequest) StartTime() time.Time {
	return request.TimeStamp
}

type TaskStatus struct {
	TaskId    int64
	TaskType  TaskType
	Input     []string
	StartTime time.Time
	WorkerId  int32
	ReduceNum int
}
