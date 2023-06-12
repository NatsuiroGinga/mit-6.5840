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
	id              int
	excludeWorkerId int
	reduceNum       int
	isExit          bool
}

func (t *Task) GetInput() []string {
	return t.input
}

func (t *Task) IsCreate() bool {
	return t.isCreate
}

func (t *Task) GetTaskType() TaskType {
	return t.taskType
}

func (t *Task) GetId() int {
	return t.id
}

func (t *Task) GetExcludeWorkerId() int {
	return t.excludeWorkerId
}

func (t *Task) GetReduceNum() int {
	return t.reduceNum
}

func (t *Task) IsExit() bool {
	return t.isExit
}

type TaskMsg struct {
	msgType   MsgType
	taskId    int
	taskType  TaskType
	input     []string
	output    []string
	timeStamp time.Time
	workerId  int
	reduceNum int
}

func (m *TaskMsg) GetMsgType() MsgType {
	return m.msgType
}

func (m *TaskMsg) GetTaskId() int {
	return m.taskId
}

func (m *TaskMsg) GetTaskType() TaskType {
	return m.taskType
}

func (m *TaskMsg) GetInput() []string {
	return m.input
}

func (m *TaskMsg) GetOutput() []string {
	return m.output
}

func (m *TaskMsg) GetTimeStamp() time.Time {
	return m.timeStamp
}

func (m *TaskMsg) GetWorkerId() int {
	return m.workerId
}

func (m *TaskMsg) GetReduceNum() int {
	return m.reduceNum
}

type TaskStatus struct {
	taskId    int
	taskType  int
	input     []string
	startTime time.Time
	workerId  int
	reduceNum int
}

func (s *TaskStatus) GetTaskId() int {
	return s.taskId
}

func (s *TaskStatus) GetTaskType() int {
	return s.taskType
}

func (s *TaskStatus) GetInput() []string {
	return s.input
}

func (s *TaskStatus) GetStartTime() time.Time {
	return s.startTime
}

func (s *TaskStatus) GetWorkerId() int {
	return s.workerId
}

func (s *TaskStatus) GetReduceNum() int {
	return s.reduceNum
}
