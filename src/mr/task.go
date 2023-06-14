package mr

import "time"

type Task struct {
	TaskId        int
	State         CoordinatorPhase
	Input         string
	NReduce       int
	Intermediates []string // intermediate files (map) or input files (reduce)
	Output        string   // output file (reduce)
}

// TaskMeta is the meta data of a task
type CoordinatorTask struct {
	Status        TaskStatus
	TaskReference *Task
	StartTime     time.Time
}

type TaskStatus int

const (
	StatusIdle       TaskStatus = 1 + iota // task is idle
	StatusInProgress                       // task is in progress
	StatusCompleted                        // task is completed
)

var taskStatusName = [...]string{
	1: "Idle",
	2: "InProgress",
	3: "Completed",
}

func (t TaskStatus) String() string {
	if t <= 0 || int(t) >= len(taskStatusName) {
		return "Unknown"
	}
	return taskStatusName[t]
}
