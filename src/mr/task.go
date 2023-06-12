package mr

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
