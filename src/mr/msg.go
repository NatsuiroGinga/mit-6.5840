package mr

// MsgType is the type of the message
type MsgType int

const (
	Create MsgType = 1 + iota
	Finish
	Update
	Fail
)

var msgTypes = [...]string{
	1: "Create",
	2: "Finish",
	3: "Update",
	4: "Fail",
}

func (t MsgType) String() string {
	if t <= 0 || int(t) >= len(msgTypes) {
		return "Unknown"
	}
	return msgTypes[t]
}
