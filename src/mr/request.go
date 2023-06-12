package mr

// RequestType is the type of the request
type RequestType int

const (
	// Initial is the initial request
	Initial RequestType = iota + 1
	// Finished is the finished request
	Finished
	// Failed is the failed request
	Failed
)

// requestTypes is the string representation of the request type
var requestTypes = [...]string{
	1: "Initial",
	2: "Finished",
	3: "Failed",
}

func (r RequestType) String() string {
	if r <= 0 || int(r) >= len(requestTypes) {
		return "Unknown"
	}
	return requestTypes[r]
}
